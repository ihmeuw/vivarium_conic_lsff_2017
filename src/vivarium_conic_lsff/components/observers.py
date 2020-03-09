from collections import Counter
from itertools import product
from typing import Dict

import pandas as pd
import numpy as np
from vivarium_public_health.metrics import (MortalityObserver as MortalityObserver_)
from vivarium_public_health.metrics.utilities import (get_output_template, get_group_counts,
                                                      QueryString, to_years, get_person_time,
                                                      get_deaths, get_years_of_life_lost,
                                                      get_age_bins, get_time_iterable)

from vivarium_conic_lsff import globals as project_globals


class MortalityObserver(MortalityObserver_):

    def setup(self, builder):
        super().setup(builder)
        columns_required = ['tracked', 'alive', 'entrance_time', 'exit_time', 'cause_of_death',
                            'years_of_life_lost', 'age']
        if self.config.by_sex:
            columns_required += ['sex']
        self.age_bins = get_age_bins(builder)
        # Overwrites attribute set in parent class
        self.population_view = builder.population.get_view(columns_required)
        self.exposure = builder.value.get_value(f'{project_globals.LBWSG_MODEL_NAME}.exposure')

    def metrics(self, index, metrics):
        pop = self.population_view.get(index)
        pop.loc[pop.exit_time.isnull(), 'exit_time'] = self.clock()

        exposure = self.exposure(index, skip_post_processor=True)
        pop[project_globals.BIRTH_WEIGHT_STATUS_COLUMN] =\
                np.where(exposure[project_globals.BIRTH_WEIGHT] < project_globals.UNDERWEIGHT,
                         project_globals.BIRTH_WEIGHT_UNDERWEIGHT, project_globals.BIRTH_WEIGHT_NORMAL)
        pop[project_globals.GESTATIONAL_AGE_STATUS_COLUMN] =\
                np.where(exposure[project_globals.GESTATION_TIME] < project_globals.PRETERM,
                         project_globals.GESTATIONAL_AGE_PRETERM, project_globals.GESTATIONAL_AGE_NORMAL)

        measure_getters = (
            (get_person_time, ()),
            (get_deaths, (project_globals.CAUSES_OF_DEATH,)),
            (get_years_of_life_lost, (self.life_expectancy, project_globals.CAUSES_OF_DEATH)),
        )

        categories = product(project_globals.BIRTH_WEIGHT_CATEGORIES, project_globals.GESTATIONAL_AGE_CATEGORIES)
        for bw_cat, ga_cat in categories:
            pop_in_group = pop.loc[(pop[project_globals.BIRTH_WEIGHT_STATUS_COLUMN] == bw_cat)
                                   & (pop[project_globals.GESTATIONAL_AGE_STATUS_COLUMN] == ga_cat)]
            base_args = (pop_in_group, self.config.to_dict(), self.start_time, self.clock(), self.age_bins)

            for measure_getter, extra_args in measure_getters:
                measure_data = measure_getter(*base_args, *extra_args)
                measure_data = {f'{k}_birthweight_{bw_cat}_gestational_age_{ga_cat}': v
                                for k, v in measure_data.items()}
                metrics.update(measure_data)

        the_living = pop[(pop.alive == 'alive') & pop.tracked]
        the_dead = pop[pop.alive == 'dead']
        metrics[project_globals.TOTAL_YLLS_COLUMN] = self.life_expectancy(the_dead.index).sum()
        metrics['total_population_living'] = len(the_living)
        metrics['total_population_dead'] = len(the_dead)

        return metrics


class DiseaseObserver:
    """Observes disease counts, person time, and prevalent cases for a cause.
    By default, this observer computes aggregate susceptible person time
    and counts of disease cases over the entire simulation.  It can be
    configured to bin these into age_groups, sexes, and years by setting
    the ``by_age``, ``by_sex``, and ``by_year`` flags, respectively.
    It also records prevalent cases on a particular sample date each year.
    These will also be binned based on the flags set for the observer.
    Additionally, the sample date is configurable and defaults to July 1st
    of each year.
    In the model specification, your configuration for this component should
    be specified as, e.g.:
    .. code-block:: yaml
        configuration:
            metrics:
                {YOUR_DISEASE_NAME}_observer:
                    by_age: True
                    by_year: False
                    by_sex: True
                    prevalence_sample_date:
                        month: 4
                        day: 10
    """
    configuration_defaults = {
        'metrics': {
            'disease_observer': {
                'by_age': False,
                'by_year': False,
                'by_sex': False,
            }
        }
    }

    def __init__(self, disease: str):
        self.disease = disease
        self.configuration_defaults = {
            'metrics': {f'{disease}_observer': DiseaseObserver.configuration_defaults['metrics']['disease_observer']}
        }

    @property
    def name(self):
        return f'disease_observer.{self.disease}'

    def setup(self, builder):
        self.config = builder.configuration['metrics'][f'{self.disease}_observer'].to_dict()
        self.clock = builder.time.clock()
        self.age_bins = get_age_bins(builder)
        self.counts = Counter()
        self.person_time = Counter()

        self.states = project_globals.DISEASE_MODEL_MAP[self.disease]['states']
        self.transitions = project_globals.DISEASE_MODEL_MAP[self.disease]['transitions']

        self.previous_state_column = f'previous_{self.disease}'
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=[self.previous_state_column])

        columns_required = ['alive', f'{self.disease}', self.previous_state_column]
        for state in self.states:
            columns_required.append(f'{state}_event_time')
        if self.config['by_age']:
            columns_required += ['age']
        if self.config['by_sex']:
            columns_required += ['sex']
        self.population_view = builder.population.get_view(columns_required)

        builder.value.register_value_modifier('metrics', self.metrics)
        # FIXME: The state table is modified before the clock advances.
        # In order to get an accurate representation of person time we need to look at
        # the state table before anything happens.
        builder.event.register_listener('time_step__prepare', self.on_time_step_prepare)
        builder.event.register_listener('collect_metrics', self.on_collect_metrics)

    def on_initialize_simulants(self, pop_data):
        self.population_view.update(pd.Series('', index=pop_data.index, name=self.previous_state_column))

    def on_time_step_prepare(self, event):
        pop = self.population_view.get(event.index)
        # Ignoring the edge case where the step spans a new year.
        # Accrue all counts and time to the current year.
        for state in self.states:
            state_person_time_this_step = get_state_person_time(pop, self.config, self.disease, state,
                                                                self.clock().year, event.step_size, self.age_bins)
            self.person_time.update(state_person_time_this_step)

        # This enables tracking of transitions between states
        prior_state_pop = self.population_view.get(event.index)
        prior_state_pop[self.previous_state_column] = prior_state_pop[self.disease]
        self.population_view.update(prior_state_pop)

    def on_collect_metrics(self, event):
        pop = self.population_view.get(event.index)
        for transition in self.transitions:
            transition_counts_this_step = get_transition_count(pop, self.config, self.disease, transition,
                                                               event.time, self.age_bins)
            self.counts.update(transition_counts_this_step)

    def metrics(self, index, metrics):
        metrics.update(self.counts)
        metrics.update(self.person_time)
        return metrics

    def __repr__(self):
        return f"DiseaseObserver({self.disease})"


def get_state_person_time(pop, config, disease, state, current_year, step_size, age_bins):
    """Custom person time getter that handles state column name assumptions"""
    base_key = get_output_template(**config).substitute(measure=f'{state}_person_time',
                                                        year=current_year)
    base_filter = QueryString(f'alive == "alive" and {disease} == "{state}"')
    person_time = get_group_counts(pop, base_filter, base_key, config, age_bins,
                                   aggregate=lambda x: len(x) * to_years(step_size))
    return person_time


def get_transition_count(pop, config, disease, transition, event_time, age_bins):
    from_state, to_state = transition.split('_TO_')
    event_this_step = ((pop[f'{to_state}_event_time'] == event_time)
                       & (pop[f'previous_{disease}'] == from_state))
    transitioned_pop = pop.loc[event_this_step]
    base_key = get_output_template(**config).substitute(measure=f'{transition}_event_count',
                                                        year=event_time.year)
    base_filter = QueryString('')
    transition_count = get_group_counts(transitioned_pop, base_filter, base_key, config, age_bins)
    return transition_count



class LiveBirthWithNTDObserver:
    """Observes births and births with neural tube defects. Output can be stratified
    by year and by sex.
    """
    configuration_defaults = {
        'metrics': {
            project_globals.NTD_OBSERVER: {
                'by_year': True,
                'by_sex': True,
            }
        }
    }

    @property
    def name(self):
        return project_globals.NTD_OBSERVER

    def setup(self, builder):
        self.disease = project_globals.NTD_MODEL_NAME
        self.config = builder.configuration['metrics'][project_globals.NTD_OBSERVER].to_dict()
        self.config['by_age'] = False

        tmp = builder.configuration['time']['start']
        self._sim_start = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self._sim_end = pd.Timestamp(**builder.configuration.time.end.to_dict())

        columns_required = ['alive', 'dead', f'{self.disease}', 'entrance_time', 'tracked']
        if self.config['by_sex']:
            columns_required.append('sex')

        self.population_view = builder.population.get_view(columns_required)
        builder.value.register_value_modifier('metrics', self.metrics)


    def metrics(self, index, metrics):
        pop = self.population_view.get(index)
        births = get_births(pop, self.config, self._sim_start, self._sim_end)
        metrics.update(births)
        return metrics


    def __repr__(self):
        return f"DiseaseObserver({self.disease})"


def get_births(pop: pd.DataFrame, config: Dict[str, bool], sim_start: pd.Timestamp,
               sim_end: pd.Timestamp) -> Dict[str, int]:
    """Counts the number of births and births with neural tube defects prevelant.
    Parameters
    ----------
    pop
        The population dataframe to be counted. It must contain sufficient
        columns for any necessary filtering (e.g. the ``age`` column if
        filtering by age).
    config
        A dict with ``by_age``, ``by_sex``, and ``by_year`` keys and
        boolean values.
    sim_start
        The simulation start time.
    sim_end
        The simulation end time.
    Returns
    -------
    births
        All births and births with neural tube defects present.
    """
    base_filter = QueryString('')
    base_key = get_output_template(**config)
    time_spans = get_time_iterable(config, sim_start, sim_end)

    births = {}
    for year, (t_start, t_end) in time_spans:
        start = max(sim_start, t_start)
        end = min(sim_end, t_end)
        born_in_span = pop.query(f'"{start}" <= entrance_time and entrance_time < "{end}"')

        cat_year_key = base_key.substitute(measure='live_births', year=year)
        filter = base_filter
        group_births = get_group_counts(born_in_span, filter, cat_year_key, config, pd.DataFrame())
        births.update(group_births)

        cat_year_key = base_key.substitute(measure='born_with_ntds', year=year)
        filter = base_filter + f'{project_globals.NTD_MODEL_NAME} == "{project_globals.NTD_MODEL_NAME}"'
        empty_age_bins = pd.DataFrame()
        group_ntd_births = get_group_counts(born_in_span, filter, cat_year_key, config, empty_age_bins)
        births.update(group_ntd_births)
    return births



class LBWSGObserver:

    @property
    def name(self):
        return f'risk_observer.low_birth_weight_and_short_gestation'

    def setup(self, builder):
        value_key = 'low_birth_weight_and_short_gestation.exposure'
        self.lbwsg = builder.value.get_value(value_key)
        builder.value.register_value_modifier('metrics', self.metrics)
        self.results = {}
        columns = ['sex']
        self.population_view = builder.population.get_view(columns)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 requires_columns=columns,
                                                 requires_values=[value_key])

    def on_initialize_simulants(self, pop_data):
        pop = self.population_view.get(pop_data.index)
        raw_exposure = self.lbwsg(pop_data.index, skip_post_processor=True)
        exposure = self.lbwsg(pop_data.index)
        pop = pd.concat([pop, raw_exposure, exposure], axis=1)
        stats = self.get_lbwsg_stats(pop)
        self.results.update(stats)

    def get_lbwsg_stats(self, pop):
        stats = {'birth_weight_mean': 0,
                 'birth_weight_sd': 0,
                 'birth_weight_proportion_below_2500g': 0,
                 'gestational_age_mean': 0,
                 'gestational_age_sd': 0,
                 'gestational_age_proportion_below_37w': 0,
                 }
        if not pop.empty:
            stats[f'birth_weight_mean'] = pop.birth_weight.mean()
            stats[f'birth_weight_sd'] = pop.birth_weight.std()
            stats[f'birth_weight_proportion_below_2500g'] = (
                    len(pop[pop.birth_weight < project_globals.UNDERWEIGHT]) / len(pop)
            )
            stats[f'gestational_age_mean'] = pop.gestation_time.mean()
            stats[f'gestational_age_sd'] = pop.gestation_time.std()
            stats[f'gestational_age_proportion_below_37w'] = (
                    len(pop[pop.gestation_time < project_globals.PRETERM]) / len(pop)
            )
        return stats

    def metrics(self, index, metrics):
        metrics.update(self.results)
        return metrics
