from collections import Counter

import pandas as pd
from vivarium_public_health.metrics.utilities import (get_output_template, get_group_counts,
                                                      QueryString, to_years, get_age_bins)

from vivarium_conic_lsff import globals as project_globals


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
        self.live_birth_counts = Counter()
        self.with_disease = Counter()
        tmp = builder.configuration['time']['start']
        self._step_start_time = pd.Timestamp(tmp['year'], tmp['month'], tmp['day'])

        columns_required = ['alive', f'{self.disease}', 'entrance_time']
        if self.config['by_sex']:
            columns_required.append('sex')
        self.population_view = builder.population.get_view(columns_required)

        builder.value.register_value_modifier('metrics', self.metrics)
        builder.event.register_listener('collect_metrics', self.on_collect_metrics)

    def on_collect_metrics(self, event):
        pop = self.population_view.get(event.index)
        live_births_this_step = get_live_births(pop, self.config, self._step_start_time, event.time.year)
        self.live_birth_counts.update(live_births_this_step)
        births_with_ntds_this_step = get_with_ntd_births(pop, self.config, self._step_start_time, event.time.year)
        self.with_disease.update(births_with_ntds_this_step)
        self._step_start_time = pd.Timestamp(event.time.year, event.time.month, event.time.day)

    def metrics(self, index, metrics):
        metrics.update(self.live_birth_counts)
        metrics.update(self.with_disease)
        return metrics

    def __repr__(self):
        return f"DiseaseObserver({self.disease})"


def get_live_births(pop, config, reference_time, year):
    filter = QueryString(f'alive == "alive" and entrance_time >= "{reference_time}"')
    base_key = get_output_template(**config).substitute(measure=f'live_births', year=year)
    cfg = {'by_age': False, 'by_sex': config['by_sex']}
    return get_group_counts(pop, filter, base_key, cfg, pd.DataFrame())


def get_with_ntd_births(pop, config, reference_time, year):
    filter = QueryString(f'alive == "alive"'
            f' and {project_globals.NTD_MODEL_NAME} == "{project_globals.NTD_MODEL_NAME}"'
            f' and entrance_time >= "{reference_time}"')
    base_key = get_output_template(**config).substitute(measure=f'born_with_ntd', year=year)
    cfg = {'by_age': False, 'by_sex': config['by_sex']}
    return get_group_counts(pop, filter, base_key, cfg, pd.DataFrame())

