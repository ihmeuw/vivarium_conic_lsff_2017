from collections import Counter
import itertools
import typing
from typing import Dict, Iterable, List, Tuple, Union

import pandas as pd
from vivarium_public_health.metrics.disability import get_years_lived_with_disability
from vivarium_public_health.metrics import (MortalityObserver as MortalityObserver_,
                                            DisabilityObserver as DisabilityObserver_)
from vivarium_public_health.metrics.utilities import (get_output_template, get_group_counts,
                                                      QueryString, to_years, get_person_time,
                                                      get_deaths, get_years_of_life_lost,
                                                      get_age_bins, get_time_iterable)

from vivarium_conic_lsff import globals as project_globals
from vivarium_conic_lsff.components import VitaminADeficiency

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.event import Event
    from vivarium.framework.population import SimulantData


class ResultsStratifier:
    """Centralized component for handling results stratification.

    This should be used as a sub-component for observers.  The observers
    can then ask this component for population subgroups and labels during
    results production and have this component manage adjustments to the
    final column labels for the subgroups.

    """

    def __init__(self, observer_name: str):
        self.name = f'{observer_name}_results_stratifier'

    def setup(self, builder: 'Builder'):
        """Perform this component's setup."""
        # The only thing you should request here are resources necessary for
        # results stratification.
        self.population_view = builder.population.get_view([
            project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN,
            project_globals.VITAMIN_A_COVERAGE_START_COLUMN,
            'age',
            'tracked',  # Ensure we get the full population.
        ])
        self.vitamin_a_coverage = builder.value.get_value('vitamin_a_fortification.effectively_covered')

    def group(self, population: pd.DataFrame) -> Iterable[Tuple[Tuple[str, ...], pd.DataFrame]]:
        """Takes the full population and yields stratified subgroups.

        Parameters
        ----------
        population
            The population to stratify.

        Yields
        ------
            A tuple of stratification labels and the population subgroup
            corresponding to those labels.

        """
        folic_acid_covered = self.folic_acid_covered(population)
        vitamin_a_covered = self.vitamin_a_covered(population)

        groups = itertools.product(project_globals.FOLIC_ACID_FORTIFICATION_GROUPS,
                                   project_globals.VITAMIN_A_FORTIFICATION_GROUPS)
        for folic_acid_group, vitamin_a_group in groups:
            if population.empty:
                pop_in_group = population
            else:
                pop_in_group = population.loc[(folic_acid_covered == folic_acid_group)
                                              & (vitamin_a_covered == vitamin_a_group)]
            yield (folic_acid_group, vitamin_a_group), pop_in_group

    @staticmethod
    def update_labels(measure_data: Dict[str, float], labels: Tuple[str, ...]) -> Dict[str, float]:
        """Updates a dict of measure data with stratification labels.

        Parameters
        ----------
        measure_data
            The measure data with unstratified column names.
        labels
            The stratification labels. Yielded along with the population
            subgroup the measure data was produced from by a call to
            :obj:`ResultsStratifier.group`.

        Returns
        -------
            The measure data with column names updated with the stratification
            labels.

        """
        folic_acid_group, vitamin_a_group = labels
        measure_data = {f'{k}_folic_acid_{folic_acid_group}_vitamin_a_{vitamin_a_group}': v
                        for k, v in measure_data.items()}
        return measure_data

    def folic_acid_covered(self, population: pd.DataFrame) -> pd.Series:
        pop = self.population_view.get(population.index)
        return pop[project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN]

    def vitamin_a_covered(self, population: pd.DataFrame) -> pd.Series:
        pop = self.population_view.get(population.index)
        raw_coverage = self.vitamin_a_coverage(population.index).map({'cat1': 'uncovered',
                                                                      'cat2': 'effectively_covered'})
        started = ~pop[project_globals.VITAMIN_A_COVERAGE_START_COLUMN].isnull()
        underage = pop.age <= 0.5
        uncovered = (raw_coverage == 'uncovered') & ~started
        covered = (
                ((raw_coverage == 'uncovered') & started)
                | ((raw_coverage == 'effectively_covered') & underage)
        )
        effectively_covered = (raw_coverage == 'effectively_covered') & ~underage

        raw_coverage.loc[uncovered] = 'uncovered'
        raw_coverage.loc[covered] = 'covered'
        raw_coverage.loc[effectively_covered] = 'effectively_covered'
        return raw_coverage


class MortalityObserver(MortalityObserver_):

    def __init__(self):
        super().__init__()
        self.stratifier = ResultsStratifier(self.name)

    @property
    def sub_components(self) -> List[ResultsStratifier]:
        return [self.stratifier]

    def metrics(self, index, metrics):
        pop = self.population_view.get(index)
        pop.loc[pop.exit_time.isnull(), 'exit_time'] = self.clock()

        measure_getters = (
            # FIXME: get person time needs to happen every time step.
            (get_person_time, ()),
            (get_deaths, (self.causes,)),
            (get_years_of_life_lost, (self.life_expectancy, self.causes)),
        )

        for labels, pop_in_group in self.stratifier.group(pop):
            base_args = (pop_in_group, self.config.to_dict(), self.start_time, self.clock(), self.age_bins)

            for measure_getter, extra_args in measure_getters:
                measure_data = measure_getter(*base_args, *extra_args)
                measure_data = self.stratifier.update_labels(measure_data, labels)
                metrics.update(measure_data)

        the_living = pop[(pop.alive == 'alive') & pop.tracked]
        the_dead = pop[pop.alive == 'dead']
        metrics[project_globals.TOTAL_YLLS_COLUMN] = self.life_expectancy(the_dead.index).sum()
        metrics['total_population_living'] = len(the_living)
        metrics['total_population_dead'] = len(the_dead)

        return metrics


class DisabilityObserver(DisabilityObserver_):

    def __init__(self):
        super().__init__()
        self.stratifier = ResultsStratifier(self.name)

    @property
    def sub_components(self) -> List[ResultsStratifier]:
        return [self.stratifier]

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: 'Builder'):
        super().setup(builder)
        if builder.components.get_components_by_type(VitaminADeficiency):
            self.causes += [project_globals.VITAMIN_A_MODEL_NAME]
            self.disability_weight_pipelines = {cause: builder.value.get_value(f'{cause}.disability_weight')
                                                for cause in self.causes}

    def on_time_step_prepare(self, event: 'Event'):
        pop = self.population_view.get(event.index, query='tracked == True and alive == "alive"')
        self.update_metrics(pop)

        pop.loc[:, project_globals.TOTAL_YLDS_COLUMN] += self.disability_weight(pop.index)
        self.population_view.update(pop)

    def update_metrics(self, pop: pd.DataFrame):
        for labels, pop_in_group in self.stratifier.group(pop):
            ylds_this_step = get_years_lived_with_disability(pop_in_group, self.config.to_dict(),
                                                             self.clock().year, self.step_size(),
                                                             self.age_bins, self.disability_weight_pipelines,
                                                             self.causes)
            ylds_this_step = self.stratifier.update_labels(ylds_this_step, labels)
            self.years_lived_with_disability.update(ylds_this_step)


class DiseaseObserver:
    """Observes transition counts and person time for a cause."""
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
        self.stratifier = ResultsStratifier(self.name)

    @property
    def name(self) -> str:
        return f'disease_observer.{self.disease}'

    @property
    def sub_components(self) -> List[ResultsStratifier]:
        return [self.stratifier]

    def setup(self, builder: 'Builder'):
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

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        self.population_view.update(pd.Series('', index=pop_data.index, name=self.previous_state_column))

    def on_time_step_prepare(self, event: 'Event'):
        pop = self.population_view.get(event.index)
        # Ignoring the edge case where the step spans a new year.
        # Accrue all counts and time to the current year.
        for labels, pop_in_group in self.stratifier.group(pop):
            for state in self.states:
                # noinspection PyTypeChecker
                state_person_time_this_step = get_state_person_time(pop_in_group, self.config, self.disease, state,
                                                                    self.clock().year, event.step_size, self.age_bins)
                state_person_time_this_step = self.stratifier.update_labels(state_person_time_this_step, labels)
                self.person_time.update(state_person_time_this_step)

        # This enables tracking of transitions between states
        prior_state_pop = self.population_view.get(event.index)
        prior_state_pop[self.previous_state_column] = prior_state_pop[self.disease]
        self.population_view.update(prior_state_pop)

    def on_collect_metrics(self, event: 'Event'):
        pop = self.population_view.get(event.index)
        for labels, pop_in_group in self.stratifier.group(pop):
            for transition in self.transitions:
                # noinspection PyTypeChecker
                transition_counts_this_step = get_transition_count(pop_in_group, self.config, self.disease, transition,
                                                                   event.time, self.age_bins)
                transition_counts_this_step = self.stratifier.update_labels(transition_counts_this_step, labels)
                self.counts.update(transition_counts_this_step)

    def metrics(self, index: pd.Index, metrics: Dict[str, float]):
        metrics.update(self.counts)
        metrics.update(self.person_time)
        return metrics

    def __repr__(self) -> str:
        return f"DiseaseObserver({self.disease})"


def get_state_person_time(pop: pd.DataFrame, config: Dict[str, bool],
                          disease: str, state: str, current_year: Union[str, int],
                          step_size: pd.Timedelta, age_bins: pd.DataFrame) -> Dict[str, float]:
    """Custom person time getter that handles state column name assumptions"""
    base_key = get_output_template(**config).substitute(measure=f'{state}_person_time',
                                                        year=current_year)
    base_filter = QueryString(f'alive == "alive" and {disease} == "{state}"')
    person_time = get_group_counts(pop, base_filter, base_key, config, age_bins,
                                   aggregate=lambda x: len(x) * to_years(step_size))
    return person_time


def get_transition_count(pop: pd.DataFrame, config: Dict[str, bool],
                         disease: str, transition: project_globals.TransitionString,
                         event_time: pd.Timestamp, age_bins: pd.DataFrame) -> Dict[str, float]:
    """Counts transitions that occurred this step."""
    event_this_step = ((pop[f'previous_{disease}'] == transition.from_state)
                       & (pop[disease] == transition.to_state))
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

    def __init__(self):
        self.stratifier = ResultsStratifier(self.name)

    @property
    def name(self):
        return project_globals.NTD_OBSERVER

    @property
    def sub_components(self) -> List[ResultsStratifier]:
        return [self.stratifier]

    def setup(self, builder):
        self.disease = project_globals.NTD_MODEL_NAME
        self.config = builder.configuration['metrics'][project_globals.NTD_OBSERVER].to_dict()
        self.config['by_age'] = False

        self._sim_start = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self._sim_end = pd.Timestamp(**builder.configuration.time.end.to_dict())

        columns_required = ['alive', f'{self.disease}', 'entrance_time', 'tracked']
        if self.config['by_sex']:
            columns_required.append('sex')

        self.population_view = builder.population.get_view(columns_required)
        builder.value.register_value_modifier('metrics', self.metrics)

    def metrics(self, index, metrics):
        pop = self.population_view.get(index)
        for labels, pop_in_group in self.stratifier.group(pop):
            births = get_births(pop_in_group, self.config, self._sim_start, self._sim_end)
            births = self.stratifier.update_labels(births, labels)
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
        group_births = get_group_counts(born_in_span, base_filter, cat_year_key, config, pd.DataFrame())
        births.update(group_births)

        cat_year_key = base_key.substitute(measure='born_with_ntds', year=year)
        filter_update = f'{project_globals.NTD_MODEL_NAME} == "{project_globals.NTD_MODEL_NAME}"'
        empty_age_bins = pd.DataFrame()
        group_ntd_births = get_group_counts(born_in_span, base_filter + filter_update,
                                            cat_year_key, config, empty_age_bins)
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
