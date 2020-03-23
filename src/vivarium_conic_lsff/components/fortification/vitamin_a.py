"""Vitamin a fortification model."""
import typing

import pandas as pd

from vivarium_conic_lsff.components.fortification.parameters import (sample_vitamin_a_coverage,
                                                                     sample_vitamin_a_relative_risk,
                                                                     sample_vitamin_a_time_to_effect)
from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.event import Event
    from vivarium.framework.population import SimulantData


class VitaminAFortificationCoverage:
    """Model of vitamin a fortification coverage.

    This component manages both the population level coverage of vitamin
    a fortification as well as whether individuals are effectively covered.
    Effective coverage means an individual is consuming vitamin a fortified
    goods and the fortified food is having an impact on their deficiency
    probability.  There is a time delay between when an individual
    starts consuming the fortified goods and when they start having an impact.

    The initial population effective coverage is based on the coverage level
    at simulation start (with the assumption that it has been constant
    for all time in the past).  Newborns are assigned an effective coverage
    based on current coverage level at birth.

    """

    @property
    def name(self) -> str:
        """This component's canonical name."""
        return 'vitamin_a_fortification_coverage'

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: 'Builder'):
        """Perform this component's setup."""
        self.clock = builder.time.clock()

        coverage_data = self.load_coverage_data(builder)
        coverage = builder.lookup.build_table(coverage_data)
        self.coverage_level = builder.value.register_value_producer(
            'vitamin_a_fortification.coverage_level',
            source=coverage)
        self.effectively_covered = builder.value.register_value_producer(
            'vitamin_a_fortification.effectively_covered',
            source=self.get_effectively_covered)

        time_to_effect_data = self.load_time_to_effect_data(builder)
        self.time_to_effect = builder.lookup.build_table(time_to_effect_data)

        self.randomness = builder.randomness.get_stream(self.name)
        columns_created = [project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN,
                           project_globals.VITAMIN_A_COVERAGE_START_COLUMN]
        self.population_view = builder.population.get_view(columns_created + ['tracked'])
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created,
                                                 requires_values=['vitamin_a_fortification.coverage_level'],
                                                 requires_streams=[self.name])
        builder.event.register_listener('time_step', self.on_time_step)

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        """Create coverage propensity and coverage start time columns."""
        propensity = self.randomness.get_draw(pop_data.index)
        is_covered = self.is_covered(propensity)
        pop_update = pd.DataFrame({
            project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN: propensity,
            project_globals.VITAMIN_A_COVERAGE_START_COLUMN: pd.NaT
        }, pop_data.index)
        pop_update.loc[is_covered, project_globals.VITAMIN_A_COVERAGE_START_COLUMN] = pd.Timestamp('1-1-1990')
        self.population_view.update(pop_update)

    def on_time_step(self, event: 'Event'):
        """Update coverage start time for all newly covered individuals."""
        pop = self.population_view.get(event.index, query='tracked == True and alive=="alive"')
        is_covered = self.is_covered(pop[project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN])
        not_previously_covered = pop[project_globals.VITAMIN_A_COVERAGE_START_COLUMN].isna()
        newly_covered = is_covered & not_previously_covered
        pop.loc[newly_covered, project_globals.VITAMIN_A_COVERAGE_START_COLUMN] = event.time
        self.population_view.update(pop)

    def get_effectively_covered(self, index: pd.Index) -> pd.Series:
        """Get's all people who are covered and whose coverage is impacting
        their vitamin a deficiency probability.

        """
        pop = self.population_view.get(index)
        curr_time = self.clock()
        coverage_start_time = pop[project_globals.VITAMIN_A_COVERAGE_START_COLUMN]
        time_to_effect = self.time_to_effect(index)
        # noinspection PyTypeChecker
        effectively_covered = (curr_time - coverage_start_time) > time_to_effect
        effectively_covered = effectively_covered.map({True: 'cat2', False: 'cat1'})
        effectively_covered.name = 'value'
        return effectively_covered

    def is_covered(self, propensity: pd.Series) -> pd.Series:
        """Helper method for finding covered people from their propensity."""
        coverage = self.coverage_level(propensity.index)
        # noinspection PyTypeChecker
        return propensity < coverage

    @staticmethod
    def load_coverage_data(builder: 'Builder') -> float:
        """Load baseline coverage."""
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_vitamin_a_coverage(location, draw, 'baseline')

    @staticmethod
    def load_time_to_effect_data(builder: 'Builder') -> float:
        """Load delay between fortification start and effective coverage."""
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_vitamin_a_time_to_effect(location, draw)


class VitaminAFortificationEffect:
    """Model of the impact of vitamin A fortification on vitamin A deficiency.

    This component models a multiplicative effect of fortified foods on
    vitamin A deficiency. Fortification does not apply to children under
    6 months old so they can never be at risk.

    """

    @property
    def name(self) -> str:
        """This component's canonical name."""
        return 'vitamin_a_fortification_effect'

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: 'Builder'):
        """Load data and construct value modifiers for this component."""
        relative_risk_data = self.load_relative_risk_data(builder)
        self.relative_risk = builder.lookup.build_table(relative_risk_data,
                                                        parameter_columns=['age'])
        paf_data = self.load_population_attributable_fraction_data(builder)
        self.population_attributable_fraction = builder.lookup.build_table(paf_data,
                                                                           parameter_columns=['age'])

        self.effectively_covered = builder.value.get_value('vitamin_a_fortification.effectively_covered')
        builder.value.register_value_modifier('vitamin_a_deficiency.exposure_parameters.paf',
                                              self.population_attributable_fraction,
                                              requires_columns=['age'])
        builder.value.register_value_modifier('vitamin_a_deficiency.exposure_parameters',
                                              self.adjust_vitamin_a_exposure_probability,
                                              requires_columns=['age'])

    def adjust_vitamin_a_exposure_probability(self, index: pd.Index, exposure_probability: pd.Series) -> pd.Series:
        """Value modifier for vitamin a deficiency exposure."""
        effectively_covered = self.effectively_covered(index)
        rr = self.relative_risk(index).lookup(index, effectively_covered)
        return exposure_probability * rr

    @staticmethod
    def load_relative_risk_data(builder: 'Builder') -> pd.DataFrame:
        """Load rr data for fortification on vitamin a deficiency."""
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        rr = sample_vitamin_a_relative_risk(location, draw)
        relative_risk = pd.DataFrame({
            'age_start': [0., 0.5],
            'age_end': [0.5, 10.],
            # cat2 is covered
            'cat2': [1, 1],
            'cat1': [1, rr],
        })
        return relative_risk

    @staticmethod
    def load_population_attributable_fraction_data(builder: 'Builder') -> pd.DataFrame:
        """Compute paf data for fortification on vitamin a deficiency."""
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        rr = sample_vitamin_a_relative_risk(location, draw)
        relative_risk = pd.DataFrame({
            'age_start': [0., 0.5],
            'age_end': [0.5, 10.],
            # key is whether a person is covered by fortification
            'cat2': [1, 1],
            'cat1': [1, rr],
        }).set_index(['age_start', 'age_end'])
        coverage = sample_vitamin_a_coverage(location, draw, 'baseline')
        exposure = 1 - coverage
        mean_rr = relative_risk.loc[:, 'cat1']*exposure + relative_risk.loc[:, 'cat2']*(1-exposure)
        paf = (mean_rr - 1)/mean_rr
        return paf.reset_index()
