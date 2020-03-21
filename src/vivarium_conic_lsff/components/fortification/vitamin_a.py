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

    @property
    def name(self):
        return 'vitamin_a_fortification_coverage'

    def setup(self, builder: 'Builder'):
        self.clock = builder.time.clock()

        coverage_data = self.load_coverage_data(builder)
        coverage = builder.lookup.build_table(coverage_data)
        self.coverage_level = builder.value.register_value_producer(
            'vitamin_a_fortification.coverage_level',
            source = coverage)
        self.effectively_covered = builder.value.register_value_producer(
            'vitamin_a_fortification.effectively_covered',
            source=self.get_effectively_covered)

        time_to_effect_data = self.load_time_to_effect_data(builder)
        self.time_to_effect = builder.lookup.build_table(time_to_effect_data)

        self.randomness = builder.randomness.get_stream(self.name)
        columns_created = [project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN,
                           project_globals.VITAMIN_A_COVERAGE_START_COLUMN]
        self.population_view = builder.population.get_view(columns_created)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created,
                                                 requires_values=['vitamin_a_fortification.coverage_level'],
                                                 requires_streams=[self.name])
        builder.event.register_listener('time_step', self.on_time_step)

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        propensity = self.randomness.get_draw(pop_data.index)
        cov = self.coverage_level(pop_data.index)
        is_covered = cov < propensity
        pop_update = pd.DataFrame({
            project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN: propensity,
            project_globals.VITAMIN_A_COVERAGE_START_COLUMN: pd.NaT
        }, pop_data.index)
        pop_update.loc[is_covered, project_globals.VITAMIN_A_COVERAGE_START_COLUMN] = pd.Timestamp('1-1-1990')
        self.population_view.update(pop_update)

    def on_time_step(self, event: 'Event'):
        pop = self.population_view.get(event.index, query='alive=="alive"')
        cov = self.coverage_level(event.index)
        propensity = pop[project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN]
        is_covered = cov < propensity
        not_previously_covered = pop[project_globals.VITAMIN_A_COVERAGE_START_COLUMN].isna()
        newly_covered = is_covered & not_previously_covered
        pop.loc[newly_covered, project_globals.VITAMIN_A_COVERAGE_START_COLUMN] = event.time
        self.population_view.update(pop)

    def get_effectively_covered(self, index: pd.Index):
        pop = self.population_view.get(index)
        curr_time = self.clock()
        coverage_start_time = pop[project_globals.VITAMIN_A_COVERAGE_START_COLUMN]
        time_to_effect = self.time_to_effect(index)
        effectively_covered = (curr_time - coverage_start_time) > time_to_effect
        effectively_covered.name = 'value'
        return effectively_covered


    @staticmethod
    def load_coverage_data(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_vitamin_a_coverage(location, draw, 'baseline')

    @staticmethod
    def load_time_to_effect_data(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_vitamin_a_time_to_effect(location, draw)
