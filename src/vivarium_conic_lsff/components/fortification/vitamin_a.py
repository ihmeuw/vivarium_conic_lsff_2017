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
            source=coverage)
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
        is_covered = self.is_covered(propensity)
        pop_update = pd.DataFrame({
            project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN: propensity,
            project_globals.VITAMIN_A_COVERAGE_START_COLUMN: pd.NaT
        }, pop_data.index)
        pop_update.loc[is_covered, project_globals.VITAMIN_A_COVERAGE_START_COLUMN] = pd.Timestamp('1-1-1990')
        self.population_view.update(pop_update)

    def on_time_step(self, event: 'Event'):
        pop = self.population_view.get(event.index, query='alive=="alive"')
        is_covered = self.is_covered(pop[project_globals.VITAMIN_A_FORTIFICATION_PROPENSITY_COLUMN])
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
        effectively_covered = effectively_covered.map({True: 'cat2', False: 'cat1'})
        effectively_covered.name = 'value'
        return effectively_covered

    def is_covered(self, propensity):
        coverage = self.coverage_level(propensity.index)
        return propensity < coverage

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


class VitaminAFortificationEffect:

    @property
    def name(self) -> str:
        return 'vitamin_a_fortification_effect'

    def setup(self, builder: 'Builder'):
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
        effectively_covered = self.effectively_covered(index)
        rr = self.relative_risk(index).lookup(index, effectively_covered)
        return exposure_probability * rr

    @staticmethod
    def load_relative_risk_data(builder):
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
    def load_population_attributable_fraction_data(builder: 'Builder'):
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
