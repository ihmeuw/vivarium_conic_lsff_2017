import typing

import pandas as pd

from vivarium_conic_lsff.components.fortification.parameters import (sample_folic_acid_coverage,
                                                                     sample_folic_acid_relative_risk)
from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.population import SimulantData


class FolicAcidFortificationCoverage:

    @property
    def name(self):
        return 'folic_acid_fortification_coverage'

    def setup(self, builder: 'Builder'):
        coverage_data = self.load_coverage_data(builder)
        coverage = builder.lookup.build_table(coverage_data)
        self.coverage_level = builder.value.register_value_producer(
            'folic_acid_fortification.coverage_level',
            source=coverage)
        self.effective_coverage_level = builder.value.register_value_producer(
            'folic_acid_fortification.effective_coverage_level',
            source=coverage
        )

        self._column = project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN
        self.randomness = builder.randomness.get_stream(self._column)
        self.population_view = builder.population.get_view([self._column])
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=[self._column],
                                                 requires_values=['folic_acid_fortification.effective_coverage_level'],
                                                 requires_streams=[self._column])

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        if pop_data.user_data['sim_state'] == 'setup':  # Initial population
            pop_update = pd.Series('unknown', index=pop_data.index, name=self._column)
        else:  # New sims
            draw = self.randomness.get_draw(pop_data.index)
            effective_coverage = self.effective_coverage_level(pop_data.index)
            pop_update = pd.Series((draw < effective_coverage).map({True: 'covered', False: 'uncovered'}),
                                   index=pop_data.index,
                                   name=self._column)
        self.population_view.update(pop_update)

    @staticmethod
    def load_coverage_data(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_coverage(location, draw, 'baseline')


class FolicAcidFortificationEffect:

    @property
    def name(self):
        return 'folic_acid_fortification_effect'

    def setup(self, builder: 'Builder'):
        relative_risk_data = self.load_relative_risk_data(builder)
        self.relative_risk = builder.lookup.build_table(relative_risk_data)
        paf_data = self.load_population_attributable_fraction_data(builder)
        self.population_attributable_fraction = builder.lookup.build_table(paf_data)
        builder.value.register_value_modifier('neural_tube_defects.birth_prevalence.population_attributable_fraction',
                                              self.population_attributable_fraction)
        builder.value.register_value_modifier('neural_tube_defects.birth_prevalence',
                                              self.adjust_birth_prevalence)

        self._column = project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN
        self.population_view = builder.population.get_view([self._column])

    def adjust_birth_prevalence(self, index, birth_prevalence):
        covered = self.population_view.get(index)[self._column]
        not_covered = covered[covered == 'false'].index
        birth_prevalence.loc[not_covered] *= self.relative_risk(not_covered)
        return birth_prevalence

    @staticmethod
    def load_relative_risk_data(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_relative_risk(location, draw)

    @staticmethod
    def load_population_attributable_fraction_data(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        rr = sample_folic_acid_relative_risk(location, draw)
        coverage = sample_folic_acid_coverage(location, draw, 'baseline')
        exposure = 1 - coverage
        mean_rr = rr*exposure + 1*(1-exposure)
        paf = (mean_rr - 1)/mean_rr
        return paf
