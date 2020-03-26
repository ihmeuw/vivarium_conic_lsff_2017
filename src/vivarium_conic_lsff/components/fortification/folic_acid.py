import typing

import pandas as pd

from vivarium_conic_lsff.components.fortification.parameters import (sample_folic_acid_coverage,
                                                                     sample_folic_acid_relative_risk,
                                                                     sample_iron_fortification_coverage)
from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.population import SimulantData



class FolicAcidAndIronFortificationCoverage:

    @property
    def name(self):
        return 'folic_acid_fortification_coverage'

    def setup(self, builder: 'Builder'):
        fa_coverage_data = self.load_coverage_data_folic_acid(builder)
        fa_coverage = builder.lookup.build_table(fa_coverage_data)
        self.fa_coverage_level = builder.value.register_value_producer(
            'folic_acid_fortification.coverage_level',
            source=fa_coverage)
        self.fa_effective_coverage_level = builder.value.register_value_producer(
            'folic_acid_fortification.effective_coverage_level',
            source=fa_coverage
        )
        
        iron_coverage_data = self.load_coverage_data_iron(builder)
        iron_coverage = builder.lookup.build_table(iron_coverage_data)
        self.iron_coverage_level = builder.value.register_value_producer(
            'iron_fortification.coverage_level',
            source=iron_coverage)
        self.iron_effective_coverage_level = builder.value.register_value_producer(
            'iron_fortification.effective_coverage_level',
            source=iron_coverage
        )

        # common randomness source
        self._common_key = project_globals.IRON_FOLIC_ACID_RANDOMNESS
        self.randomness = builder.randomness.get_stream(self._common_key)

        # tracking columns for maternal fortification status
        self._fa_column = project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN
        self._iron_column = project_globals.IRON_FORTIFICATION_COVERAGE_COLUMN

        self.population_view = builder.population.get_view([self._fa_column, self._iron_column])

        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=[self._fa_column, self._iron_column],
                                                 requires_values=['folic_acid_fortification.effective_coverage_level',
                                                                  'iron_fortification.effective_coverage_level'],
                                                 requires_streams=[self._common_key])

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        if pop_data.user_data['sim_state'] == 'setup':  # Initial population
            update_folic_acid = pd.Series('unknown', index=pop_data.index, name=self._fa_column)
            update_iron = pd.Series('unknown', index=pop_data.index, name=self._iron_column)
        else:  # New sims
            draw = self.randomness.get_draw(pop_data.index)
            effective_coverage_fa = self.fa_effective_coverage_level(pop_data.index)
            update_folic_acid = pd.Series((draw < effective_coverage_fa).map({True: 'covered', False: 'uncovered'}),
                                   index=pop_data.index,
                                   name=self._fa_column)
            effective_coverage_iron = self.iron_effective_coverage_level(pop_data.index)
            update_iron = pd.Series((draw < effective_coverage_iron).map({True: 'covered', False: 'uncovered'}),
                                   index=pop_data.index,
                                   name=self._iron_column)
        pop_update = pd.concat([update_folic_acid, update_iron], axis=1, keys=[self._fa_column, self._iron_column])
        self.population_view.update(pop_update)

    @staticmethod
    def load_coverage_data_folic_acid(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_coverage(location, draw, 'baseline')

    @staticmethod
    def load_coverage_data_iron(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_iron_fortification_coverage(location, draw, 'baseline')


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
