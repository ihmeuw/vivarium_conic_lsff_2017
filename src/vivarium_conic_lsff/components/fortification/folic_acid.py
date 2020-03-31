import typing

import pandas as pd
import numpy as np

from vivarium_conic_lsff.components.fortification.parameters import (sample_folic_acid_coverage,
                                                                     sample_folic_acid_relative_risk,
                                                                     sample_iron_fortification_coverage)
from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.event import Event
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

        # tracking columns for maternal fortification status and coverage age start
        self._fa_column = project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN
        self._iron_fortified_column = project_globals.IRON_FORTIFICATION_COVERAGE_COLUMN
        self._iron_coverage_start_time = project_globals.IRON_COVERAGE_START_TIME_COLUMN
        self._iron_fort_propensity = project_globals.IRON_FORTIFICATION_PROPENSITY_COLUMN
        created_columns = [self._fa_column, self._iron_fortified_column,
                           self._iron_coverage_start_time, self._iron_fort_propensity]

        self.population_view = builder.population.get_view(created_columns)

        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=created_columns,
                                                 requires_values=['folic_acid_fortification.effective_coverage_level',
                                                                  'iron_fortification.effective_coverage_level'],
                                                 requires_streams=[self._common_key])
        builder.event.register_listener('time_step', self.on_time_step)


    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        pop_update = pd.DataFrame()
        if pop_data.user_data['sim_state'] == 'setup':  # Initial population
            update_maternal_folic_acid = pd.Series('unknown', index=pop_data.index, name=self._fa_column)
            update_maternal_iron = pd.Series('unknown', index=pop_data.index, name=self._iron_fortified_column)

            propensity = self.randomness.get_draw(pop_data.index)
            is_covered_individual = self.is_covered(propensity)
            update_iron_coverage_start_time = pd.Series((is_covered_individual).map({True: pop_data.creation_time, False: np.nan}),
                                                        index=pop_data.index,
                                                        name=self._iron_coverage_start_time)
            pop_update[self._iron_fort_propensity] = propensity
        else:  # New sims
            draw = self.randomness.get_draw(pop_data.index)

            effective_coverage_fa = self.fa_effective_coverage_level(pop_data.index)
            update_maternal_folic_acid = pd.Series((draw < effective_coverage_fa).map({True: 'covered', False: 'uncovered'}),
                                          index=pop_data.index,
                                          name=self._fa_column)

            effective_coverage_iron = self.iron_effective_coverage_level(pop_data.index)
            update_maternal_iron = pd.Series((draw < effective_coverage_iron).map({True: 'covered', False: 'uncovered'}),
                                    index=pop_data.index,
                                    name=self._iron_fortified_column)
            update_iron_coverage_start_time = pd.Series((update_maternal_iron=='covered').map({True: pop_data.creation_time, False: np.nan}),
                                                        index=pop_data.index,
                                                        name=self._iron_coverage_start_time)

        pop_update[self._fa_column] = update_maternal_folic_acid
        pop_update[self._iron_fortified_column] =  update_maternal_iron
        pop_update[self._iron_coverage_start_time] = update_iron_coverage_start_time

        self.population_view.update(pop_update)


    def on_time_step(self, event: 'Event'):
        """Update coverage start age for all newly covered individuals."""
        pop = self.population_view.get(event.index, query='tracked == True and alive=="alive"')
        is_covered = self.is_covered(pop[self._iron_fort_propensity])
        not_previously_covered = pop[self._iron_coverage_start_time].isna()
        newly_covered = is_covered & not_previously_covered
        pop.loc[newly_covered, self._iron_coverage_start_time] = event.time
        self.population_view.update(pop)

    def is_covered(self, propensity: pd.Series) -> pd.Series:
        """Helper method for finding covered people from their propensity."""
        coverage = self.iron_coverage_level(propensity.index)
        # noinspection PyTypeChecker
        return propensity < coverage

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
