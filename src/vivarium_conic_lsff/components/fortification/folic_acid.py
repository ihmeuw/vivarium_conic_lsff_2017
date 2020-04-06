import typing

import pandas as pd
import numpy as np
import scipy.stats

from vivarium.framework.randomness import get_hash
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

        self.flour_consumption = FlourConsumptionDistribution()

        # common randomness source
        self._common_key = project_globals.IRON_FOLIC_ACID_RANDOMNESS
        self.randomness = builder.randomness.get_stream(self._common_key)

        # tracking columns for maternal fortification status and coverage age start
        self._fa_column = project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN
        self._iron_fortified_column = project_globals.IRON_FORTIFICATION_COVERAGE_COLUMN
        self._iron_coverage_start_time = project_globals.IRON_COVERAGE_START_TIME_COLUMN
        self._iron_fort_propensity = project_globals.IRON_FORTIFICATION_PROPENSITY_COLUMN
        self._iron_fort_food_consumption = project_globals.IRON_FORTIFICATION_FOOD_CONSUMPTION
        created_columns = [self._fa_column, self._iron_fortified_column,
                           self._iron_coverage_start_time, self._iron_fort_propensity,
                           self._iron_fort_food_consumption]

        self.population_view = builder.population.get_view(created_columns)

        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=created_columns,
                                                 requires_values=['folic_acid_fortification.effective_coverage_level',
                                                                  'iron_fortification.effective_coverage_level'],
                                                 requires_streams=[self._common_key])
        builder.event.register_listener('time_step', self.on_time_step)


    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        pop_update = pd.DataFrame()
        draw = self.randomness.get_draw(pop_data.index)

        if pop_data.user_data['sim_state'] == 'setup':  # Initial population
            update_maternal_folic_acid = pd.Series('unknown', index=pop_data.index, name=self._fa_column)
            update_maternal_iron = pd.Series('unknown', index=pop_data.index, name=self._iron_fortified_column)

            is_covered_individual = self.is_covered(draw)
            update_iron_coverage_start_time = pd.Series((is_covered_individual).map({True: pop_data.creation_time, False: np.nan}),
                                                        index=pop_data.index,
                                                        name=self._iron_coverage_start_time)
            pop_update[self._iron_fort_propensity] = draw
        else:  # New sims
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
        pop_update[self._iron_fort_food_consumption] = self.flour_consumption.ppf(draw)

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


class MaternalIronFortificationEffect:

    @property
    def name(self):
        return 'maternal_iron_fortification_effect'

    def setup(self, builder: 'Builder'):
        self.treatment_effects = self.load_treatment_effects(builder)

        builder.value.register_value_modifier(f'low_birth_weight_and_short_gestation.exposure',
                                              self.adjust_birth_weights)

        self._fortified = project_globals.IRON_FORTIFICATION_COVERAGE_COLUMN
        self._amount_fortified_food = project_globals.IRON_FORTIFICATION_FOOD_CONSUMPTION
        self.population_view = builder.population.get_view([self._fortified, self._amount_fortified_food])

    def adjust_birth_weights(self, index, birth_weights):
        fortified_status = self.population_view.get(index)[self._fortified]
        idx_fortified = fortified_status.loc[fortified_status=='covered'].index
        if len(idx_fortified):
            amount_fortified_food = self.population_view.get(index)[self._amount_fortified_food]
            bw_shift = self.compute_shift(amount_fortified_food.loc[idx_fortified])
            birth_weights.loc[idx_fortified, 'birth_weight'] += bw_shift
        return birth_weights

    def compute_shift(self, amount_fortified_food: pd.Series) -> pd.Series:
        location, draw, amount_compound_iron, iron_effect = self.treatment_effects

        elemental_iron = amount_compound_iron * project_globals.IF_ELEMENTAL_IRON_RATIO
        iron_per_day = (amount_fortified_food / 1000) * elemental_iron

        seed = get_hash(f'iron_fortification_coverage_draw_{draw}_location_{location}')
        np.random.seed(seed)
        propensity = np.random.random(len(amount_fortified_food))
        shifts = iron_per_day * (iron_effect.ppf(propensity) / 10)

        return shifts


    @staticmethod
    def load_treatment_effects(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        amount_compound_iron = get_iron_content_for_location(location)
        iron_effect = get_iron_effect_distribution()
        return [
            location,
            draw,
            amount_compound_iron,
            iron_effect
        ]


def get_iron_content_for_location(loc: project_globals.SIM_LOCATIONS):
    '''Units are mg/kg of iron compound such as NaFeEDTA (sodium ferric ethylenediaminetetraacetate)
       as opposed to elemental iron.'''
    values_per_location = {
        project_globals.SIM_LOCATIONS.ETHIOPIA: (30,),
        project_globals.SIM_LOCATIONS.INDIA: (14, 21.5),
        project_globals.SIM_LOCATIONS.NIGERIA: (40,)
    }
    compound_iron = values_per_location[loc]
    if len(compound_iron) > 1:
        return np.random.uniform(compound_iron[0], compound_iron[1])
    else:
        return compound_iron[0]


def get_iron_effect_distribution():
    '''Return normal distribution of birthweight shifts resulting from iron fortification'''
    # 0.975-quantile of standard normal distribution (=1.96, approximately)
    q_975_stdnorm = scipy.stats.norm().ppf(0.975)
    std = (project_globals.IF_Q975_BW_SHIFT - project_globals.IF_MEAN_BW_SHIFT) / q_975_stdnorm
    return scipy.stats.norm(project_globals.IF_MEAN_BW_SHIFT, std)


class FlourConsumptionDistribution():
    q0 = 0
    q1 = 77.5
    q2 = 100
    q3 = 200
    q4 = 350.5

    def ppf(self, propensity: pd.Series) -> pd.Series:
        flour_consumption = pd.Series(0.0, index=propensity.index)
        first_quartile = propensity < 0.25
        first_quartile_p = propensity.loc[first_quartile]
        flour_consumption.loc[first_quartile] = (first_quartile_p / 0.25 ) * self.q1

        second_quartile = ((0.25 <= propensity) & (propensity < 0.50))
        second_quartile_p = propensity.loc[second_quartile]
        flour_consumption.loc[second_quartile] = ((second_quartile_p - 0.25) / 0.25) * (self.q2 - self.q1) + self.q1

        third_quartile = ((0.50 <= propensity) & (propensity < 0.75))
        third_quartile_p = propensity.loc[third_quartile]
        flour_consumption.loc[third_quartile] = ((third_quartile_p - 0.50) / 0.50) * (self.q3 - self.q2) + self.q2

        fourth_quartile = (0.75 <= propensity)
        fourth_quartile_p = propensity.loc[fourth_quartile]
        flour_consumption.loc[fourth_quartile] = ((fourth_quartile_p - 0.75) / 0.75) * (self.q4 - self.q3) + self.q3

        return flour_consumption

