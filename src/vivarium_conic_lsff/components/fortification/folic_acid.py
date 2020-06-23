import typing

import pandas as pd
import numpy as np
import scipy.stats

from vivarium.framework.randomness import get_hash

from vivarium_conic_lsff import globals as project_globals
from vivarium_conic_lsff.components.fortification import parameters as params

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
            source=self.iron_coverage_level
        )

        iron_content_ratio = self.load_iron_content_ratio(builder)
        self.iron_amount_distribution = IronAmountDistribution(params.FLOUR_QUANTILES, iron_content_ratio)
        self.iron_amount = builder.value.register_value_producer(
            'iron_fortification.iron_amount', source=self.get_iron_amount)

        # common randomness source
        self._common_key = project_globals.IRON_FOLIC_ACID_RANDOMNESS
        self.randomness = builder.randomness.get_stream(self._common_key)

        # tracking columns for maternal fortification status and coverage age start
        self._fa_column = project_globals.FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN
        self._iron_fortified_mom_column = project_globals.IRON_FORTIFICATION_COVERAGE_MOM_COLUMN
        self._iron_coverage_start_age = project_globals.IRON_COVERAGE_START_AGE_COLUMN
        self._iron_fort_propensity = project_globals.IRON_FORTIFICATION_PROPENSITY_COLUMN
        self._iron_fort_food_consumption = project_globals.IRON_FORTIFICATION_FOOD_CONSUMPTION
        created_columns = [self._fa_column, self._iron_fortified_mom_column,
                           self._iron_coverage_start_age, self._iron_fort_propensity,
                           self._iron_fort_food_consumption]

        self.population_view = builder.population.get_view(created_columns + ['age'])

        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=created_columns,
                                                 requires_values=['folic_acid_fortification.effective_coverage_level',
                                                                  'iron_fortification.effective_coverage_level'],
                                                 requires_streams=[self._common_key])
        builder.event.register_listener('time_step', self.on_time_step)

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        pop_update = pd.DataFrame()
        draw = self.randomness.get_draw(pop_data.index)
        update_iron_coverage_start_age = pd.Series(np.nan, index=pop_data.index, name=self._iron_coverage_start_age)
        pop_age = self.population_view.subview(['age']).get(pop_data.index)

        if pop_data.user_data['sim_state'] == 'setup':  # Initial population
            update_maternal_folic_acid = pd.Series('unknown', index=pop_data.index, name=self._fa_column)
            update_maternal_iron = pd.Series('unknown', index=pop_data.index, name=self._iron_fortified_mom_column)
            iron_covered = self.is_iron_covered(draw)
            update_iron_amount = self.iron_amount(pop_data.index, iron_covered)
            pop_update[self._iron_fort_propensity] = draw
        else:  # New sims
            effective_coverage_fa = self.fa_effective_coverage_level(pop_data.index)
            update_maternal_folic_acid = pd.Series((draw < effective_coverage_fa).map({True: 'covered', False: 'uncovered'}),
                                                   index=pop_data.index,
                                                   name=self._fa_column)
            iron_covered = draw < self.iron_effective_coverage_level(pop_data.index)
            update_maternal_iron = pd.Series((iron_covered).map({True: 'covered', False: 'uncovered'}),
                                             index=pop_data.index,
                                             name=self._iron_fortified_mom_column)
            update_iron_amount = self.iron_amount(pop_data.index, iron_covered)

        update_iron_coverage_start_age.loc[iron_covered] = pop_age.loc[iron_covered].age

        pop_update[self._fa_column] = update_maternal_folic_acid
        pop_update[self._iron_fortified_mom_column] = update_maternal_iron
        pop_update[self._iron_coverage_start_age] = update_iron_coverage_start_age
        pop_update[self._iron_fort_food_consumption] = update_iron_amount

        self.population_view.update(pop_update)

    def on_time_step(self, event: 'Event'):
        """Update coverage start age for all newly covered individuals.
        """
        pop = self.population_view.get(event.index, query='alive=="alive"')
        is_covered = self.is_iron_covered(pop[self._iron_fort_propensity])
        not_previously_covered = pop[self._iron_coverage_start_age].isna()
        newly_covered = is_covered & not_previously_covered
        pop.loc[newly_covered, self._iron_coverage_start_age] = pop.loc[newly_covered].age
        self.population_view.update(pop)

    def is_iron_covered(self, propensity: pd.Series) -> pd.Series:
        """Helper method for finding covered people from their propensity."""
        coverage = self.iron_coverage_level(propensity.index)
        # noinspection PyTypeChecker
        return propensity < coverage

    def get_iron_amount(self, index, iron_covered):
        iron_amounts = pd.Series(0.0, index=index)
        draw_iron_consumption = self.randomness.get_draw(index, additional_key='iron_consumption')
        iron_amounts[iron_covered] = self.iron_amount_distribution.ppf(draw_iron_consumption)
        return iron_amounts

    @staticmethod
    def load_coverage_data_iron(builder: 'Builder') -> float:
        return params.sample_iron_fortification_coverage(
            builder.configuration.input_data.location,
            builder.configuration.input_data.input_draw_number,
            'baseline')

    @staticmethod
    def load_coverage_data_folic_acid(builder: 'Builder') -> float:
        return params.sample_folic_acid_coverage(
            builder.configuration.input_data.location,
            builder.configuration.input_data.input_draw_number,
            'baseline')

    @staticmethod
    def load_iron_content_ratio(builder: 'Builder') -> float:
        return iron_content_ratio(
            builder.configuration.input_data.input_draw_number,
            builder.configuration.input_data.location)


def iron_content_ratio(draw: str, location: str) -> float:
    """ Used from both the coverage and maternal fortification effect """
    seed = get_hash(project_globals.IRON_RANDOM_SEEDS.IF_AMOUNT.format(draw=draw, location=location))
    np.random.seed(seed)
    iron_lower, iron_upper = params.IRON_VALUES_PER_LOCATION[location]
    if iron_lower == iron_upper:
        return iron_upper
    else:
        return scipy.stats.uniform(iron_lower, iron_upper).rvs()


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
        not_covered = covered[covered != 'covered'].index
        birth_prevalence.loc[not_covered] *= self.relative_risk(not_covered)
        return birth_prevalence

    @staticmethod
    def load_relative_risk_data(builder: 'Builder') -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return params.sample_folic_acid_relative_risk(location, draw)

    @staticmethod
    def load_population_attributable_fraction_data(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        rr = params.sample_folic_acid_relative_risk(location, draw)
        coverage = params.sample_folic_acid_coverage(location, draw, 'baseline')
        exposure = 1 - coverage
        mean_rr = rr * exposure + 1 * (1 - exposure)
        paf = (mean_rr - 1) / mean_rr
        return paf


class MaternalIronFortificationEffect:

    @property
    def name(self):
        return 'maternal_iron_fortification_effect'

    def setup(self, builder: 'Builder'):
        self.treatment_effects = self.load_treatment_effects(builder)
        builder.value.register_value_modifier(f'low_birth_weight_and_short_gestation.exposure',
                                              self.adjust_birth_weights)
        self._fortified = project_globals.IRON_FORTIFICATION_COVERAGE_MOM_COLUMN
        self._iron_consumed = project_globals.IRON_FORTIFICATION_FOOD_CONSUMPTION
        self.population_view = builder.population.get_view([self._fortified, self._iron_consumed])

    def adjust_birth_weights(self, index, birth_weights):
        fortified_status = self.population_view.get(index)[self._fortified]
        idx_fortified = fortified_status.loc[fortified_status == 'covered'].index
        effect_fortified, baseline_shift = self.treatment_effects
        birth_weights.loc[index, 'birth_weight'] -= baseline_shift
        if len(idx_fortified):
            elemental_iron = self.population_view.get(index)[self._iron_consumed].loc[idx_fortified]
            bw_shift_fortified = elemental_iron * effect_fortified
            birth_weights.loc[idx_fortified, 'birth_weight'] += bw_shift_fortified
        return birth_weights

    @staticmethod
    def load_treatment_effects(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        baseline_iron_coverage = params.sample_iron_fortification_coverage(location, draw, 'baseline')
        iron_ratio = iron_content_ratio(draw, location)
        iron_effect = get_iron_bw_effect(draw, location)
        baseline_shift = iron_effect * baseline_iron_coverage * params.MEAN_FLOUR_CONSUMPTION * iron_ratio
        return (iron_effect, baseline_shift)


def get_iron_bw_effect(draw, location):
    seed = get_hash(project_globals.IRON_RANDOM_SEEDS.IF_BW_SHIFT.format(draw=draw, location=location))
    np.random.seed(seed)
    q_975_stdnorm = scipy.stats.norm().ppf(0.975)
    std = (params.IF_Q975_BW_SHIFT - params.IF_MEAN_BW_SHIFT) / q_975_stdnorm
    return scipy.stats.norm(params.IF_MEAN_BW_SHIFT, std).rvs() / params.IRON_EFFECT_DENOMINATOR


class IronAmountDistribution():
    def __init__(self, flour_quantiles, iron_ratio: float):
        self._flour_quantiles = flour_quantiles
        self._iron_ratio = iron_ratio

    def ppf(self, propensity: pd.Series) -> pd.Series:
        flour_consumption = pd.Series(0.0, index=propensity.index)
        first_quartile = propensity < 0.25
        first_quartile_p = propensity.loc[first_quartile]
        flour_consumption.loc[first_quartile] = (first_quartile_p / 0.25) * self._flour_quantiles.Q1

        second_quartile = ((0.25 <= propensity) & (propensity < 0.50))
        second_quartile_p = propensity.loc[second_quartile]
        flour_consumption.loc[second_quartile] = (((second_quartile_p - 0.25) / 0.25)
                                                  * (self._flour_quantiles.Q2 - self._flour_quantiles.Q1)
                                                  + self._flour_quantiles.Q1)

        third_quartile = ((0.50 <= propensity) & (propensity < 0.75))
        third_quartile_p = propensity.loc[third_quartile]
        flour_consumption.loc[third_quartile] = (((third_quartile_p - 0.50) / 0.25)
                                                 * (self._flour_quantiles.Q3 - self._flour_quantiles.Q2)
                                                 + self._flour_quantiles.Q2)

        fourth_quartile = (0.75 <= propensity)
        fourth_quartile_p = propensity.loc[fourth_quartile]
        flour_consumption.loc[fourth_quartile] = (((fourth_quartile_p - 0.75) / 0.25)
                                                  * (self._flour_quantiles.Q4 - self._flour_quantiles.Q3)
                                                  + self._flour_quantiles.Q3)

        return flour_consumption * self._iron_ratio


class HemoglobinIronFortificationEffect:

    @property
    def name(self):
        return 'hemoglobin_iron_fortification_effect'

    def setup(self, builder: 'Builder'):
        self.treatment_effects = self.load_treatment_effects(builder)
        columns_required = ['age', project_globals.IRON_COVERAGE_START_AGE_COLUMN]
        builder.value.register_value_modifier(f'{project_globals.IRON_DEFICIENCY_MODEL_NAME}.exposure',
                                              self.adjust_hemoglobin_levels,
                                              requires_columns=columns_required)
        self.population_view = builder.population.get_view(columns_required)
        self.iron_responsive = builder.value.get_value('iron_responsive')

    def adjust_hemoglobin_levels(self, index, hemoglobin_levels):
        baseline_shift, hemoglobin_fortification_effect = self.treatment_effects
        pop_data = self.population_view.get(index)
        hemoglobin_levels[index] -= baseline_shift
        idx_covered = pop_data.loc[(pop_data.age > 0.5)
                                   & (~pop_data.get(project_globals.IRON_COVERAGE_START_AGE_COLUMN).isnull())
                                   & self.iron_responsive(index)].index
        shift = self.compute_shift(pop_data.loc[idx_covered], hemoglobin_fortification_effect)
        hemoglobin_levels.loc[idx_covered] += shift
        return hemoglobin_levels

    def compute_shift(self, pop_data: pd.DataFrame, hemoglobin_fortification_effect : float) -> pd.Series:
        shift = pd.Series(0.0, index=pop_data.index)
        coverage_start_age = pop_data.get(project_globals.IRON_COVERAGE_START_AGE_COLUMN)
        coverage_duration = pop_data.age - coverage_start_age

        idx_young_short_coverage = pop_data.loc[
            (coverage_start_age < 1.5) & (coverage_duration < 0.5)].index
        idx_young_long_coverage = pop_data.loc[
            (coverage_start_age < 1.5) & (coverage_duration >= 0.5)].index
        idx_older_short_coverage = pop_data.loc[
            (coverage_start_age >= 1.5) & (coverage_duration < 0.5)].index
        idx_older_long_coverage = pop_data.loc[
            (coverage_start_age >= 1.5) & (coverage_duration >= 0.5)].index

        shift.loc[idx_young_short_coverage] = (hemoglobin_fortification_effect
                                               * (pop_data.loc[idx_young_short_coverage].age / 1.5)
                                               * (coverage_duration.loc[idx_young_short_coverage] / 0.5))
        shift.loc[idx_young_long_coverage] = (hemoglobin_fortification_effect
                                              * (pop_data.loc[idx_young_long_coverage].age - 0.5) / 1.5)
        shift.loc[idx_older_short_coverage] = (hemoglobin_fortification_effect
                                               * (pop_data.loc[idx_older_short_coverage].age - 0.5))
        shift.loc[idx_older_long_coverage] = hemoglobin_fortification_effect

        return shift

    @staticmethod
    def load_treatment_effects(builder: 'Builder'):
        baseline_iron_coverage = params.sample_iron_fortification_coverage(
            builder.configuration.input_data.location,
            builder.configuration.input_data.input_draw_number, 'baseline')
        iron_hemoglobin_effect = get_iron_hemoglobin_effect(
            builder.configuration.input_data.input_draw_number)
        baseline_shift = baseline_iron_coverage * iron_hemoglobin_effect
        return iron_hemoglobin_effect, baseline_shift


def get_iron_hemoglobin_effect(draw: int):
    """Return normal distribution of hemoglobin shifts resulting from iron fortification"""
    seed = get_hash(project_globals.IRON_RANDOM_SEEDS.IF_HEMO_EFFECT.format(draw=draw))
    np.random.seed(seed)
    q_975_stdnorm = scipy.stats.norm().ppf(0.975)
    std = (params.HEMOGLOBIN_SHIFT_Q_975 - params.HEMOGLOBIN_SHIFT_MEAN) / q_975_stdnorm
    return scipy.stats.norm(params.HEMOGLOBIN_SHIFT_MEAN, std).rvs()
