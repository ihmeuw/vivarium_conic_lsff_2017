import pandas as pd

from typing import NamedTuple

from vivarium.framework.randomness import get_hash

from vivarium_conic_lsff.utilities import (BetaParams, sample_beta_distribution,
                                           LogNormParams, sample_lognormal_distribution,
                                           )

from vivarium_conic_lsff import globals as project_globals

FOLIC_ACID_COVERAGE = {
    'Ethiopia': [
        {
            'baseline': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.1,
                beta=9.9,
            ),
            'intervention_start': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.5,
                beta=3.1,
            ),
            'intervention_end': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.8,
                beta=2.36,
            ),
            'weight': 1,
        },
    ],
    'India': [
        {
            'baseline': BetaParams.from_statistics(
                mean=0.063,
                upper_bound=0.079,
                lower_bound=0.048,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.071,
                upper_bound=0.091,
                lower_bound=0.056,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.832,
                upper_bound=0.865,
                lower_bound=0.795,
            ),
            'weight': 1
        },
    ],
    'Nigeria': [
        {  # Kano state
            'baseline': BetaParams.from_statistics(
                mean=0.227,
                upper_bound=0.255,
                lower_bound=0.200,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.838,
                upper_bound=0.862,
                lower_bound=0.814,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.839,
                upper_bound=0.863,
                lower_bound=0.815,
            ),
            'weight': 4/25,
        },
        {  # Lagos state
            'baseline': BetaParams.from_statistics(
                mean=0.054,
                upper_bound=0.069,
                lower_bound=0.038
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.138,
                upper_bound=0.161,
                lower_bound=0.115,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.142,
                upper_bound=0.165,
                lower_bound=0.118
            ),
            'weight': 21/25,
        }
    ]
}

FOLIC_ACID_FORTIFICATION_RELATIVE_RISK = LogNormParams.from_statistics(
    median=1.71,
    upper_bound=2.04
)


def sample_folic_acid_coverage(location: str, draw: int, coverage_time: str) -> float:
    seed = get_hash(f'folic_acid_fortification_coverage_draw_{draw}_location_{location}')
    return sum([coverage_params['weight'] * sample_beta_distribution(seed, coverage_params[coverage_time])
                for coverage_params in FOLIC_ACID_COVERAGE[location]])


def sample_folic_acid_relative_risk(location: str, draw: int) -> float:
    seed = get_hash(f'folic_acid_fortification_relative_risk_draw_{draw}_location_{location}')
    return sample_lognormal_distribution(seed, FOLIC_ACID_FORTIFICATION_RELATIVE_RISK)


VITAMIN_A_COVERAGE = {
    'Ethiopia': [
        {
            'baseline': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.1,
                beta=9.9,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.440,
                upper_bound=0.540,
                lower_bound=0.340,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.550,
                upper_bound=0.650,
                lower_bound=0.450,
            ),
            'weight': 1,
        },
    ],
    'India': [
        {
            'baseline': BetaParams.from_statistics(
                mean=0.243,
                upper_bound=0.279,
                lower_bound=0.211,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.894,
                upper_bound=0.918,
                lower_bound=0.870,
            ),
            'intervention_end': BetaParams(
                upper_bound=1.0,
                lower_bound=1.0,
                alpha=1.0,
                beta=1.0
            ),
            'weight': 1
        },
    ],
    'Nigeria': [
        {  # Kano state
            'baseline': BetaParams.from_statistics(
                mean=0.076,
                upper_bound=0.059,
                lower_bound=0.094,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.359,
                upper_bound=0.391,
                lower_bound=0.327,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.984,
                upper_bound=0.992,
                lower_bound=0.976,
            ),
            'weight': 4/25,
        },
        {  # Lagos state
            'baseline': BetaParams.from_statistics(
                mean=0.072,
                upper_bound=0.089,
                lower_bound=0.055
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.227,
                upper_bound=0.255,
                lower_bound=0.199,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.986,
                upper_bound=0.993,
                lower_bound=0.978
            ),
            'weight': 21/25,
        }
    ]
}


VITAMIN_A_FORTIFICATION_RELATIVE_RISK = LogNormParams.from_statistics(
    median=2.22,
    upper_bound=5.26
)

VITAMIN_A_FORTIFICATION_TIME_TO_EFFECT = LogNormParams.from_statistics(
    median=5/12,
    upper_bound=1
)


def sample_vitamin_a_coverage(location: str, draw: int, coverage_time: str) -> float:
    seed = get_hash(f'vitamin_a_fortification_coverage_draw_{draw}_location_{location}')
    return sum([coverage_params['weight'] * sample_beta_distribution(seed, coverage_params[coverage_time])
                for coverage_params in VITAMIN_A_COVERAGE[location]])


def sample_vitamin_a_relative_risk(location: str, draw: int) -> float:
    seed = get_hash(f'vitamin_a_fortification_relative_risk_draw_{draw}_location_{location}')
    return sample_lognormal_distribution(seed, VITAMIN_A_FORTIFICATION_RELATIVE_RISK)


def sample_vitamin_a_time_to_effect(location: str, draw: int) -> float:
    seed = get_hash(f'vitamin_a_fortification_time_to_effect_draw_{draw}_location_{location}')
    return sample_lognormal_distribution(seed, VITAMIN_A_FORTIFICATION_TIME_TO_EFFECT)


#
#   Iron Parameters
#
IRON_FORTIFICATION_COVERAGE = {
    'Ethiopia': [
        {
            'baseline': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.1,
                beta=9.9,
            ),
            'intervention_start': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.5,
                beta=3.1,
            ),
            'intervention_end': BetaParams(
                upper_bound=1,
                lower_bound=0,
                alpha=0.8,
                beta=2.36,
            ),
            'weight': 1,
        },
    ],
    'India': [
        {
            'baseline': BetaParams.from_statistics(
                mean=0.063,
                upper_bound=0.079,
                lower_bound=0.048,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.071,
                upper_bound=0.091,
                lower_bound=0.056,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.832,
                upper_bound=0.865,
                lower_bound=0.795,
            ),
            'weight': 1
        },
    ],
    'Nigeria': [
        {  # Kano state
            'baseline': BetaParams.from_statistics(
                mean=0.227,
                upper_bound=0.255,
                lower_bound=0.200,
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.838,
                upper_bound=0.862,
                lower_bound=0.814,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.839,
                upper_bound=0.863,
                lower_bound=0.815,
            ),
            'weight': 4/25,
        },
        {  # Lagos state
            'baseline': BetaParams.from_statistics(
                mean=0.054,
                upper_bound=0.069,
                lower_bound=0.038
            ),
            'intervention_start': BetaParams.from_statistics(
                mean=0.138,
                upper_bound=0.161,
                lower_bound=0.115,
            ),
            'intervention_end': BetaParams.from_statistics(
                mean=0.142,
                upper_bound=0.165,
                lower_bound=0.118
            ),
            'weight': 21/25,
        }
    ]
}

HEMOGLOBIN_SHIFT_MEAN = 3.0
HEMOGLOBIN_SHIFT_Q_975 = 6.1

IF_MEAN_BW_SHIFT = 15.1
IF_Q975_BW_SHIFT = 24.2

IRON_VALUES_PER_LOCATION = {
    # grams per kilogram
    project_globals.LOCATIONS.ETHIOPIA: (0.030, 0.030),
    project_globals.LOCATIONS.INDIA: (0.014, 0.0215),
    project_globals.LOCATIONS.NIGERIA: (0.040, 0.040)
}

class __FlourQuantiles(NamedTuple):
    Q0: float = 0
    Q1: float = 77.5
    Q2: float = 100
    Q3: float = 200
    Q4: float = 350.5

FLOUR_QUANTILES = __FlourQuantiles()


IRON_FORTIFICATION_RELATIVE_RISK = LogNormParams.from_statistics(
    median=1.71,
    upper_bound=2.04
)

IRON_EFFECT_DENOMINATOR = 10

def sample_iron_fortification_coverage(location: str, draw: int, coverage_time: str) -> float:
    seed = get_hash(f'iron_fortification_coverage_draw_{draw}_location_{location}')
    return sum([coverage_params['weight'] * sample_beta_distribution(seed, coverage_params[coverage_time])
                for coverage_params in IRON_FORTIFICATION_COVERAGE[location]])

MEAN_FLOUR_CONSUMPTION = (
    ((FLOUR_QUANTILES.Q0 + (FLOUR_QUANTILES.Q1 - FLOUR_QUANTILES.Q0) / 2) +
    (FLOUR_QUANTILES.Q1 + (FLOUR_QUANTILES.Q2 - FLOUR_QUANTILES.Q1) / 2) +
    (FLOUR_QUANTILES.Q2 + (FLOUR_QUANTILES.Q3 - FLOUR_QUANTILES.Q2) / 2) +
    (FLOUR_QUANTILES.Q3 + (FLOUR_QUANTILES.Q4 - FLOUR_QUANTILES.Q3) / 2)) / 4)





