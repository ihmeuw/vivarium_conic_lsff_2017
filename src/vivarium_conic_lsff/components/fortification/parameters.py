import pandas as pd
from vivarium.framework.randomness import get_hash

from vivarium_conic_lsff.utilities import (BetaParams, sample_beta_distribution,
                                           LogNormParams, sample_lognormal_distribution)


FOLIC_ACID_DELAY = pd.Timedelta(days=365.25)
FOLIC_ACID_ANNUAL_PROPORTION_INCREASE = 0.1
FOLIC_ACID_FORTIFICATION_COVERAGE_COLUMN = 'mother_ate_folic_acid_fortified_food'

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

RELATIVE_RISK = LogNormParams(
    median=1.71,
    upper_bound=2.04
)


def sample_folic_acid_coverage(location: str, draw: int, coverage_time: str) -> float:
    seed = get_hash(f'folic_acid_fortification_coverage_draw_{draw}_location_{location}')
    return sum([coverage_params['weight'] * sample_beta_distribution(seed, coverage_params[coverage_time])
                for coverage_params in FOLIC_ACID_COVERAGE[location]])


def sample_folic_acid_relative_risk(location: str, draw: int) -> float:
    seed = get_hash(f'folic_acid_fortification_relative_risk_draw_{draw}_location_{location}')
    return sample_lognormal_distribution(seed, RELATIVE_RISK)


