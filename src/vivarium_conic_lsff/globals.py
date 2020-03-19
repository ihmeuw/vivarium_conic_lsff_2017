import itertools

from typing import NamedTuple

####################
# Project metadata #
####################

PROJECT_NAME = 'vivarium_conic_lsff'
CLUSTER_PROJECT = 'proj_cost_effect'

CLUSTER_QUEUE = 'all.q'
MAKE_ARTIFACT_MEM = '3G'
MAKE_ARTIFACT_CPU = '1'
MAKE_ARTIFACT_RUNTIME = '3:00:00'
MAKE_ARTIFACT_SLEEP = 10

LOCATIONS = [
    'India',
    'Nigeria',
    'Ethiopia',
]


#############
# Data Keys #
#############

METADATA_LOCATIONS = 'metadata.locations'

POPULATION_STRUCTURE = 'population.structure'
POPULATION_AGE_BINS = 'population.age_bins'
POPULATION_DEMOGRAPHY = 'population.demographic_dimensions'
POPULATION_TMRLE = 'population.theoretical_minimum_risk_life_expectancy'

ALL_CAUSE_CSMR = 'cause.all_causes.cause_specific_mortality_rate'

COVARIATE_LIVE_BIRTHS_BY_SEX = 'covariate.live_births_by_sex.estimate'

DIARRHEA_CAUSE_SPECIFIC_MORTALITY_RATE = 'cause.diarrheal_diseases.cause_specific_mortality_rate'
DIARRHEA_PREVALENCE = 'cause.diarrheal_diseases.prevalence'
DIARRHEA_INCIDENCE_RATE = 'cause.diarrheal_diseases.incidence_rate'
DIARRHEA_REMISSION_RATE = 'cause.diarrheal_diseases.remission_rate'
DIARRHEA_EXCESS_MORTALITY_RATE = 'cause.diarrheal_diseases.excess_mortality_rate'
DIARRHEA_DISABILITY_WEIGHT = 'cause.diarrheal_diseases.disability_weight'
DIARRHEA_RESTRICTIONS = 'cause.diarrheal_diseases.restrictions'

MEASLES_CAUSE_SPECIFIC_MORTALITY_RATE = 'cause.measles.cause_specific_mortality_rate'
MEASLES_PREVALENCE = 'cause.measles.prevalence'
MEASLES_INCIDENCE_RATE = 'cause.measles.incidence_rate'
MEASLES_EXCESS_MORTALITY_RATE = 'cause.measles.excess_mortality_rate'
MEASLES_DISABILITY_WEIGHT = 'cause.measles.disability_weight'
MEASLES_RESTRICTIONS = 'cause.measles.restrictions'

LRI_CAUSE_SPECIFIC_MORTALITY_RATE = 'cause.lower_respiratory_infections.cause_specific_mortality_rate'
LRI_PREVALENCE = 'cause.lower_respiratory_infections.prevalence'
LRI_INCIDENCE_RATE = 'cause.lower_respiratory_infections.incidence_rate'
LRI_REMISSION_RATE = 'cause.lower_respiratory_infections.remission_rate'
LRI_EXCESS_MORTALITY_RATE = 'cause.lower_respiratory_infections.excess_mortality_rate'
LRI_DISABILITY_WEIGHT = 'cause.lower_respiratory_infections.disability_weight'
LRI_RESTRICTIONS = 'cause.lower_respiratory_infections.restrictions'

NEURAL_TUBE_DEFECTS_CAUSE_SPECIFIC_MORTALITY_RATE = 'cause.neural_tube_defects.cause_specific_mortality_rate'
NEURAL_TUBE_DEFECTS_PREVALENCE = 'cause.neural_tube_defects.prevalence'
NEURAL_TUBE_DEFECTS_BIRTH_PREVALENCE = 'cause.neural_tube_defects.birth_prevalence'
NEURAL_TUBE_DEFECTS_EXCESS_MORTALITY_RATE = 'cause.neural_tube_defects.excess_mortality_rate'
NEURAL_TUBE_DEFECTS_DISABILITY_WEIGHT = 'cause.neural_tube_defects.disability_weight'
NEURAL_TUBE_DEFECTS_RESTRICTIONS = 'cause.neural_tube_defects.restrictions'

LBWSG_DISTRIBUTION = 'risk_factor.low_birth_weight_and_short_gestation.distribution'
LBWSG_CATEGORIES = 'risk_factor.low_birth_weight_and_short_gestation.categories'
LBWSG_EXPOSURE = 'risk_factor.low_birth_weight_and_short_gestation.exposure'
LBWSG_RELATIVE_RISK = 'risk_factor.low_birth_weight_and_short_gestation.relative_risk'
LBWSG_PAF = 'risk_factor.low_birth_weight_and_short_gestation.population_attributable_fraction'

# Note: no mortality associated with vitamin A deficiency in this model
VITAMIN_A_DEFICIENCY_CATEGORIES = 'risk_factor.vitamin_a_deficiency.categories'
VITAMIN_A_DEFICIENCY_EXPOSURE = 'risk_factor.vitamin_a_deficiency.exposure'
VITAMIN_A_DEFICIENCY_RELATIVE_RISK = 'risk_factor.vitamin_a_deficiency.relative_risk'
VITAMIN_A_DEFICIENCY_PAF = 'risk_factor.vitamin_a_deficiency.population_attributable_fraction'
VITAMIN_A_DEFICIENCY_DISTRIBUTION = 'risk_factor.vitamin_a_deficiency.distribution'
VITAMIN_A_DEFICIENCY_RESTRICTIONS = 'risk_factor.vitamin_a_deficiency.restrictions'
VITAMIN_A_DEFICIENCY_DISABILITY_WEIGHT = 'cause.vitamin_a_deficiency.disability_weight'

IRON_DEFICIENCY_EXPOSURE = 'risk_factor.iron_deficiency.exposure'
IRON_DEFICIENCY_EXPOSURE_SD = 'risk_factor.iron_deficiency.exposure_standard_deviation'

IRON_DEFICIENCY_NO_ANEMIA_IRON_RESPONSIVE_PROPORTION = 'risk_factor.iron_deficiency.no_anemia_iron_responsive_proportion'
IRON_DEFICIENCY_MILD_ANEMIA_IRON_RESPONSIVE_PROPORTION = 'risk_factor.iron_deficiency.mild_anemia_iron_responsive_proportion'
IRON_DEFICIENCY_MODERATE_ANEMIA_IRON_RESPONSIVE_PROPORTION = 'risk_factor.iron_deficiency.moderate_anemia_iron_responsive_proportion'
IRON_DEFICIENCY_SEVERE_ANEMIA_IRON_RESPONSIVE_PROPORTION = 'risk_factor.iron_deficiency.severe_anemia_iron_responsive_proportion'

IRON_DEFICIENCY_MILD_ANEMIA_DISABILITY_WEIGHT = 'risk_factor.iron_deficiency.mild_anemia_disability_weight'
IRON_DEFICIENCY_MODERATE_ANEMIA_DISABILITY_WEIGHT = 'risk_factor.iron_deficiency.moderate_anemia_disability_weight'
IRON_DEFICIENCY_SEVERE_ANEMIA_DISABILITY_WEIGHT = 'risk_factor.iron_deficiency.severe_anemia_disability_weight'

IRON_DEFICIENCY_RESTRICTIONS = 'risk_factor.iron_deficiency.restrictions'

ANEMIA_SEQUELAE_ID_MAP = {
    'mild': (
        # responsive
        [
            144,
            172,
            177,
            182,
            206,
            240,
            438,
            442,
            525,
            537,
            1004,
            1008,
            1012,
            1016,
            1020,
            1024,
            1028,
            1032,
            1106,
            1361,
            1373,
            1385,
            1397,
            1409,
            1421,
            1433,
            1445,
            4952,
            4955,
            4976,
            4985,
            4988,
            5009,
            5225,
            5228,
            5249,
            5252,
            5273,
            5276,
            5393,
            5567,
            5579,
            5627,
            5648,
            5651,
            5654,
            5678,
            5699,
            5702,
            7202,
            7214,
            22989,
            22990,
            22991,
            22992,
            22993,
            23030,
            23034,
            23038,
            23042,
            23046        ],
        # non_responsive
        [
            531,
            645,
            648,
            651,
            654,
            1057,
            1061,
            1065,
            1069,
            1079,
            1089,
            1099,
            1120,
            5018,
            5027,
            5036,
            5051,
            5063,
            5075,
            5087,
            5099,
            5111,
            5123,
            5606,
            5705,
        ]
    ),
    'moderate': (
        # responsive
        [145,
         173,
         178,
         183,
         207,
         241,
         439,
         443,
         526,
         538,
         1005,
         1009,
         1013,
         1017,
         1021,
         1025,
         1029,
         1033,
         1107,
         1364,
         1376,
         1388,
         1400,
         1412,
         1424,
         1436,
         1448,
         4958,
         4961,
         4979,
         4991,
         4994,
         5012,
         5219,
         5222,
         5243,
         5246,
         5267,
         5270,
         5396,
         5570,
         5582,
         5630,
         5657,
         5660,
         5663,
         5681,
         5708,
         5711,
         5714,
         7205,
         7217,
         22999,
         23000,
         23001,
         23002,
         23003,
         23031,
         23035,
         23039,
         23043,
         23047],
        # non_responsive
        [532,
         646,
         649,
         652,
         655,
         1058,
         1062,
         1066,
         1070,
         1080,
         1090,
         1100,
         1121,
         5021,
         5030,
         5039,
         5054,
         5066,
         5078,
         5090,
         5102,
         5114,
         5126,
         5609]
    ),
    'severe': (
        # responsive
        [146,
         174,
         179,
         184,
         208,
         242,
         440,
         444,
         527,
         539,
         1006,
         1010,
         1014,
         1018,
         1022,
         1026,
         1030,
         1034,
         1108,
         1367,
         1379,
         1391,
         1403,
         1415,
         1427,
         1439,
         1451,
         4964,
         4967,
         4982,
         4997,
         5000,
         5015,
         5213,
         5216,
         5237,
         5240,
         5261,
         5264,
         5399,
         5573,
         5585,
         5633,
         5666,
         5669,
         5672,
         5717,
         5720,
         5723,
         5684,
         7208,
         7220,
         23009,
         23010,
         23011,
         23012,
         23013,
         23032,
         23036,
         23040,
         23044,
         23048],
        # non_responsive
        [5129,
         533,
         1059,
         1060,
         1063,
         1064,
         1067,
         1068,
         1071,
         1074,
         1075,
         1077,
         1081,
         1083,
         1085,
         1087,
         1091,
         1093,
         1095,
         1097,
         1101,
         1122,
         647,
         650,
         653,
         656,
         5024,
         5033,
         5042,
         5057,
         5069,
         5081,
         5093,
         5612,
         5105,
         5117]
    )
}


###########################
# Disease Model variables #
###########################

DIARRHEA_MODEL_NAME = 'diarrheal_diseases'
DIARRHEA_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{DIARRHEA_MODEL_NAME}'
DIARRHEA_WITH_CONDITION_STATE_NAME = DIARRHEA_MODEL_NAME
DIARRHEA_MODEL_STATES = (DIARRHEA_SUSCEPTIBLE_STATE_NAME, DIARRHEA_WITH_CONDITION_STATE_NAME)
DIARRHEA_MODEL_TRANSITIONS = (
    f'{DIARRHEA_SUSCEPTIBLE_STATE_NAME}_TO_{DIARRHEA_WITH_CONDITION_STATE_NAME}',
    f'{DIARRHEA_WITH_CONDITION_STATE_NAME}_TO_{DIARRHEA_SUSCEPTIBLE_STATE_NAME}',
)

MEASLES_MODEL_NAME = 'measles'
MEASLES_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{MEASLES_MODEL_NAME}'
MEASLES_WITH_CONDITION_STATE_NAME = MEASLES_MODEL_NAME
MEASLES_MODEL_STATES = (MEASLES_SUSCEPTIBLE_STATE_NAME, MEASLES_WITH_CONDITION_STATE_NAME)
MEASLES_MODEL_TRANSITIONS = (
    f'{MEASLES_SUSCEPTIBLE_STATE_NAME}_TO_{MEASLES_WITH_CONDITION_STATE_NAME}',
    f'{MEASLES_WITH_CONDITION_STATE_NAME}_TO_{MEASLES_SUSCEPTIBLE_STATE_NAME}',
)

LRI_MODEL_NAME = 'lower_respiratory_infections'
LRI_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{LRI_MODEL_NAME}'
LRI_WITH_CONDITION_STATE_NAME = LRI_MODEL_NAME
LRI_MODEL_STATES = (LRI_SUSCEPTIBLE_STATE_NAME, LRI_WITH_CONDITION_STATE_NAME)
LRI_MODEL_TRANSITIONS = (
    f'{LRI_SUSCEPTIBLE_STATE_NAME}_TO_{LRI_WITH_CONDITION_STATE_NAME}',
    f'{LRI_WITH_CONDITION_STATE_NAME}_TO_{LRI_SUSCEPTIBLE_STATE_NAME}',
)

NTD_MODEL_NAME = 'neural_tube_defects'
NTD_OBSERVER = f'{NTD_MODEL_NAME}_births'
NTD_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{NTD_MODEL_NAME}'
NTD_WITH_CONDITION_STATE_NAME = NTD_MODEL_NAME
NTD_MODEL_STATES = (NTD_SUSCEPTIBLE_STATE_NAME, NTD_WITH_CONDITION_STATE_NAME)

VITAMIN_A_MODEL_NAME = 'vitamin_a_deficiency'
VITAMIN_A_WITH_CONDITION_STATE_NAME = VITAMIN_A_MODEL_NAME
VITAMIN_A_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{VITAMIN_A_MODEL_NAME}'
VITAMIN_A_MODEL_STATES = (VITAMIN_A_SUSCEPTIBLE_STATE_NAME, VITAMIN_A_WITH_CONDITION_STATE_NAME)
VITAMIN_A_MODEL_TRANSITIONS = (
    f'{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}_TO_{VITAMIN_A_WITH_CONDITION_STATE_NAME}',
    F'{VITAMIN_A_WITH_CONDITION_STATE_NAME}_TO_{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}'
)
VITAMIN_A_BAD_EVENT_COUNT = f'{VITAMIN_A_MODEL_NAME}_event_count'
VITAMIN_A_BAD_EVENT_TIME = f'{VITAMIN_A_MODEL_NAME}_event_time'
VITAMIN_A_GOOD_EVENT_COUNT = f'{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}_event_count'
VITAMIN_A_GOOD_EVENT_TIME = f'{VITAMIN_A_SUSCEPTIBLE_STATE_NAME}_event_time'
VITAMIN_A_PROPENSITY = f'{VITAMIN_A_MODEL_NAME}_propensity'
VITAMIN_A_RISK_CATEGORIES = ['cat1', 'cat2']

IRON_DEFICIENCY_MODEL_NAME = 'iron_deficiency'
ANEMIA_SEVERITY_GROUPS = ['none', 'mild', 'moderate', 'severe']


DISEASE_MODELS = (DIARRHEA_MODEL_NAME, MEASLES_MODEL_NAME, LRI_MODEL_NAME)
DISEASE_MODEL_MAP = {
    DIARRHEA_MODEL_NAME: {
        'states': DIARRHEA_MODEL_STATES,
        'transitions': DIARRHEA_MODEL_TRANSITIONS,
    },
    MEASLES_MODEL_NAME: {
        'states': MEASLES_MODEL_STATES,
        'transitions': MEASLES_MODEL_TRANSITIONS,
    },
    LRI_MODEL_NAME: {
        'states': LRI_MODEL_STATES,
        'transitions': LRI_MODEL_TRANSITIONS,
    },
    NTD_MODEL_NAME: {
        'states': NTD_MODEL_STATES,
        'transitions': (),
    },
    VITAMIN_A_MODEL_NAME: {
        'states': VITAMIN_A_MODEL_STATES,
        'transitions': VITAMIN_A_MODEL_TRANSITIONS,
    }
}

STATES = tuple(state for model in DISEASE_MODELS for state in DISEASE_MODEL_MAP[model]['states'])
TRANSITIONS = tuple(transition for model in DISEASE_MODELS for transition in DISEASE_MODEL_MAP[model]['transitions'])


########################
# Risk Model Constants #
########################

LBWSG_MODEL_NAME = 'low_birth_weight_and_short_gestation'
BIRTH_WEIGHT = 'birth_weight'
GESTATION_TIME = 'gestation_time'
LBWSG_COLUMNS = [BIRTH_WEIGHT, GESTATION_TIME]
UNDERWEIGHT = 2500  # grams
MAX_BIRTH_WEIGHT = 4500  # grams
PRETERM = 37  # weeks
MAX_GESTATIONAL_TIME = 42  # weeks


class __HEMOGLOBIN_DISTRIBUTION(NamedTuple):
    WEIGHT_GAMMA: float = 0.4
    WEIGHT_GUMBEL: float = 0.6
    EXPOSURE_MAX: float = 220.


HEMOGLOBIN_DISTRIBUTION = __HEMOGLOBIN_DISTRIBUTION()


class __LBWSG_MISSING_CATEGORY(NamedTuple):
    CAT: str = 'cat212'
    NAME: str = 'Birth prevalence - [37, 38) wks, [1000, 1500) g'
    EXPOSURE: float = 0.


LBWSG_MISSING_CATEGORY = __LBWSG_MISSING_CATEGORY()

BIRTH_WEIGHT_STATUS_COLUMN = 'underweight'
BIRTH_WEIGHT_NORMAL = 'normal'
BIRTH_WEIGHT_UNDERWEIGHT = 'underweight'
BIRTH_WEIGHT_CATEGORIES = (BIRTH_WEIGHT_NORMAL, BIRTH_WEIGHT_UNDERWEIGHT)

GESTATIONAL_AGE_STATUS_COLUMN = 'preterm'
GESTATIONAL_AGE_NORMAL = 'normal'
GESTATIONAL_AGE_PRETERM = 'preterm'
GESTATIONAL_AGE_CATEGORIES = (GESTATIONAL_AGE_NORMAL, GESTATIONAL_AGE_PRETERM)


#################################
# Results columns and variables #
#################################

TOTAL_POPULATION_COLUMN = 'total_population'
TOTAL_YLDS_COLUMN = 'years_lived_with_disability'
TOTAL_YLLS_COLUMN = 'years_of_life_lost'

STANDARD_COLUMNS = {
    'total_population': TOTAL_POPULATION_COLUMN,
    'total_ylls': TOTAL_YLLS_COLUMN,
    'total_ylds': TOTAL_YLDS_COLUMN,
}

# Columns from parallel runs
INPUT_DRAW_COLUMN = 'input_draw'
RANDOM_SEED_COLUMN = 'random_seed'

THROWAWAY_COLUMNS = ([f'{state}_event_count' for state in STATES]
                     + [f'{state}_prevalent_cases_at_sim_end' for state in STATES])

TOTAL_POPULATION_COLUMN_TEMPLATE = 'total_population_{POP_STATE}'
PERSON_TIME_COLUMN_TEMPLATE = 'person_time_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_vitamin_a_{VITAMIN_A_CAT}_anemia_{ANEMIA_GROUP}'
DEATH_COLUMN_TEMPLATE = 'death_due_to_{CAUSE_OF_DEATH}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_vitamin_a_{VITAMIN_A_CAT}_anemia_{ANEMIA_GROUP}'
YLLS_COLUMN_TEMPLATE = 'ylls_due_to_{CAUSE_OF_DEATH}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_vitamin_a_{VITAMIN_A_CAT}_anemia_{ANEMIA_GROUP}'
YLDS_COLUMN_TEMPLATE = 'ylds_due_to_{CAUSE_OF_DISABILITY}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_vitamin_a_{VITAMIN_A_CAT}_anemia_{ANEMIA_GROUP}'
STATE_PERSON_TIME_COLUMN_TEMPLATE = '{STATE}_person_time_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_vitamin_a_{VITAMIN_A_CAT}_anemia_{ANEMIA_GROUP}'
TRANSITION_COUNT_COLUMN_TEMPLATE = '{TRANSITION}_event_count_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_vitamin_a_{VITAMIN_A_CAT}_anemia_{ANEMIA_GROUP}'
BIRTHS_COLUMN_TEMPLATE = 'live_births_in_{YEAR}_among_{SEX}'
BORN_WITH_NTD_COLUMN_TEMPLATE = 'born_with_ntds_in_{YEAR}_among_{SEX}'
BIRTH_WEIGHT_COLUMN_TEMPLATE = 'birth_weight_{STAT_STATE}'
GESTATIONAL_AGE_COLUMN_TEMPLATE = 'gestational_age_{STAT_STATE}'

COLUMN_TEMPLATES = {
    'population': TOTAL_POPULATION_COLUMN_TEMPLATE,
    'person_time': PERSON_TIME_COLUMN_TEMPLATE,
    'deaths': DEATH_COLUMN_TEMPLATE,
    'ylls': YLLS_COLUMN_TEMPLATE,
    'ylds': YLDS_COLUMN_TEMPLATE,
    'state_person_time': STATE_PERSON_TIME_COLUMN_TEMPLATE,
    'transition_count': TRANSITION_COUNT_COLUMN_TEMPLATE,
    'births': BIRTHS_COLUMN_TEMPLATE,
    'born_with_ntds': BORN_WITH_NTD_COLUMN_TEMPLATE,
    'birth_weight': BIRTH_WEIGHT_COLUMN_TEMPLATE,
    'gestational_age': GESTATIONAL_AGE_COLUMN_TEMPLATE,
}

NON_COUNT_TEMPLATES = [
    'birth_weight',
    'gestational_age'
]

POP_STATES = ('living', 'dead', 'tracked', 'untracked')
STAT_MEASURES = ('mean', 'sd')
SEXES = ('male', 'female')
YEARS = tuple(range(2020, 2025))
AGE_GROUPS = ('early_neonatal', 'late_neonatal', 'post_neonatal', '1_to_4')
CAUSES_OF_DEATH = (
    'other_causes',
    DIARRHEA_WITH_CONDITION_STATE_NAME,
    MEASLES_WITH_CONDITION_STATE_NAME,
    LRI_WITH_CONDITION_STATE_NAME,
    NTD_WITH_CONDITION_STATE_NAME,
)

CAUSES_OF_DISABILITY = (
    DIARRHEA_WITH_CONDITION_STATE_NAME,
    MEASLES_WITH_CONDITION_STATE_NAME,
    LRI_WITH_CONDITION_STATE_NAME,
    NTD_WITH_CONDITION_STATE_NAME,
    VITAMIN_A_WITH_CONDITION_STATE_NAME,
    IRON_DEFICIENCY_MODEL_NAME,
)

TEMPLATE_FIELD_MAP = {
    'POP_STATE': POP_STATES,
    'YEAR': YEARS,
    'SEX': SEXES,
    'AGE_GROUP': AGE_GROUPS,
    'CAUSE_OF_DEATH': CAUSES_OF_DEATH,
    'CAUSE_OF_DISABILITY': CAUSES_OF_DISABILITY,
    'STATE': STATES,
    'TRANSITION': TRANSITIONS,
    'STAT_STATE': STAT_MEASURES,
    'VITAMIN_A_CAT': VITAMIN_A_RISK_CATEGORIES,
    'ANEMIA_GROUP': ANEMIA_SEVERITY_GROUPS,

}


def RESULT_COLUMNS(kind='all'):
    if kind not in COLUMN_TEMPLATES and kind != 'all':
        raise ValueError(f'Unknown result column type {kind}')
    columns = []
    if kind == 'all':
        for k in COLUMN_TEMPLATES:
            columns += RESULT_COLUMNS(k)
        columns = list(STANDARD_COLUMNS.values()) + columns
    else:
        template = COLUMN_TEMPLATES[kind]
        filtered_field_map = {field: values
                              for field, values in TEMPLATE_FIELD_MAP.items() if f'{{{field}}}' in template}
        fields, value_groups = filtered_field_map.keys(), itertools.product(*filtered_field_map.values())
        for value_group in value_groups:
            columns.append(template.format(**{field: value for field, value in zip(fields, value_group)}).lower())
    return columns
