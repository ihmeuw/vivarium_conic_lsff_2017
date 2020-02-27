from pathlib import Path
from typing import NamedTuple, List

import pandas as pd
import yaml

from vivarium_conic_lsff import globals as project_globals


SCENARIO_COLUMN = 'scenario'
GROUPBY_COLUMNS = [
    project_globals.INPUT_DRAW_COLUMN,
    # SCENARIO_COLUMN
]
PERSON_YEAR_SCALE = 100_000
DROP_COLUMNS = ['measure']
# SHARED_COLUMNS = [
#     'age_group',
#     'treatment_group',
#     'input_draw',
#     # 'scenario'
# ]


class Stratification:
    def __init__(self, age=True, sex=True, year=True):
        self._levels = {}
        if age:
            self._levels['age'] = ('age_group', 'in_age_group')
        if sex:
            self._levels['sex'] = ('sex', '_among_')
        if year:
            self._levels['year'] = ('year', '_in_')

    @property
    def levels(self):
        return self._levels.values()


def make_measure_data(data):
    measure_data = MeasureData(
        population=get_population_data(data),
        person_time=get_measure_data(data, 'person_time', with_cause=False),
        ylls=get_measure_data(data, 'ylls'),
        ylds=get_measure_data(data, 'ylds'),
        deaths=get_measure_data(data, 'deaths'),
        state_person_time=get_measure_data(data, 'state_person_time', with_cause=False, state=True),
        transition_count=get_measure_data(data, 'transition_count', with_cause=False, transition=True),
        births=get_measure_data(data, 'births', with_cause=False, state=False,
                                           stratified=Stratification(age=False, sex=True, year=True)),
        births_with_ntd=get_measure_data(data, 'born_with_ntd', with_cause=False, state=False,
                                           stratified=Stratification(age=False, sex=True, year=True)),
    )
    return measure_data


# def make_final_data(measure_data):
#     final_data = FinalData(
#         mortality_rate=get_rate_data(measure_data, 'deaths', 'mortality_rate'),
#         ylls=get_rate_data(measure_data, 'ylls', 'ylls'),
#         ylds=get_rate_data(measure_data, 'ylds', 'ylds'),
#         dalys=get_dalys(measure_data),
#     )
#     return final_data


class MeasureData(NamedTuple):
    population: pd.DataFrame
    person_time: pd.DataFrame
    ylls: pd.DataFrame
    ylds: pd.DataFrame
    deaths: pd.DataFrame
    state_person_time: pd.DataFrame
    transition_count: pd.DataFrame
    births: pd.DataFrame
    births_with_ntd: pd.DataFrame

    def dump(self, output_dir: Path):
        for key, df in self._asdict().items():
            df.to_hdf(output_dir / f'{key}.hdf', key=key)
            df.to_csv(output_dir / f'{key}.csv')


class FinalData(NamedTuple):
    mortality_rate: pd.DataFrame
    ylls: pd.DataFrame
    ylds: pd.DataFrame
    dalys: pd.DataFrame

    def dump(self, output_dir: Path):
        for key, df in self._asdict().items():
            df.to_hdf(output_dir / f'{key}.hdf', key=key)
            df.to_csv(output_dir / f'{key}.csv')


def read_data(path: Path) -> (pd.DataFrame, List[str]):
    data = pd.read_hdf(path)
    data = (data
            .drop(columns=data.columns.intersection(project_globals.THROWAWAY_COLUMNS))
            .reset_index(drop=True)
            # TODO: add back when we have scenarios
            # .rename(columns={project_globals.OUTPUT_SCENARIO_COLUMN: SCENARIO_COLUMN}))
            )
    data[project_globals.INPUT_DRAW_COLUMN] = data[project_globals.INPUT_DRAW_COLUMN].astype(int)
    data[project_globals.RANDOM_SEED_COLUMN] = data[project_globals.RANDOM_SEED_COLUMN].astype(int)
    with (path.parent / 'keyspace.yaml').open() as f:
        keyspace = yaml.full_load(f)
    return data, keyspace


def filter_out_incomplete(data, keyspace):
    output = []
    for draw in keyspace[project_globals.INPUT_DRAW_COLUMN]:
        # For each draw, gather all random seeds completed for all scenarios.
        random_seeds = set(keyspace[project_globals.RANDOM_SEED_COLUMN])
        draw_data = data.loc[data[project_globals.INPUT_DRAW_COLUMN] == draw]
        # TODO: add back when we have scenarios
        # for scenario in keyspace[project_globals.OUTPUT_SCENARIO_COLUMN]:
        #     seeds_in_data = draw_data.loc[data[SCENARIO_COLUMN] == scenario,
        #                                   project_globals.RANDOM_SEED_COLUMN].unique()
        #     random_seeds = random_seeds.intersection(seeds_in_data)
        draw_data = draw_data.loc[draw_data[project_globals.RANDOM_SEED_COLUMN].isin(random_seeds)]
        output.append(draw_data)
    return pd.concat(output, ignore_index=True).reset_index(drop=True)


def aggregate_over_seed(data):
    non_count_columns = []
    for non_count_template in project_globals.NON_COUNT_TEMPLATES:
        non_count_columns += project_globals.RESULT_COLUMNS(non_count_template)
    count_columns = [c for c in data.columns if c not in non_count_columns + GROUPBY_COLUMNS]

    # non_count_data = data[non_count_columns + GROUPBY_COLUMNS].groupby(GROUPBY_COLUMNS).mean()
    count_data = data[count_columns + GROUPBY_COLUMNS].groupby(GROUPBY_COLUMNS).sum()
    return pd.concat([
        count_data,
        # non_count_data
    ], axis=1).reset_index()


def pivot_data(data):
    return (data
            .set_index(GROUPBY_COLUMNS)
            .stack()
            .reset_index()
            .rename(columns={f'level_{len(GROUPBY_COLUMNS)}': 'process', 0: 'value'}))


def sort_data(data):
    sort_order = [
        'age_group',
        'risk',
        'cause',
        'treatment_group',
        'measure',
        'input_draw'
    ]
    sort_order = [c for c in sort_order if c in data.columns]
    other_cols = [c for c in data.columns if c not in sort_order]
    data = data[sort_order + other_cols].sort_values(sort_order)
    return data.reset_index(drop=True)


def split_processing_column(data, with_cause, state, transition, stratified):
    data['treatment_group'] = 'all'
    for level in stratified.levels:
        data['process'], data[level[0]] = data.process.str.split(level[1]).str
    if with_cause:
        data['measure'], data['cause'] = data.process.str.split('_due_to_').str
    elif state:
        data['state'], _ = data.process.str.split('_person_time').str
        data['measure'] = 'person_time'
    elif transition:
        data['measure'], _ = data.process.str.split('_event_count').str
    else:
        data['measure'] = data['process']
    return data.drop(columns='process')


def get_population_data(data):
    total_pop = pivot_data(data[[project_globals.TOTAL_POPULATION_COLUMN]
                                + project_globals.RESULT_COLUMNS('population')
                                + GROUPBY_COLUMNS])
    total_pop = total_pop.rename(columns={'process': 'measure'})
    total_pop['treatment_group'] = 'all'
    return sort_data(total_pop)


def get_measure_data(data, measure, with_cause=True, state=False, transition=False, stratified=Stratification()):
    data = pivot_data(data[project_globals.RESULT_COLUMNS(measure) + GROUPBY_COLUMNS])
    data = split_processing_column(data, with_cause, state, transition, stratified)
    return sort_data(data)


# def get_risk_categories(data):
#     data = pivot_data(data[project_globals.RESULT_COLUMNS('category_counts') + GROUPBY_COLUMNS])
#     data['risk'], data['process'] = data.process.str.split('_cat').str
#     data['measure'] = data['measure'].apply(lambda x: f'cat{x}')
#     data = data.drop(columns='process')
#     return sort_data(data)


# def get_rate_numerator(measure_data: MeasureData, numerator_label: str):
#     numerator = getattr(measure_data, numerator_label).drop(columns=DROP_COLUMNS)
#     all_cause_numerator = numerator.groupby(SHARED_COLUMNS).value.sum().reset_index()
#     all_cause_numerator['cause'] = 'all_causes'
#     return pd.concat([numerator, all_cause_numerator], ignore_index=True).set_index(SHARED_COLUMNS + ['cause'])


# def compute_rate(measure_data: MeasureData, numerator: pd.DataFrame, measure: str):
#     person_time = measure_data.person_time.drop(columns=DROP_COLUMNS).set_index(SHARED_COLUMNS)
#     rate_data = (numerator / person_time * PERSON_YEAR_SCALE).fillna(0).reset_index()
#     rate_data['measure'] = f'{measure}_per_100k_py'
#     return rate_data


# def get_rate_data(measure_data: MeasureData, numerator_label: str, measure: str) -> pd.DataFrame:
#     numerator = get_rate_numerator(measure_data, numerator_label)
#     rate_data = compute_rate(measure_data, numerator, measure)
#     return sort_data(rate_data)


# def get_dalys(measure_data: MeasureData):
#     ylls = get_rate_numerator(measure_data, 'ylls')
#     ylds = get_rate_numerator(measure_data, 'ylds')
#     ylls.loc[ylds.index] += ylds
#     dalys = compute_rate(measure_data, ylls, 'dalys')
#     return sort_data(dalys)