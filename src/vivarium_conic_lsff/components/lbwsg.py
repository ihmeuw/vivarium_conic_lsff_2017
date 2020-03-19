"""
Low birth weight and short gestation (LBWSG) is a non-standard risk
implementation that has been used in several public health models.

Note that because the input data is so large, it relies on a custom relative
risk data loader that expects data saved in keys by draw.
"""
from typing import Tuple

import pandas as pd
from vivarium_public_health.utilities import EntityString, TargetString
from vivarium_public_health.risks.data_transformations import pivot_categorical

from vivarium_conic_lsff import globals as project_globals


class LBWSGRisk:

    @property
    def name(self):
        return f"risk.{project_globals.LBWSG_MODEL_NAME}"

    def setup(self, builder):
        self.risk = EntityString(f'risk_factor.{project_globals.LBWSG_MODEL_NAME}')
        self.exposure_distribution = LBWSGDistribution(builder)

        # FIXME: These are not actual birth weights/gestational times, but the
        # raw values that source pipelines.  They should use different column
        # names but that's too much to try to fix now.  We didn't build the
        # distribution class with clear enough boundaries to make the
        # distinction.
        created_columns = project_globals.LBWSG_COLUMNS
        self.population_view = builder.population.get_view(created_columns + ['tracked'])
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=created_columns,
                                                 requires_columns=['age', 'sex'])

        self._raw_exposure = builder.value.register_value_producer(
            f'{self.risk.name}.raw_exposure',
            source=lambda index: self.population_view.get(index),
            requires_columns=project_globals.LBWSG_COLUMNS
        )
        self.exposure = builder.value.register_value_producer(
            f'{self.risk.name}.exposure',
            source=self._raw_exposure,
            preferred_post_processor=self.exposure_distribution.convert_to_categorical,
            requires_values=f'{self.risk.name}.raw_exposure'
        )

    def on_initialize_simulants(self, pop_data):
        exposure = self.exposure_distribution.get_birth_weight_and_gestational_age(pop_data.index)
        df = pd.DataFrame({
            project_globals.BIRTH_WEIGHT: exposure[project_globals.BIRTH_WEIGHT],
            project_globals.GESTATION_TIME: exposure[project_globals.GESTATION_TIME]
        }, index=pop_data.index)
        self.population_view.update(df)


# FIXME: This class is not a clear representation of the lbwsg distribution.
# It should act as a standalone (e.g. a library) class that could be used
# to stand up and sample from the distribution outside the simulation, because
# we have definite need of that.  A possibility is to port it to the
# risk_distributions library and construct a simulation wrapper like we do
# with the ensemble distributions in vph.
class LBWSGDistribution:

    def __init__(self, builder):
        self.risk = EntityString(f'risk_factor.{project_globals.LBWSG_MODEL_NAME}')
        self.randomness = builder.randomness.get_stream(f'{self.risk.name}.exposure')

        self.categories_by_interval = get_lbwsg_categories_by_interval(builder)
        self.intervals_by_category = self.categories_by_interval.reset_index().set_index('cat')
        self.max_gt_by_bw, self.max_bw_by_gt = self._get_boundary_mappings()

        self.exposure_parameters = builder.lookup.build_table(self.get_exposure_data(builder),
                                                              key_columns=['sex'],
                                                              parameter_columns=['age', 'year'])

    def get_birth_weight_and_gestational_age(self, index):
        category_draw = self.randomness.get_draw(index, additional_key='category')
        exposure = self.exposure_parameters(index)[self.categories_by_interval.values]
        exposure_sum = exposure.cumsum(axis='columns')
        category_index = (exposure_sum.T < category_draw).T.sum('columns')
        categorical_exposure = pd.Series(self.categories_by_interval.values[category_index],
                                         index=index, name='cat')
        return self._convert_to_continuous(categorical_exposure)

    def convert_to_categorical(self, exposure, _):
        # FIXME: DIRTY HACK.  The problem with absolute shifts is that
        #  they can take you way out of a realistic domain for your
        #  values.  Like giving people negative birth weights
        birth_weight = exposure[project_globals.BIRTH_WEIGHT]
        exposure.loc[birth_weight < 100, project_globals.BIRTH_WEIGHT] = 100
        exposure = self._convert_boundary_cases(exposure)
        categorical_exposure = self.categories_by_interval.iloc[self._get_categorical_index(exposure)]
        categorical_exposure.index = exposure.index
        return categorical_exposure

    def _convert_boundary_cases(self, exposure):
        birth_weight = exposure[project_globals.BIRTH_WEIGHT]
        gestation_time = exposure[project_globals.GESTATION_TIME]
        eps = 1e-4
        outside_bounds = self._get_categorical_index(exposure) == -1
        shift_down = outside_bounds & (
                (birth_weight < 1000)
                | ((1000 < birth_weight) & (birth_weight < project_globals.MAX_BIRTH_WEIGHT)
                   & (40 < gestation_time))
        )
        shift_left = outside_bounds & (
            ((1000 < birth_weight) & (gestation_time < 34))
                | ((project_globals.MAX_BIRTH_WEIGHT < birth_weight)
                   & (gestation_time < project_globals.MAX_GESTATIONAL_TIME))
        )
        tmrel = outside_bounds & (
                (project_globals.MAX_BIRTH_WEIGHT < birth_weight)
                & (project_globals.MAX_GESTATIONAL_TIME < gestation_time)
        )

        max_gt_for_bw = self.max_gt_by_bw.loc[exposure.loc[shift_down, project_globals.BIRTH_WEIGHT]].values
        exposure.loc[shift_down, project_globals.GESTATION_TIME] = max_gt_for_bw - eps

        max_bw_for_gt = self.max_bw_by_gt.loc[exposure.loc[shift_left, project_globals.GESTATION_TIME]].values
        exposure.loc[shift_left, project_globals.BIRTH_WEIGHT] = max_bw_for_gt - eps

        exposure.loc[tmrel, project_globals.GESTATION_TIME] = project_globals.MAX_GESTATIONAL_TIME - eps
        exposure.loc[tmrel, project_globals.BIRTH_WEIGHT] = project_globals.MAX_BIRTH_WEIGHT - eps

        return exposure

    def _get_categorical_index(self, exposure):
        exposure_bw_gt_index = exposure.set_index(project_globals.LBWSG_COLUMNS).index
        return self.categories_by_interval.index.get_indexer(exposure_bw_gt_index, method=None)

    def _convert_to_continuous(self, categorical_exposure):
        birth_weight_draw = self.randomness.get_draw(categorical_exposure.index,
                                                     additional_key=project_globals.BIRTH_WEIGHT)
        gestational_time_draw = self.randomness.get_draw(categorical_exposure.index,
                                                         additional_key=project_globals.GESTATION_TIME)
        draws = {project_globals.BIRTH_WEIGHT: birth_weight_draw,
                 project_globals.GESTATION_TIME: gestational_time_draw}

        def single_values_from_category(row):
            idx = row['index']
            bw_draw = draws[project_globals.BIRTH_WEIGHT][idx]
            gt_draw = draws[project_globals.GESTATION_TIME][idx]

            intervals = self.intervals_by_category.loc[row['cat']]

            birth_weight = (intervals.birth_weight.left
                            + bw_draw * (intervals.birth_weight.right - intervals.birth_weight.left))
            gestational_age = (intervals.gestation_time.left
                               + gt_draw * (intervals.gestation_time.right - intervals.gestation_time.left))

            return birth_weight, gestational_age

        values = categorical_exposure.reset_index().apply(single_values_from_category, axis=1)
        return pd.DataFrame(list(values), index=categorical_exposure.index,
                            columns=project_globals.LBWSG_COLUMNS)

    def _get_boundary_mappings(self):
        cats = self.categories_by_interval.reset_index()
        max_gt_by_bw = pd.Series({bw_interval: pd.Index(group.gestation_time).right.max()
                                  for bw_interval, group in cats.groupby(project_globals.BIRTH_WEIGHT)})
        max_bw_by_gt = pd.Series({gt_interval: pd.Index(group.birth_weight).right.max()
                                  for gt_interval, group in cats.groupby(project_globals.GESTATION_TIME)})
        return max_gt_by_bw, max_bw_by_gt

    @staticmethod
    def get_exposure_data(builder):
        exposure = read_data_by_draw(builder, project_globals.LBWSG_EXPOSURE)
        exposure = pivot_categorical(exposure)
        exposure[project_globals.LBWSG_MISSING_CATEGORY.CAT] = project_globals.LBWSG_MISSING_CATEGORY.EXPOSURE
        return exposure


def get_lbwsg_categories_by_interval(builder):
    category_dict = builder.data.load(project_globals.LBWSG_CATEGORIES)
    category_dict[project_globals.LBWSG_MISSING_CATEGORY.CAT] = project_globals.LBWSG_MISSING_CATEGORY.NAME
    cats = (pd.DataFrame.from_dict(category_dict, orient='index')
            .reset_index()
            .rename(columns={'index': 'cat', 0: 'name'}))
    idx = pd.MultiIndex.from_tuples(cats.name.apply(get_intervals_from_name),
                                    names=project_globals.LBWSG_COLUMNS)
    cats = cats['cat']
    cats.index = idx
    return cats


def get_intervals_from_name(name: str) -> Tuple[pd.Interval, pd.Interval]:
    """Converts a LBWSG category name to a pair of intervals.

    The first interval corresponds to gestational age in weeks, the
    second to birth weight in grams.
    """
    numbers_only = (name.replace('Birth prevalence - [', '')
                    .replace(',', '')
                    .replace(') wks [', ' ')
                    .replace(') g', ''))
    numbers_only = [int(n) for n in numbers_only.split()]
    # FIXME: The raw ordering here is brittle
    return (pd.Interval(numbers_only[2], numbers_only[3], closed='left'),
            pd.Interval(numbers_only[0], numbers_only[1], closed='left'))


class LBWSGRiskEffect:
    """A component to model the impact of the low birth weight and short gestation
     risk factor on the target rate of some affected entity.
    """

    def __init__(self, target: str):
        """
        Parameters
        ----------
        target :
            Type, name, and target rate of entity to be affected by risk factor,
            supplied in the form "entity_type.entity_name.measure"
            where entity_type should be singular (e.g., cause instead of causes).
        """
        self.risk = EntityString(f'risk_factor.low_birth_weight_and_short_gestation')
        self.target = TargetString(target)

    @property
    def name(self):
        return f"risk_effect.{self.risk}.{self.target}"

    def setup(self, builder):
        rr_data = self.get_relative_risk_data(builder)
        paf_data = self.get_population_attributable_fraction_data(builder)
        self.relative_risk = builder.lookup.build_table(rr_data,
                                                        key_columns=['sex'],
                                                        parameter_columns=['age', 'year'])
        self.population_attributable_fraction = builder.lookup.build_table(paf_data,
                                                                           key_columns=['sex'],
                                                                           parameter_columns=['age', 'year'])

        self.exposure_effect = self.get_exposure_effect(builder)

        builder.value.register_value_modifier(f'{self.target.name}.{self.target.measure}',
                                              modifier=self.adjust_target)
        builder.value.register_value_modifier(
            f'{self.target.name}.{self.target.measure}.population_attributable_fraction',
            modifier=self.population_attributable_fraction)

    def adjust_target(self, index, target):
        return self.exposure_effect(target, self.relative_risk(index))

    def get_relative_risk_data(self, builder):
        relative_risk_data = read_data_by_draw(builder, f'{self.risk}.relative_risk')
        correct_target = ((relative_risk_data['affected_entity'] == 'all')
                          & (relative_risk_data['affected_measure'] == 'excess_mortality_rate'))
        relative_risk_data = (relative_risk_data[correct_target]
                              .drop(['affected_entity', 'affected_measure'], 'columns'))
        relative_risk_data = pivot_categorical(relative_risk_data)
        relative_risk_data[project_globals.LBWSG_MISSING_CATEGORY.CAT] = (relative_risk_data['cat106']
                                                                          + relative_risk_data['cat116']) / 2
        return relative_risk_data

    def get_population_attributable_fraction_data(self, builder):
        index_columns = ['sex', 'age_start', 'age_end', 'year_start', 'year_end']
        rr_data = self.get_relative_risk_data(builder).set_index(index_columns)
        exposure_data = self.get_exposure_data(builder).set_index(index_columns)
        mean_rr = (rr_data * exposure_data).sum(axis=1)
        paf_data = ((mean_rr - 1)/mean_rr).reset_index().rename(columns={0: 'value'})
        return paf_data

    def get_exposure_effect(self, builder):
        risk_exposure = builder.value.get_value(f'{self.risk.name}.exposure')

        def exposure_effect(rates, rr):
            exposure = risk_exposure(rr.index)
            return rates * (rr.lookup(exposure.index, exposure))

        return exposure_effect

    @staticmethod
    def get_exposure_data(builder):
        exposure = read_data_by_draw(builder, project_globals.LBWSG_EXPOSURE)
        exposure = pivot_categorical(exposure)
        exposure[project_globals.LBWSG_MISSING_CATEGORY.CAT] = project_globals.LBWSG_MISSING_CATEGORY.EXPOSURE
        return exposure


def read_data_by_draw(builder, key):
    path = builder.configuration.input_data.artifact_path
    draw = builder.configuration.input_data.input_draw_number
    key = key.replace(".", "/")
    with pd.HDFStore(path, mode='r') as store:
        index = store.get(f'{key}/index')
        draw = store.get(f'{key}/draw_{draw}')
    draw = draw.rename("value")
    data = pd.concat([index, draw], axis=1)
    data = data.drop(columns='location')
    return data
