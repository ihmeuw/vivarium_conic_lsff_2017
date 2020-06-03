import typing

import numpy as np
import pandas as pd
import scipy.stats
from vivarium_public_health.utilities import to_years

from vivarium_public_health.risks.distributions import clip

from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.population import SimulantData


class IronDeficiency:

    def __init__(self):
        self._distribution = IronDeficiencyDistribution()

    @property
    def name(self):
        return project_globals.IRON_DEFICIENCY_MODEL_NAME

    @property
    def sub_components(self):
        return [self._distribution]

    def setup(self, builder: 'Builder'):
        self.randomness = builder.randomness.get_stream(f'{self.name}.propensity')

        threshold_data = self.load_iron_responsiveness_threshold(builder)
        self.thresholds = builder.lookup.build_table(threshold_data,
                                                     key_columns=['sex'],
                                                     parameter_columns=['age', 'year'])
        self.iron_responsive = builder.value.register_value_producer(
            f'iron_responsive',
            source=self.get_iron_responsive,
            requires_columns=['age', 'sex', 'iron_responsiveness_propensity'],
            requires_values=[f'{self.name}.exposure'])
        disability_weight_data = self.load_disability_weight_data(builder)
        self.raw_disability_weight = builder.lookup.build_table(disability_weight_data,
                                                                key_columns=['sex'],
                                                                parameter_columns=['age', 'year'])
        self.disability_weight = builder.value.register_value_producer(f'{self.name}.disability_weight',
                                                                       source=self.get_disability_weight,
                                                                       requires_columns=['age', 'sex'],
                                                                       requires_values=[f'{self.name}.exposure'])
        builder.value.register_value_modifier('disability_weight', self.disability_weight)
        
        self.raw_exposure = builder.value.register_value_producer(f'{self.name}.raw_exposure',
                                                                  source=self.get_exposure,
                                                                  requires_values=[f'{self.name}.exposure_parameters'])

        self.exposure = builder.value.register_value_producer(f'{self.name}.exposure',
                                                              source=self.raw_exposure)

        self.severity = builder.value.register_value_producer('anemia_severity',
                                                              source=self.get_severity)

        columns_created = [f'{self.name}_propensity', 'iron_responsiveness_propensity']
        columns_required = ['age', 'sex']

        self.population_view = builder.population.get_view(columns_created + columns_required)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created,
                                                 requires_columns=columns_required,
                                                 requires_streams=[f'{self.name}.propensity'])

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        propensity = self.randomness.get_draw(pop_data.index)
        iron_responsive_propensity = self.randomness.get_draw(pop_data.index, additional_key='iron_responsiveness')
        pop_update = pd.DataFrame({
            f'{self.name}_propensity': propensity,
            f'iron_responsiveness_propensity': iron_responsive_propensity
        }, index=pop_data.index)
        self.population_view.update(pop_update)

    def get_exposure(self, index):
        propensity = self.population_view.subview([f'{self.name}_propensity']).get(index).iron_deficiency_propensity
        return self._compute_exposure(propensity)

    def get_disability_weight(self, index):
        disability_data = self.raw_disability_weight(index)
        severity = self.severity(index)
        disability_weight = pd.Series(disability_data.lookup(index, severity), index=index)
        return disability_weight

    def get_iron_responsive(self, index):
        propensity = (self.population_view
                      .subview(['iron_responsiveness_propensity'])
                      .get(index)
                      .iron_responsiveness_propensity)
        severity = self._private_severity(index)
        threshold = pd.Series(self.thresholds(index).lookup(index, severity), index=index)
        iron_responsive = propensity < threshold
        iron_responsive.name = 'iron_responsive'
        return iron_responsive

    def _private_severity(self, index):
        exposure = self.get_exposure(index)
        severity = self._get_severity(exposure)
        return severity

    def get_severity(self, index):
        exposure = self.exposure(index)
        severity = self._get_severity(exposure)
        severity.name = 'anemia_severity'
        return severity

    def _compute_exposure(self, propensity):
        return self._distribution.ppf(propensity)

    def _get_severity(self, exposure):
        age = self.population_view.subview(['age']).get(exposure.index).age
        severity = pd.Series('none', index=exposure.index, name='anemia_severity')

        neonatal = age < to_years(pd.Timedelta(days=28))
        mild = ((neonatal & (130 <= exposure) & (exposure < 150))
                | (~neonatal & (100 <= exposure) & (exposure < 110)))
        moderate = ((neonatal & (90 <= exposure) & (exposure < 130))
                    | (~neonatal & (70 <= exposure) & (exposure < 100)))
        severe = ((neonatal & (exposure < 90))
                  | (~neonatal & (exposure < 70)))
        severity.loc[mild] = 'mild'
        severity.loc[moderate] = 'moderate'
        severity.loc[severe] = 'severe'
        return severity

    def load_iron_responsiveness_threshold(self, builder):
        data = []
        keys = {
            'mild': project_globals.IRON_DEFICIENCY_MILD_ANEMIA_IRON_RESPONSIVE_PROPORTION,
            'moderate': project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_IRON_RESPONSIVE_PROPORTION,
            'severe': project_globals.IRON_DEFICIENCY_SEVERE_ANEMIA_IRON_RESPONSIVE_PROPORTION
        }
        for severity, data_key in keys.items():
            proportion = builder.data.load(data_key)
            proportion = (proportion
                          .set_index([c for c in proportion.columns if c != 'value'])
                          .rename(columns={'value': severity}))
            data.append(proportion)
        data = pd.concat(data, axis=1).reset_index()
        data.loc[:, 'none'] = 1.
        return data

    def load_disability_weight_data(self, builder):
        data = []
        keys = {
            'mild': project_globals.IRON_DEFICIENCY_MILD_ANEMIA_DISABILITY_WEIGHT,
            'moderate': project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_DISABILITY_WEIGHT,
            'severe': project_globals.IRON_DEFICIENCY_SEVERE_ANEMIA_DISABILITY_WEIGHT,
        }
        for severity, data_key in keys.items():
            disability_weight = builder.data.load(data_key)
            disability_weight = (disability_weight
                                 .set_index([c for c in disability_weight.columns if c != 'value'])
                                 .rename(columns={'value': severity}))
            data.append(disability_weight)
        data = pd.concat(data, axis=1).reset_index()
        data.loc[:, 'none'] = 0.
        return data


class IronDeficiencyDistribution:

    @property
    def name(self):
        return f'{project_globals.IRON_DEFICIENCY_MODEL_NAME}_exposure_distribution'

    def setup(self, builder: 'Builder'):
        exposure_parameters = self.load_exposure_parameters(builder)
        exposure_data = builder.lookup.build_table(exposure_parameters,
                                                   key_columns=['sex'],
                                                   parameter_columns=['age', 'year'])
        self.exposure_parameters = builder.value.register_value_producer(
            f'{project_globals.IRON_DEFICIENCY_MODEL_NAME}.exposure_parameters',
            source=exposure_data,
            requires_columns=['age', 'sex']
        )

    def ppf(self, propensity: pd.Series) -> pd.Series:
        propensity = clip(propensity)
        exposure_data = self.exposure_parameters(propensity.index)
        mean = exposure_data['mean']
        sd = exposure_data['sd']
        exposure = (project_globals.HEMOGLOBIN_DISTRIBUTION.WEIGHT_GAMMA * self._gamma_ppf(propensity, mean, sd)
                    + project_globals.HEMOGLOBIN_DISTRIBUTION.WEIGHT_GUMBEL * self._mirrored_gumbel_ppf(propensity, mean, sd))
        return pd.Series(exposure, index=propensity.index, name='value')

    @staticmethod
    def _gamma_ppf(propensity, mean, sd):
        shape = (mean / sd)**2
        scale = sd**2 / mean
        return scipy.stats.gamma(a=shape, scale=scale).ppf(propensity)

    @staticmethod
    def _mirrored_gumbel_ppf(propensity, mean, sd):
        x_max = project_globals.HEMOGLOBIN_DISTRIBUTION.EXPOSURE_MAX
        alpha = x_max - mean - (sd * np.euler_gamma * np.sqrt(6) / np.pi)
        scale = sd * np.sqrt(6) / np.pi
        return x_max - scipy.stats.gumbel_r(alpha, scale=scale).ppf(1 - propensity)

    @staticmethod
    def load_exposure_parameters(builder):
        exposure_mean = builder.data.load(project_globals.IRON_DEFICIENCY_EXPOSURE).drop(columns=['parameter'])
        exposure_mean = (exposure_mean
                         .set_index([c for c in exposure_mean.columns if c != 'value'])
                         .rename(columns={'value': 'mean'}))
        exposure_sd = builder.data.load(project_globals.IRON_DEFICIENCY_EXPOSURE_SD)
        exposure_sd = (exposure_sd
                       .set_index([c for c in exposure_sd.columns if c != 'value'])
                       .rename(columns={'value': 'sd'}))
        exposure_parameters = pd.concat([exposure_mean, exposure_sd], axis=1).reset_index()
        return exposure_parameters
