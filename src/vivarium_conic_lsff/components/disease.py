import typing

import numpy as np
import pandas as pd
import scipy.stats
from vivarium.framework.values import list_combiner, union_post_processor
from vivarium_public_health.utilities import to_years
from vivarium_public_health.risks.data_transformations import pivot_categorical

from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.event import Event
    from vivarium.framework.population import SimulantData


class VitaminADeficiency:
    """
    Model Vitamin A deficiency (VAD).

    VAD is a disease fully attributed by a risk. The clinical definition
    of the with-condition state corresponds to a particular exposure of a risk.
    VAD is a categorical risk with 'cat1' denoting with-condition and
    'cat2' being without-condition. There is no mortality associated with VAD.

    This class fulfills requirements for Disease model, Disease state, and risk. The
    following qualities show this.

    Risk:
        - exposes pipelines for "exposure_parameters" and "exposure_parameters_paf"
          to satisfy requirements for a risk distribution.

        - provides an "exposure" pipeline to fulfill risk requirements

    Disease Model:
        - adds the state name to the state column for disease state

        - creates event_count and event_time columns for disease state

        - initializes simulants

        - exposes a pipeline for producing and changing disability weights
    """

    # RiskEffect requires this block
    configuration_defaults = {
        project_globals.VITAMIN_A_MODEL_NAME: {
            "exposure": 'data',
            "rebinned_exposed": [],
            "category_thresholds": [],
        }
    }

    @property
    def name(self):
        return project_globals.VITAMIN_A_MODEL_NAME

    def setup(self, builder: 'Builder'):
        self.clock = builder.time.clock()

        columns_created = [self.name,
                           project_globals.VITAMIN_A_GOOD_EVENT_TIME, project_globals.VITAMIN_A_GOOD_EVENT_COUNT,
                           project_globals.VITAMIN_A_BAD_EVENT_TIME, project_globals.VITAMIN_A_BAD_EVENT_COUNT,
                           project_globals.VITAMIN_A_PROPENSITY]

        view_columns = columns_created + ['alive', 'age', 'sex']
        self.population_view = builder.population.get_view(view_columns)

        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created,
                                                 requires_columns=['age', 'sex'],
                                                 requires_streams=[f'{self.name}_initial_states'])

        self.randomness = builder.randomness.get_stream(f'{self.name}_initial_states')

        disability_weight_data = builder.data.load(project_globals.VITAMIN_A_DEFICIENCY_DISABILITY_WEIGHT)

        self.base_disability_weight = builder.lookup.build_table(disability_weight_data, key_columns=['sex'],
                                                                 parameter_columns=['age', 'year'])
        self.disability_weight = builder.value.register_value_producer(
            f'{self.name}.disability_weight',
            source=self.compute_disability_weight,
            requires_columns=['age', 'sex', 'alive', self.name])

        builder.value.register_value_modifier('disability_weight', modifier=self.disability_weight)

        exposure_data = builder.data.load(project_globals.VITAMIN_A_DEFICIENCY_EXPOSURE)
        exposure_data = pivot_categorical(exposure_data)
        exposure_data = exposure_data.drop('cat2', axis=1)

        self._base_exposure = builder.lookup.build_table(exposure_data, key_columns=['sex'],
                                                         parameter_columns=['age', 'year'])
        self.exposure_proportion = builder.value.register_value_producer(f'{self.name}.exposure_parameters',
                                                                         source=self.exposure_proportion_source,
                                                                         requires_values=[f'{self.name}.exposure_parameters.paf'],
                                                                         requires_columns=['age', 'sex'])
        base_paf = builder.lookup.build_table(0)
        self.joint_paf = builder.value.register_value_producer(f'{self.name}.exposure_parameters.paf',
                                                               source=lambda index: [base_paf(index)],
                                                               preferred_combiner=list_combiner,
                                                               preferred_post_processor=union_post_processor)

        self.randomness = builder.randomness.get_stream(f'initial_{self.name}_propensity')

        self.exposure = builder.value.register_value_producer(
            f'{self.name}.exposure',
            source=self.get_current_exposure,
            requires_columns=['age', 'sex', f'{self.name}_propensity'])

        builder.event.register_listener('time_step', self.on_time_step)

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        # Remains constant throughout the simulation
        propensity = self.randomness.get_draw(pop_data.index)

        exposure = self._get_sample_exposure(propensity)
        disease_status = exposure.map({'cat1': project_globals.VITAMIN_A_BAD, 'cat2': project_globals.VITAMIN_A_GOOD})
        pop_update = pd.DataFrame({
            self.name: disease_status,
            project_globals.VITAMIN_A_BAD_EVENT_TIME: pd.NaT,
            project_globals.VITAMIN_A_BAD_EVENT_COUNT: 0,
            project_globals.VITAMIN_A_GOOD_EVENT_TIME: pd.NaT,
            project_globals.VITAMIN_A_GOOD_EVENT_COUNT: 0,
            project_globals.VITAMIN_A_PROPENSITY: propensity
            }, index=pop_data.index)
        self.population_view.update(pop_update)

    def on_time_step(self, event: 'Event'):
        pop = self.population_view.get(event.index)
        exposure = self.exposure(event.index)

        current_disease_status = exposure.map({'cat1': project_globals.VITAMIN_A_BAD,
                                               'cat2': project_globals.VITAMIN_A_GOOD})
        old_disease_status = pop[self.name]

        incident_cases = ((old_disease_status == project_globals.VITAMIN_A_GOOD)
                          & (current_disease_status == project_globals.VITAMIN_A_BAD))
        remitted_cases = ((old_disease_status == project_globals.VITAMIN_A_GOOD)
                          & (current_disease_status == project_globals.VITAMIN_A_BAD))

        pop[self.name] = current_disease_status
        pop.loc[incident_cases, project_globals.VITAMIN_A_BAD_EVENT_TIME] = event.time
        pop.loc[remitted_cases, project_globals.VITAMIN_A_GOOD_EVENT_TIME] = event.time
        pop.loc[incident_cases, project_globals.VITAMIN_A_BAD_EVENT_COUNT] += 1
        pop.loc[remitted_cases, project_globals.VITAMIN_A_GOOD_EVENT_COUNT] += 1

        self.population_view.update(pop)

    def compute_disability_weight(self, index):
        disability_weight = pd.Series(0, index=index)
        with_condition = self.population_view.get(index, query=f'alive=="alive" and {self.name}=="{self.name}"').index
        disability_weight.loc[with_condition] = self.base_disability_weight(with_condition)
        return disability_weight

    def exposure_proportion_source(self, index):
        base_exposure = self._base_exposure(index).values
        joint_paf = self.joint_paf(index).values
        return pd.Series(base_exposure * (1-joint_paf), index=index, name='values')

    def get_current_exposure(self, index):
        propensity = self.randomness.get_draw(index)
        return self._get_sample_exposure(propensity)

    def _get_sample_exposure(self, propensity):
        exposed = propensity < self.exposure_proportion(propensity.index)
        exposure = pd.Series(exposed.replace({True: 'cat1', False: 'cat2'}),
                             name=self.name + '_exposure', index=propensity.index)
        return exposure


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
        columns_created = ['iron_responsive', f'{self.name}_propensity']
        columns_required = ['age', 'sex']
        self.randomness = builder.randomness.get_stream(f'{self.name}.propensity')
        threshold_data = self.load_iron_responsiveness_threshold(builder)
        self.thresholds = builder.lookup.build_table(threshold_data,
                                                     key_columns=['sex'],
                                                     parameter_columns=['age', 'year'])
        disability_weight_data = self.load_disability_weight_data(builder)
        self.raw_disability_weight = builder.lookup.build_table(disability_weight_data,
                                                                key_columns=['sex'],
                                                                parameter_columns=['age', 'year'])
        self.disability_weight = builder.value.register_value_producer(f'{self.name}.disability_weight',
                                                                       source=self.get_disability_weight,
                                                                       requires_columns=['age', 'sex'],
                                                                       requires_values=[f'{self.name}.exposure'])

        self.exposure = builder.value.register_value_producer(f'{self.name}.exposure',
                                                              source=self.get_exposure,
                                                              requires_values=[f'{self.name}.exposure_parameters'])

        self.population_view = builder.population.get_view(columns_created + columns_required)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created,
                                                 requires_columns=columns_required,
                                                 requires_values=[f'{self.name}.exposure_parameters'],
                                                 requires_streams=[f'{self.name}.propensity'])

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        propensity = self.randomness.get_draw(pop_data.index)
        iron_responsive_propensity = self.randomness.get_draw(pop_data.index, additional_key='iron_responsiveness')
        exposure = self._compute_exposure(propensity)
        severity = self._get_severity(exposure)
        thresholds = self.thresholds(pop_data.index).lookup(pop_data.index, severity)

        pop_update = pd.DataFrame({
            f'{self.name}.propensity': propensity,
            'iron_responsive': iron_responsive_propensity < thresholds,
        }, index=pop_data.index)
        self.population_view.update(pop_update)

    def get_exposure(self, index):
        propensity = self.population_view.subview([f'{self.name}_propensity']).get(index)
        return self._compute_exposure(propensity)

    def get_disability_weight(self, index):
        disability_data = self.raw_disability_weight(index)
        exposure = self.exposure(index)
        severity = self._get_severity(exposure)
        return disability_data.lookup(index, severity)

    def _compute_exposure(self, propensity):
        return self._distribution.ppf(propensity)

    def _get_severity(self, exposure):
        age = self.population_view.subview(['age']).get(exposure.index)
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
            'none': project_globals.IRON_DEFICIENCY_NO_ANEMIA_IRON_RESPONSIVE_PROPORTION,
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
        return pd.concat(data, axis=1).reset_index()

    def load_disability_weight_data(self, builder):
        data = []
        keys = {
            'mild': project_globals.IRON_DEFICIENCY_MILD_ANEMIA_DISABILITY_WEIGHT,
            'moderate': project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_DISABILITY_WEIGHT,
            'severe': project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_DISABILITY_WEIGHT,
        }
        for severity, data_key in keys.items():
            disability_weight = builder.data.load(data_key)
            disability_weight = (disability_weight
                                 .set_index([c for c in disability_weight.columns if c != 'value'])
                                 .rename(columns={'value': severity}))
            data.append(disability_weight)
        data = pd.concat(data, axis=1).reset_index()
        data.loc[:, 'none'] = 0
        return data


class IronDeficiencyDistribution:

    @property
    def name(self):
        return f'{project_globals.IRON_DEFICIENCY_MODEL_NAME}_exposure_distribution'

    def setup(self, builder: 'Builder'):
        exposure_mean = builder.data.load(project_globals.IRON_DEFICIENCY_EXPOSURE)
        exposure_mean = (exposure_mean
                         .set_index([c for c in exposure_mean.columns if c != 'value'])
                         .rename(columns={'value': 'mean'}))
        exposure_sd = builder.data.load(project_globals.IRON_DEFICIENCY_EXPOSURE_SD)
        exposure_sd = (exposure_sd
                       .set_index([c for c in exposure_sd.columns if c != 'value'])
                       .rename(columns={'value': 'sd'}))
        exposure_data = builder.lookup.build_table(pd.concat([exposure_mean, exposure_sd], axis=1),
                                                   key_columns=['sex'],
                                                   parameter_columns=['age', 'year'])
        self.exposure = builder.value.register_value_producer(
            f'{project_globals.IRON_DEFICIENCY_MODEL_NAME}.exposure_parameters',
            source=exposure_data,
            requires_columns=['age', 'sex']
        )

    def ppf(self, propensity: pd.Series) -> pd.Series:
        exposure_data = self.exposure(propensity.index)
        mean = exposure_data['mean']
        sd = exposure_data['sd']
        weight_gamma = 0.4
        weight_gumbel = 0.6
        exposure = (weight_gamma * self._gamma_ppf(propensity, mean, sd)
                    + weight_gumbel * self._gumbel_ppf(propensity, mean, sd))
        return pd.Series(exposure, index=propensity.index, name='value')

    @staticmethod
    def _gamma_ppf(propensity, mean, sd):
        shape = (mean / sd)**2
        scale = sd**2 / mean
        return scipy.stats.gamma(a=shape, scale=scale).ppf(propensity)

    @staticmethod
    def _gumbel_ppf(propensity, mean, sd):
        x_max = 220
        alpha = x_max - mean - (sd * np.euler_gamma * np.sqrt(6) / np.pi)
        scale = sd * np.sqrt(6) / np.pi
        return x_max - scipy.stats.gumbel_r(alpha, scale=scale).ppf(1 - propensity)




