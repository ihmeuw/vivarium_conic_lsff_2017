import typing
import pandas as pd

from vivarium.framework.values import list_combiner, union_post_processor
from vivarium_public_health.risks.data_transformations import pivot_categorical
from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.event import Event
    from vivarium.framework.population import SimulantData


class VitaminADeficiency:

    @property
    def name(self):
        return project_globals.VITAMIN_A_MODEL_NAME

    def setup(self, builder: 'Builder'):
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

        self.clock = builder.time.clock()

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
