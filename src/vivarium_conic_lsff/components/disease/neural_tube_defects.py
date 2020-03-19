import typing

from vivarium.framework.values import list_combiner, union_post_processor
from vivarium_public_health.disease import SusceptibleState, DiseaseState as DiseaseState_, DiseaseModel

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder


class DiseaseState(DiseaseState_):

    def setup(self, builder: 'Builder'):
        super().setup(builder)
        paf = builder.lookup.build_table(0)
        self.birth_prevalence_joint_paf = builder.value.register_value_producer(
            f'{self.state_id}.birth_prevalence.population_attributable_fraction',
            source=lambda idx: [paf(idx)],
            preferred_combiner=list_combiner,
            preferred_post_processor=union_post_processor
        )
        self._birth_prevalence = self.birth_prevalence
        self.birth_prevalence = builder.value.register_value_producer(
            f'{self.state_id}.birth_prevalence',
            source=self.get_birth_prevalence,
            requires_columns=['sex'],
            requires_values=[f'{self.state_id}.birth_prevalence.population_attributable_fraction']
        )

    def get_birth_prevalence(self, index):
        birth_prevalence = self._birth_prevalence(index)
        joint_paf = self.birth_prevalence_joint_paf(index)
        return birth_prevalence * (1 - joint_paf.values)


def NeonatalSWC_without_incidence(cause):
    with_condition_data_functions = {'birth_prevalence':
                                     lambda cause, builder: builder.data.load(f"cause.{cause}.birth_prevalence")}

    healthy = SusceptibleState(cause)
    with_condition = DiseaseState(cause, get_data_functions=with_condition_data_functions)

    healthy.allow_self_transitions()
    with_condition.allow_self_transitions()

    return DiseaseModel(cause, states=[healthy, with_condition])
