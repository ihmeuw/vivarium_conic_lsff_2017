from vivarium_public_health.disease import SusceptibleState, DiseaseState, DiseaseModel


def NeonatalSIS(cause):
    with_condition_data_functions = {'birth_prevalence':
                                     lambda cause, builder: builder.data.load(f"cause.{cause}.birth_prevalence")}

    healthy = SusceptibleState(cause)
    with_condition = DiseaseState(cause, get_data_functions=with_condition_data_functions)

    healthy.allow_self_transitions()
    healthy.add_transition(with_condition, source_data_type='rate')
    with_condition.allow_self_transitions()
    with_condition.add_transition(healthy, source_data_type='rate')

    return DiseaseModel(cause, states=[healthy, with_condition])