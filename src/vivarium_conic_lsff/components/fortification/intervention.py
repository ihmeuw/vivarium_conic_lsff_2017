"""Model of an intervention on fortified vehicle coverage."""
import typing
from typing import List

import pandas as pd
from vivarium_public_health.utilities import to_years

from vivarium_conic_lsff.components.fortification.parameters import (sample_folic_acid_coverage,
                                                                     sample_vitamin_a_coverage,
                                                                     sample_iron_fortification_coverage)
from vivarium_conic_lsff import globals as project_globals

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder


class FortificationIntervention:
    """Main component for managing fortification interventions.

    The core responsibility of this component is to manage what impacts
    are happening on the coverage of various fortified vehicle based on
    the scenario being simulated.  Actual interventions on particular vehicles
    are represented as subcomponents of this component.

    """

    configuration_defaults = {
        'fortification_intervention': {
            # One of project_globals.SCENARIOS
            'scenario': project_globals.SCENARIOS.BASELINE,
            # 1 year delay after simulation start.
            'intervention_start': {
                'year': 2021,
                'month': 1,
                'day': 1
            }
        }
    }

    def __init__(self):
        # Add new intervention components here.
        self.folic_acid_intervention = FolicAcidFortificationIntervention()
        self.vitamin_a_intervention = VitaminAFortificationIntervention()
        self.iron_intervention = IronFortificationIntervention()

    @property
    def name(self) -> str:
        """This component's canonical name."""
        return 'fortification_intervention'

    @property
    def sub_components(self) -> List:
        """Specific interventions on fortified vehicle coverage."""
        # Add new intervention components here.
        return [self.folic_acid_intervention, self.vitamin_a_intervention, self.iron_intervention]

    def setup(self, builder: 'Builder'):
        """Select which fortification effects to use based on scenario."""
        scenario_setup = {
            project_globals.SCENARIOS.BASELINE: lambda x: None,
            project_globals.SCENARIOS.FOLIC_ACID: self.scenario_folic_acid,
            project_globals.SCENARIOS.VITAMIN_A: self.scenario_vitamin_a,
            project_globals.SCENARIOS.IRON: self.scenario_iron,
            project_globals.SCENARIOS.IRON_PLUS_FOLIC_ACID: self.scenario_iron_plus_folic_acid,
        }
        scenario = builder.configuration.fortification_intervention.scenario
        scenario_setup[scenario](builder)


    def scenario_folic_acid(self, builder: 'Builder'):
        builder.value.register_value_modifier(
            'folic_acid_fortification.coverage_level',
            self.folic_acid_intervention.adjust_coverage_level)
        builder.value.register_value_modifier(
            'folic_acid_fortification.effective_coverage_level',
            self.folic_acid_intervention.adjust_effective_coverage_level)

    def scenario_vitamin_a(self, builder: 'Builder'):
        builder.value.register_value_modifier(
            'vitamin_a_fortification.coverage_level',
            self.vitamin_a_intervention.adjust_coverage_level
        )

    def scenario_iron(self, builder: 'Builder'):
        builder.value.register_value_modifier(
            'iron_fortification.coverage_level',
            self.iron_intervention.adjust_coverage_level
        )

    def scenario_iron_plus_folic_acid(self, builder: 'Builder'):
        self.scenario_folic_acid(builder)
        self.scenario_iron(builder)

class FolicAcidFortificationIntervention:
    """Intervention on folic acid fortification level.

    This component adjusts both the true, current coverage level of
    folic acid fortified vehicles in the population as well as the effective
    coverage level.  For the intervention to be effective, mother's must start
    eating folic acid fortified foods some period before conception, which
    produces a time delay in effective coverage levels.

    """

    @property
    def name(self):
        """This component's canonical name."""
        return 'folic_acid_fortification_intervention'

    # noinspection PyAttributeOutsideInit,DuplicatedCode
    def setup(self, builder: 'Builder'):
        """Perform this component's setup."""
        self.sim_start = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self.intervention_start = pd.Timestamp(
            **builder.configuration.fortification_intervention.intervention_start.to_dict()
        )
        self.clock = builder.time.clock()

        coverage_start = self.load_coverage_data(builder, 'intervention_start')
        self.coverage_start = builder.lookup.build_table(coverage_start)
        coverage_end = self.load_coverage_data(builder, 'intervention_end')
        self.coverage_end = builder.lookup.build_table(coverage_end)

    # noinspection PyUnusedLocal,DuplicatedCode
    def adjust_coverage_level(self, index, coverage):
        """Adjust the true population coverage of folic acid fortification."""
        time_since_intervention_start = max(to_years(self.clock() - self.intervention_start), 0)
        if time_since_intervention_start > 0:
            c_start, c_end = self.coverage_start(index), self.coverage_end(index)
            # noinspection PyTypeChecker
            new_coverage = (c_end
                            - (c_end - c_start)
                            * (1 - project_globals.FOLIC_ACID_ANNUAL_PROPORTION_INCREASE)**time_since_intervention_start)
            coverage = new_coverage
        return coverage

    # noinspection PyUnusedLocal
    def adjust_effective_coverage_level(self, index, coverage):
        """Adjust the effective coverage of folic acid fortification."""
        time_since_intervention_start = max(
            to_years(self.clock() - (self.intervention_start + project_globals.FOLIC_ACID_DELAY)), 0
        )
        if time_since_intervention_start > 0:
            c_start, c_end = self.coverage_start(index), self.coverage_end(index)
            # noinspection PyTypeChecker
            new_coverage = (c_end
                            - (c_end - c_start)
                            * (1 - project_globals.FOLIC_ACID_ANNUAL_PROPORTION_INCREASE) ** time_since_intervention_start)
            coverage = new_coverage
        return coverage

    @staticmethod
    def load_coverage_data(builder: 'Builder', coverage_time: str) -> float:
        """Loads coverage levels at different periods of the intervention.

        `coverage_time` must be one of 'intervention_start',
        'intervention_end'.

        """
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_coverage(location, draw, coverage_time)


class VitaminAFortificationIntervention:
    """Intervention on vitamin a fortification level."""

    @property
    def name(self):
        """This component's canonical name."""
        return 'vitamin_a_fortification_intervention'

    # noinspection PyAttributeOutsideInit,DuplicatedCode
    def setup(self, builder: 'Builder'):
        """Perform this component's setup."""
        self.sim_start = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self.intervention_start = pd.Timestamp(
            **builder.configuration.fortification_intervention.intervention_start.to_dict()
        )
        self.clock = builder.time.clock()
        coverage_start = self.load_coverage_data(builder, 'intervention_start')
        self.coverage_start = builder.lookup.build_table(coverage_start)
        coverage_end = self.load_coverage_data(builder, 'intervention_end')
        self.coverage_end = builder.lookup.build_table(coverage_end)

    # noinspection PyUnusedLocal,DuplicatedCode
    def adjust_coverage_level(self, index, coverage):
        """Adjust the coverage level of vitamin a fortification."""
        time_since_intervention_start = max(to_years(self.clock() - self.intervention_start), 0)
        if time_since_intervention_start > 0:
            c_start, c_end = self.coverage_start(index), self.coverage_end(index)
            # noinspection PyTypeChecker
            new_coverage = (c_end
                            - (c_end - c_start)
                            * (1 - project_globals.VITAMIN_A_ANNUAL_PROPORTION_INCREASE)**time_since_intervention_start)
            coverage = new_coverage
        return coverage

    @staticmethod
    def load_coverage_data(builder: 'Builder', coverage_time: str) -> float:
        """Loads coverage levels at different periods of the intervention.

        `coverage_time` must be one of 'intervention_start',
        'intervention_end'.

        """
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_vitamin_a_coverage(location, draw, coverage_time)


class IronFortificationIntervention:
    """Intervention on iron fortification level."""

    @property
    def name(self):
        """This component's canonical name."""
        return 'iron_fortification_intervention'

    # noinspection PyAttributeOutsideInit,DuplicatedCode
    def setup(self, builder: 'Builder'):
        """Perform this component's setup."""
        self.sim_start = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self.intervention_start = pd.Timestamp(
            **builder.configuration.fortification_intervention.intervention_start.to_dict()
        )
        self.clock = builder.time.clock()
        coverage_start = self.load_coverage_data(builder, 'intervention_start')
        self.coverage_start = builder.lookup.build_table(coverage_start)
        coverage_end = self.load_coverage_data(builder, 'intervention_end')
        self.coverage_end = builder.lookup.build_table(coverage_end)

    # noinspection PyUnusedLocal,DuplicatedCode
    def adjust_coverage_level(self, index, coverage):
        """Adjust the coverage level of iron fortification."""
        time_since_intervention_start = max(to_years(self.clock() - self.intervention_start), 0)
        if time_since_intervention_start > 0:
            c_start, c_end = self.coverage_start(index), self.coverage_end(index)
            # noinspection PyTypeChecker
            new_coverage = (c_end
                            - (c_end - c_start)
                            * (1 - project_globals.IRON_ANNUAL_PROPORTION_INCREASE)**time_since_intervention_start)
            coverage = new_coverage
        return coverage

    @staticmethod
    def load_coverage_data(builder: 'Builder', coverage_time: str) -> float:
        """Loads coverage levels at different periods of the intervention.

        `coverage_time` must be one of 'intervention_start',
        'intervention_end'.

        """
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_iron_fortification_coverage(location, draw, coverage_time)
