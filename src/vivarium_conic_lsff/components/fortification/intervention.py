import typing

import pandas as pd
from vivarium_public_health.utilities import to_years

from vivarium_conic_lsff.components.fortification.parameters import (sample_folic_acid_coverage,
                                                                     FOLIC_ACID_DELAY,
                                                                     FOLIC_ACID_ANNUAL_PROPORTION_INCREASE)

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder


class FortificationIntervention:

    configuration_defaults = {
        'fortification_intervention': {
            'scenario': 'baseline',
            'intervention_start': {
                'year': 2021,
                'month': 7,
                'day': 2
            }
        }
    }

    @property
    def name(self):
        return 'fortification_intervention'

    def setup(self, builder: 'Builder'):
        self.sim_start = pd.Timestamp(**builder.configuration.time.start.to_dict())
        self.intervention_start = pd.Timestamp(
            **builder.configuration.fortification_intervention.intervention_start.to_dict()
        )
        self.clock = builder.time.clock()

        coverage_start = self.load_coverage_data(builder, 'intervention_start')
        self.coverage_start = builder.lookup.build_table(coverage_start)
        coverage_end = self.load_coverage_data(builder, 'intervention_end')
        self.coverage_end = builder.lookup.build_table(coverage_end)
        scenario = builder.configuration.fortification_intervention.scenario
        if scenario == 'folic_acid_fortification_scale_up':
            builder.value.register_value_modifier('folic_acid_fortification.coverage_level',
                                                  self.adjust_coverage_level)
            builder.value.register_value_modifier('folic_acid_fortification.effective_coverage_level',
                                                  self.adjust_effective_coverage_level)

    def adjust_coverage_level(self, index, coverage):
        time_since_start = max(to_years(self.clock() - self.intervention_start), 0)
        c_start, c_end = self.coverage_start(index), self.coverage_end(index)
        delta_coverage = c_end - (c_end - c_start)*(1 - FOLIC_ACID_ANNUAL_PROPORTION_INCREASE)**time_since_start
        return coverage - delta_coverage

    def adjust_effective_coverage_level(self, index, coverage):
        import pdb; pdb.set_trace()
        time_since_start = max(to_years(self.clock() - (self.intervention_start + FOLIC_ACID_DELAY)), 0)
        c_start, c_end = self.coverage_start(index), self.coverage_end(index)
        delta_coverage = c_end - (c_end - c_start)*(1 - FOLIC_ACID_ANNUAL_PROPORTION_INCREASE)**time_since_start
        return coverage - delta_coverage

    @staticmethod
    def load_coverage_data(builder: 'Builder', coverage_time: str) -> float:
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_coverage(location, draw, coverage_time)
