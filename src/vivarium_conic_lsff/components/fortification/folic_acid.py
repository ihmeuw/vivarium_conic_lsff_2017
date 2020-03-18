import typing

import pandas as pd

from vivarium_conic_lsff.components.fortification.parameters import sample_folic_acid_coverage, FOLIC_ACID_DELAY

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.population import SimulantData
    from vivarium.framework.event import Event


class FolicAcidFortificationCoverage:

    @property
    def name(self):
        return 'folic_acid_fortification_coverage'

    def setup(self, builder: 'Builder'):
        coverage_data = self.load_coverage_data(builder)
        self.coverage_level = builder.value.register_value_producer('folic_acid_fortification.coverage_level',
                                                                    source=lambda: coverage_data)
        self.effective_coverage_level = builder.value.register_value_producer(
            'folic_acid_fortification.effective_coverage_level',
            source=lambda: coverage_data
        )

    @staticmethod
    def load_coverage_data(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_coverage(location, draw, 'baseline')
