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

        self._column = 'mother_ate_folic_acid_fortified_food'
        self.randomness = builder.randomness.get_stream(self._column)
        self.population_view = builder.population.get_view([self._column])
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=[self._column],
                                                 requires_values=['folic_acid_fortification.effective_coverage_level'],
                                                 requires_streams=[self._column])

    def on_initialize_simulants(self, pop_data: 'SimulantData'):
        if pop_data.user_data['sim_state'] == 'setup':  # Initial population
            pop_update = pd.Series('unknown', index=pop_data.index, name=self._column)
        else:  # New sims
            draw = self.randomness.get_draw(pop_data.index)
            effective_coverage = self.effective_coverage_level(pop_data.index)
            pop_update = pd.Series((draw < effective_coverage).map({True: 'true', False: 'false'}),
                                   index=pop_data.index)
        self.population_view.update(pop_update)

    @staticmethod
    def load_coverage_data(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        return sample_folic_acid_coverage(location, draw, 'baseline')
