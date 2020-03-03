"""
========================
The Core Mortality Model
========================

This module contains tools modeling all cause mortality and hooks for
disease models to contribute cause-specific and excess mortality.

"""
import pandas as pd

from vivarium.framework.values import union_post_processor, list_combiner


class Mortality:

    @property
    def name(self):
        return 'mortality'

    def setup(self, builder):
        all_cause_mortality_data = builder.data.load("cause.all_causes.cause_specific_mortality_rate")
        self.all_cause_mortality_rate = builder.lookup.build_table(all_cause_mortality_data, key_columns=['sex'],
                                                                   parameter_columns=['age', 'year'])

        self.cause_specific_mortality_rate = builder.value.register_value_producer(
            'cause_specific_mortality_rate', source=builder.lookup.build_table(0)
        )

        self.mortality_rate = builder.value.register_rate_producer('mortality_rate',
                                                                   source=self.calculate_mortality_rate,
                                                                   requires_columns=['age', 'sex'])
        self.mortality_hazard = builder.value.register_value_producer('all_causes.mortality_hazard',
                                                                      source=self._mortality_hazard)
        self._mortality_hazard_paf = builder.value.register_value_producer(
            'all_causes.mortality_hazard.population_attributable_fraction',
            source=lambda index: [pd.Series(0, index=index)],
            preferred_combiner=list_combiner,
            preferred_post_processor=union_post_processor,
        )

        life_expectancy_data = builder.data.load("population.theoretical_minimum_risk_life_expectancy")
        self.life_expectancy = builder.lookup.build_table(life_expectancy_data, parameter_columns=['age'])

        self.random = builder.randomness.get_stream('mortality_handler')
        self.clock = builder.time.clock()

        columns_created = ['cause_of_death', 'years_of_life_lost']
        view_columns = columns_created + ['alive', 'exit_time', 'age', 'sex', 'location']
        self.population_view = builder.population.get_view(view_columns)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created)

        builder.event.register_listener('time_step', self.on_time_step, priority=0)

    def on_initialize_simulants(self, pop_data):
        pop_update = pd.DataFrame({'cause_of_death': 'not_dead',
                                   'years_of_life_lost': 0.},
                                  index=pop_data.index)
        self.population_view.update(pop_update)

    def on_time_step(self, event):
        pop = self.population_view.get(event.index, query="alive =='alive'")
        mortality_hazard = self.mortality_hazard(pop.index)
        deaths = self.random.filter_for_rate(pop.index, mortality_hazard, additional_key='death')
        if not deaths.empty:
            cause_of_death_weights = self.mortality_rate(deaths).divide(mortality_hazard.loc[deaths], axis=0)
            cause_of_death = self.random.choice(deaths, cause_of_death_weights.columns, cause_of_death_weights,
                                                additional_key='cause_of_death')
            pop.loc[deaths, 'alive'] = 'dead'
            pop.loc[deaths, 'exit_time'] = event.time
            pop.loc[deaths, 'years_of_life_lost'] = self.life_expectancy(deaths)
            pop.loc[deaths, 'cause_of_death'] = cause_of_death
            self.population_view.update(pop)


    def calculate_mortality_rate(self, index):
        acmr = self.all_cause_mortality_rate(index)
        csmr = self.cause_specific_mortality_rate(index, skip_post_processor=True)
        cause_deleted_mortality_rate = acmr - csmr
        return pd.DataFrame({'other_causes': cause_deleted_mortality_rate})

    def _mortality_hazard(self, index):
        mortality_rates = pd.DataFrame(self.mortality_rate(index))
        mortality_hazard = mortality_rates.sum(axis=1)
        paf = self._mortality_hazard_paf(index)
        return mortality_hazard * (1 - paf)

    def __repr__(self):
        return "Mortality()"
