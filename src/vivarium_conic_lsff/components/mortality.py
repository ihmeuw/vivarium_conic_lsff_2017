"""
========================
The Core Mortality Model
========================

Summary
=======

The mortality component models all cause mortality and allows for disease
models to to contribute cause specific mortality. At each timestep the
currently "alive" population is subjected to a mortality event that uses
the mortality hazard data to reap simulants. A weighted probable cause of
death is used to pick a cause of death. The years of life lost are calculated
by subtracting the simulant's age from the population TMRLE and the poplulation
is updated.

Pipelines Exposed
=================

 - cause_specific_mortality_rate
 - mortality_rate
 - all_causes.mortality_hazard


All cause mortality is read from the artifact (GBD). At setup cause specific
mortality is initialized to an empty table. As disease models are incorporated
they register as affecting cause specific mortality and their contributions
are reflected in the cause_specific_mortality_rate pipeline. This is population
level data.

The mortality component's mortality_rate pipeline reflects the
cause deleted mortality rate (ACMR - CSMR).

Finally, the mortality component exposes a mortality hazard pipeline and a
mortality hazard PAF pipeline (used internally). The cause specific rates are
summed and added to the cause deleted mortality rate. These values are multiplied
by 1 - PAF. The end product comprises the values in the mortality hazard pipeline.

"""
import pandas as pd

from vivarium.framework.values import union_post_processor, list_combiner
from vivarium_conic_lsff import globals as project_globals


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

        affected_unmodelled_lb_csmr_data = self.load_unmodelled_lb_affected_csmr(builder)
        affected_unmodelled_lb_csmr = builder.lookup.build_table(affected_unmodelled_lb_csmr_data,
                                                                 key_columns=['sex'],
                                                                 parameter_columns=['age', 'year'])

        self.affected_unmodelled_csmr = builder.value.register_value_producer('affected_unmodelled.csmr',
                                                                              source=affected_unmodelled_lb_csmr,
                                                                              requires_columns=['age', 'sex'])

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
        self._affected_unmodelled_mortality_hazard_paf = builder.value.register_value_producer(
            'affected_unmodelled.csmr.population_attributable_fraction',
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
        csmr_unmodelled_raw = self.affected_unmodelled_csmr.source(index)
        csmr_unmodelled = self.affected_unmodelled_csmr(index)
        cause_deleted_mortality_rate = acmr - csmr - csmr_unmodelled_raw + csmr_unmodelled
        return pd.DataFrame({'other_causes': cause_deleted_mortality_rate})

    def _mortality_hazard(self, index):
        mortality_rates = pd.DataFrame(self.mortality_rate(index))
        mortality_hazard = mortality_rates.sum(axis=1)
        paf = self._mortality_hazard_paf(index)
        return mortality_hazard * (1 - paf)

    def load_unmodelled_lb_affected_csmr(self, builder):
        df = pd.DataFrame()
        for idx, cause in enumerate(project_globals.UNMODELLED_LBWSG_AFFECTED_CAUSES):
            if 0 == idx:
                df = builder.data.load(cause)
            else:
                df.loc[:, 'value'] += builder.data.load(cause).value
        return df

    def __repr__(self):
        return "Mortality()"
