# Tests of all-cause mortality rate
# which has been a challenge, due to
# the complexity of lbwsg

import numpy as np, pandas as pd
from vivarium import InteractiveContext

def test_acmr():
    sim = InteractiveContext('src/vivarium_conic_lsff/model_specifications/india.yaml')

    pop = sim.get_population()
    age_groups = pd.cut(pop.age, bins=[0,7/365, 28/365, 1, 5], right=False)

    acmr_pipeline = sim.get_value('all_causes.mortality_hazard')

    acmr_orig = acmr_pipeline.source(pop.index).groupby(age_groups).median()
    acmr_w_risk = acmr_pipeline(pop.index).groupby(age_groups).median()

    assert np.allclose(acmr_orig, acmr_w_risk, rtol=.05), 'expect acmr to be within 5% of original'

