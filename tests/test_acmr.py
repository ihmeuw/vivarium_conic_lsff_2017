# Tests of all-cause mortality rate
# which has been a challenge, due to
# the complexity of lbwsg

import numpy as np, pandas as pd
from vivarium import InteractiveContext

def test_acmr():
    sim = InteractiveContext('src/vivarium_conic_lsff/model_specifications/india.yaml')
    sim.step() # make sure everything is setup

    pop = sim.get_population()
    age_groups = pd.cut(pop.age, bins=[0,7/365, 28/365, 1, 5], right=False)

    mr_pipeline = sim.get_value('mortality_rate')

    acmr_orig = mr_pipeline.source(pop.index).sum(axis=1)
    acmr_w_risk = mr_pipeline(pop.index).sum(axis=1) * 365 # convert back to "per person-year"

    # confirm that they are *not* identical at the individual level
    assert not np.allclose(acmr_orig,
                           acmr_w_risk, rtol=.05), 'expect acmr to be quite different for some individuals'

    # but close at pop level
    assert np.allclose(acmr_orig.groupby(age_groups).median(),
                       acmr_w_risk.groupby(age_groups).median(), rtol=.1), 'expect acmr to be within 10% of original at population level'
