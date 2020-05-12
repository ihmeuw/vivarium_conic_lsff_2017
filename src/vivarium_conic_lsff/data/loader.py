"""Loads, standardizes and validates input data for the simulation.

Abstract the extract and transform pieces of the artifact ETL.
The intent here is to provide a uniform interface around this portion
of artifact creation. The value of this interface shows up when more
complicated data needs are part of the project. See the BEP project
for an example.

`BEP <https://github.com/ihmeuw/vivarium_gates_bep/blob/master/src/vivarium_gates_bep/data/loader.py>`_

.. admonition::

   No logging is done here. Logging is done in vivarium inputs itself and forwarded.
"""
import pandas as pd
import numpy as np

from get_draws.api import get_draws
from gbd_mapping import causes, risk_factors, covariates, sequelae
from vivarium.framework.artifact import EntityKey
from vivarium_inputs import interface, utilities, utility_data, globals as vi_globals, extract
from vivarium_inputs.mapping_extension import alternative_risk_factors
import vivarium_inputs.validation.sim as validation

from vivarium_conic_lsff import paths, globals as project_globals


def get_data(lookup_key: str, location: str) -> pd.DataFrame:
    """Retrieves data from an appropriate source.

    Parameters
    ----------
    lookup_key
        The key that will eventually get put in the artifact with
        the requested data.
    location
        The location to get data for.

    Returns
    -------
        The requested data.

    """
    mapping = {
        project_globals.POPULATION_STRUCTURE: load_population_structure,
        project_globals.POPULATION_AGE_BINS: load_age_bins,
        project_globals.POPULATION_DEMOGRAPHY: load_demographic_dimensions,
        project_globals.POPULATION_TMRLE: load_theoretical_minimum_risk_life_expectancy,
        project_globals.ALL_CAUSE_CSMR: load_standard_data,
        project_globals.COVARIATE_LIVE_BIRTHS_BY_SEX: load_standard_data,

        project_globals.DIARRHEA_PREVALENCE: load_standard_data,
        project_globals.DIARRHEA_INCIDENCE_RATE: load_standard_data,
        project_globals.DIARRHEA_REMISSION_RATE: load_standard_data,
        project_globals.DIARRHEA_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.DIARRHEA_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.DIARRHEA_DISABILITY_WEIGHT: load_standard_data,
        project_globals.DIARRHEA_RESTRICTIONS: load_metadata,

        project_globals.MEASLES_PREVALENCE: load_standard_data,
        project_globals.MEASLES_INCIDENCE_RATE: load_standard_data,
        project_globals.MEASLES_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.MEASLES_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.MEASLES_DISABILITY_WEIGHT: load_standard_data,
        project_globals.MEASLES_RESTRICTIONS: load_metadata,

        project_globals.LRI_PREVALENCE: load_standard_data,
        project_globals.LRI_BIRTH_PREVALENCE: load_lri_birth_prevalence_from_meid,
        project_globals.LRI_INCIDENCE_RATE: load_standard_data,
        project_globals.LRI_REMISSION_RATE: load_standard_data,
        project_globals.LRI_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.LRI_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.LRI_DISABILITY_WEIGHT: load_standard_data,
        project_globals.LRI_RESTRICTIONS: load_metadata,

        project_globals.NEURAL_TUBE_DEFECTS_CAUSE_SPECIFIC_MORTALITY_RATE: load_standard_data,
        project_globals.NEURAL_TUBE_DEFECTS_PREVALENCE: load_standard_data,
        project_globals.NEURAL_TUBE_DEFECTS_BIRTH_PREVALENCE: load_standard_data,
        project_globals.NEURAL_TUBE_DEFECTS_EXCESS_MORTALITY_RATE: load_standard_data,
        project_globals.NEURAL_TUBE_DEFECTS_DISABILITY_WEIGHT: load_standard_data,
        project_globals.NEURAL_TUBE_DEFECTS_RESTRICTIONS: load_metadata,

        project_globals.LBWSG_DISTRIBUTION: load_metadata,
        project_globals.LBWSG_CATEGORIES: load_metadata,
        project_globals.LBWSG_EXPOSURE: load_lbwsg_exposure,
        project_globals.LBWSG_RELATIVE_RISK: load_lbwsg_relative_risk,
        project_globals.LBWSG_PAF: load_lbwsg_paf,

        project_globals.VITAMIN_A_DEFICIENCY_CATEGORIES: load_metadata,
        project_globals.VITAMIN_A_DEFICIENCY_RESTRICTIONS: load_metadata,
        project_globals.VITAMIN_A_DEFICIENCY_DISABILITY_WEIGHT: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY_EXPOSURE: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY_RELATIVE_RISK: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY_PAF: load_standard_data,
        project_globals.VITAMIN_A_DEFICIENCY_DISTRIBUTION: load_metadata,

        project_globals.IRON_DEFICIENCY_EXPOSURE: load_standard_data,
        project_globals.IRON_DEFICIENCY_RESTRICTIONS: load_metadata,
        project_globals.IRON_DEFICIENCY_EXPOSURE_SD: load_standard_data,
        project_globals.IRON_DEFICIENCY_MILD_ANEMIA_DISABILITY_WEIGHT: load_iron_deficiency_dw,
        project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_DISABILITY_WEIGHT: load_iron_deficiency_dw,
        project_globals.IRON_DEFICIENCY_SEVERE_ANEMIA_DISABILITY_WEIGHT: load_iron_deficiency_dw,
        project_globals.IRON_DEFICIENCY_NO_ANEMIA_IRON_RESPONSIVE_PROPORTION: load_no_anemia_iron_responsive_proportion,
        project_globals.IRON_DEFICIENCY_MILD_ANEMIA_IRON_RESPONSIVE_PROPORTION: load_iron_responsive_proportion,
        project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_IRON_RESPONSIVE_PROPORTION: load_iron_responsive_proportion,
        project_globals.IRON_DEFICIENCY_SEVERE_ANEMIA_IRON_RESPONSIVE_PROPORTION: load_iron_responsive_proportion,

    }
    return mapping[lookup_key](lookup_key, location)


def load_population_structure(key: str, location: str) -> pd.DataFrame:
    return interface.get_population_structure(location)


def load_age_bins(key: str, location: str) -> pd.DataFrame:
    return interface.get_age_bins()


def load_demographic_dimensions(key: str, location: str) -> pd.DataFrame:
    return interface.get_demographic_dimensions(location)


def load_theoretical_minimum_risk_life_expectancy(key: str, location: str) -> pd.DataFrame:
    return interface.get_theoretical_minimum_risk_life_expectancy()


def load_standard_data(key: str, location: str) -> pd.DataFrame:
    key = EntityKey(key)
    entity = get_entity(key)
    return interface.get_measure(entity, key.measure, location)


def load_metadata(key: str, location: str):
    key = EntityKey(key)
    entity = get_entity(key)
    metadata = entity[key.measure]
    if hasattr(metadata, 'to_dict'):
        metadata = metadata.to_dict()
    return metadata


def load_lbwsg_exposure(key: str, location: str):
    path = paths.lbwsg_data_path('exposure', location)
    data = pd.read_hdf(path)  # type: pd.DataFrame
    data['rei_id'] = risk_factors.low_birth_weight_and_short_gestation.gbd_id

    data = data.drop('modelable_entity_id', 'columns')
    data = data[data.parameter != 'cat124']  # LBWSG data has an extra residual category added by get_draws.
    data = utilities.filter_data_by_restrictions(data, risk_factors.low_birth_weight_and_short_gestation,
                                                 'outer', utility_data.get_age_group_ids())
    tmrel_cat = utility_data.get_tmrel_category(risk_factors.low_birth_weight_and_short_gestation)
    exposed = data[data.parameter != tmrel_cat]
    unexposed = data[data.parameter == tmrel_cat]
    #  FIXME: We fill 1 as exposure of tmrel category, which is not correct.
    data = pd.concat([utilities.normalize(exposed, fill_value=0), utilities.normalize(unexposed, fill_value=1)],
                     ignore_index=True)

    # normalize so all categories sum to 1
    cols = list(set(data.columns).difference(vi_globals.DRAW_COLUMNS + ['parameter']))
    sums = data.groupby(cols)[vi_globals.DRAW_COLUMNS].sum()
    data = (data.groupby('parameter')
            .apply(lambda df: df.set_index(cols).loc[:, vi_globals.DRAW_COLUMNS].divide(sums))
            .reset_index())
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS + ['parameter'])
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    validation.validate_for_simulation(data, risk_factors.low_birth_weight_and_short_gestation,
                                       'exposure', location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_lbwsg_relative_risk(key: str, location: str):
    path = paths.lbwsg_data_path('relative_risk', location)
    data = pd.read_hdf(path)  # type: pd.DataFrame
    data['rei_id'] = risk_factors.low_birth_weight_and_short_gestation.gbd_id
    data = utilities.convert_affected_entity(data, 'cause_id')
    # RRs for all causes are the same.
    data = data[data.affected_entity == 'diarrheal_diseases']
    data['affected_entity'] = 'all'
    # All lbwsg risk is about mortality.
    data.loc[:, 'affected_measure'] = 'excess_mortality_rate'
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS
                       + ['affected_entity', 'affected_measure', 'parameter']
                       + vi_globals.DRAW_COLUMNS)
    data = (
        data
            .groupby(['affected_entity', 'parameter'])
            .apply(utilities.normalize, fill_value=1)
            .reset_index(drop=True)
    )

    tmrel_cat = utility_data.get_tmrel_category(risk_factors.low_birth_weight_and_short_gestation)
    tmrel_mask = data.parameter == tmrel_cat
    data.loc[tmrel_mask, vi_globals.DRAW_COLUMNS] = (
        data
            .loc[tmrel_mask, vi_globals.DRAW_COLUMNS]
            .mask(np.isclose(data.loc[tmrel_mask, vi_globals.DRAW_COLUMNS], 1.0), 1.0)
    )

    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    validation.validate_for_simulation(data, risk_factors.low_birth_weight_and_short_gestation,
                                       'relative_risk', location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_lbwsg_paf(key: str, location: str):
    path = paths.lbwsg_data_path('population_attributable_fraction', location)
    data = pd.read_hdf(path)  # type: pd.DataFrame
    data['rei_id'] = risk_factors.low_birth_weight_and_short_gestation.gbd_id
    data = data[data.metric_id == vi_globals.METRICS['Percent']]
    # All lbwsg risk is about mortality.
    data = data[data.measure_id.isin([vi_globals.MEASURES['YLLs']])]

    temp = []
    causes_map = {c.gbd_id: c for c in causes}
    # We filter paf age groups by cause level restrictions.
    for (c_id, measure), df in data.groupby(['cause_id', 'measure_id']):
        cause = causes_map[c_id]
        measure = 'yll' if measure == vi_globals.MEASURES['YLLs'] else 'yld'
        df = utilities.filter_data_by_restrictions(df, cause, measure, utility_data.get_age_group_ids())
        temp.append(df)
    data = pd.concat(temp, ignore_index=True)

    data = utilities.convert_affected_entity(data, 'cause_id')
    data.loc[data['measure_id'] == vi_globals.MEASURES['YLLs'], 'affected_measure'] = 'excess_mortality_rate'
    data = (data.groupby(['affected_entity', 'affected_measure'])
            .apply(utilities.normalize, fill_value=0)
            .reset_index(drop=True))
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS
                       + ['affected_entity', 'affected_measure']
                       + vi_globals.DRAW_COLUMNS)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    validation.validate_for_simulation(data, risk_factors.low_birth_weight_and_short_gestation,
                                       'population_attributable_fraction', location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_iron_deficiency_dw(key: str, location: str):
    sequela_map = {
        project_globals.IRON_DEFICIENCY_MILD_ANEMIA_DISABILITY_WEIGHT:
            'sequela.mild_iron_deficiency_anemia.disability_weight',
        project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_DISABILITY_WEIGHT:
            'sequela.moderate_iron_deficiency_anemia.disability_weight',
        project_globals.IRON_DEFICIENCY_SEVERE_ANEMIA_DISABILITY_WEIGHT:
            'sequela.severe_iron_deficiency_anemia.disability_weight',
    }
    data_key = sequela_map[key]
    return load_standard_data(data_key, location)


def load_no_anemia_iron_responsive_proportion(key: str, location: str):
    responsive_ids, non_responsive_ids = [], []
    for responsive, non_responsive in project_globals.ANEMIA_SEQUELAE_ID_MAP.values():
        responsive_ids.extend(responsive)
        non_responsive_ids.extend(non_responsive)

    responsive_sequelae = [s for s in sequelae if s.gbd_id in responsive_ids]
    non_responsive_sequelae = [s for s in sequelae if s.gbd_id in non_responsive_ids]

    all_prevalence = []
    iron_responsive_prevalence = []
    for sequela in responsive_sequelae:
        try:
            prevalence = interface.get_measure(sequela, 'prevalence', location)
        except (extract.DataDoesNotExistError, extract.DataAbnormalError):
            continue
        all_prevalence.append(prevalence)
        iron_responsive_prevalence.append(prevalence)
    for sequela in non_responsive_sequelae:
        try:
            prevalence = interface.get_measure(sequela, 'prevalence', location)
        except (extract.DataDoesNotExistError, extract.DataAbnormalError):
            continue
        all_prevalence.append(prevalence)
    all_prevalence = sum(all_prevalence)
    iron_responsive_prevalence = sum(iron_responsive_prevalence)
    non_responsive_prevalence = all_prevalence - iron_responsive_prevalence

    other_anemias_prevalence = interface.get_measure(causes.hemoglobinopathies_and_hemolytic_anemias,
                                                     'prevalence', location)
    hiv_prevalence = interface.get_measure(causes.hiv_aids, 'prevalence', location)
    malaria_prevalence = interface.get_measure(causes.malaria, 'prevalence', location)
    reverse_causal_prevalence = other_anemias_prevalence + hiv_prevalence + malaria_prevalence

    proportion = (1 - all_prevalence
                  - (reverse_causal_prevalence - non_responsive_prevalence)/(1 - all_prevalence)).fillna(0)
    return proportion


def load_iron_responsive_proportion(key: str, location: str):
    sequela_map = {
        project_globals.IRON_DEFICIENCY_MILD_ANEMIA_IRON_RESPONSIVE_PROPORTION:
            project_globals.ANEMIA_SEQUELAE_ID_MAP['mild'],
        project_globals.IRON_DEFICIENCY_MODERATE_ANEMIA_IRON_RESPONSIVE_PROPORTION:
            project_globals.ANEMIA_SEQUELAE_ID_MAP['moderate'],
        project_globals.IRON_DEFICIENCY_SEVERE_ANEMIA_IRON_RESPONSIVE_PROPORTION:
            project_globals.ANEMIA_SEQUELAE_ID_MAP['severe'],
    }
    responsive_ids, non_responsive_ids = sequela_map[key]

    responsive_prevalence = []
    for s_id in responsive_ids:
        sequela = [s for s in sequelae if s.gbd_id == s_id]
        if sequela:
            sequela = sequela.pop()
        else:
            continue
        try:
            prevalence = interface.get_measure(sequela, 'prevalence', location)
        except (extract.DataDoesNotExistError, extract.DataAbnormalError):
            continue
        responsive_prevalence.append(prevalence)
    responsive_prevalence = sum(responsive_prevalence)

    non_responsive_prevalence = []
    for s_id in non_responsive_ids:
        sequela = [s for s in sequelae if s.gbd_id == s_id]
        if sequela:
            sequela = sequela.pop()
        else:
            continue
        try:
            prevalence = interface.get_measure(sequela, 'prevalence', location)
        except (extract.DataDoesNotExistError, extract.DataAbnormalError):
            continue
        non_responsive_prevalence.append(prevalence)
    non_responsive_prevalence = sum(non_responsive_prevalence)

    return (responsive_prevalence / (responsive_prevalence + non_responsive_prevalence)).fillna(0)

def load_lri_birth_prevalence_from_meid(_, location):
    """Ignore the first argument to fit in to the get_data model. """
    location_id = utility_data.get_location_id(location)
    data = get_draws('modelable_entity_id', project_globals.LRI_BIRTH_PREVALENCE_MEID,
                     source=project_globals.LRI_BIRTH_PREVALENCE_DRAW_SOURCE,
                     age_group_id=project_globals.LRI_BIRTH_PREVALENCE_AGE_ID,
                     measure_id=vi_globals.MEASURES['Prevalence'],
                     gbd_round_id=project_globals.LRI_BIRTH_PREVALENCE_GBD_ROUND,
                     location_id=location_id)
    data = data[data.measure_id == vi_globals.MEASURES['Prevalence']]
    data = utilities.normalize(data, fill_value=0)

    idx_columns = vi_globals.DEMOGRAPHIC_COLUMNS
    idx_columns.remove('age_group_id')
    data = data.filter(idx_columns + vi_globals.DRAW_COLUMNS)

    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def get_entity(key: str):
    # Map of entity types to their gbd mappings.
    type_map = {
        'cause': causes,
        'covariate': covariates,
        'risk_factor': risk_factors,
        'alternative_risk_factor': alternative_risk_factors,
        'sequela': sequelae
    }
    key = EntityKey(key)
    return type_map[key.type][key.name]
