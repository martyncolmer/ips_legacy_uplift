'''
Created on 1 Jun 2018

@author: poweld2
'''

import pandas as pd
import numpy as np


# Prepare survey data
def r_survey_input(df_survey_input):
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates input data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
                   df_ges_input - A data frame containing weighting information for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Load survey Data
    df_survey_input = df_survey_input

    # Sort input values
    sort1 = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['SAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['SAMP_PORT_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['SAMP_PORT_GRP_PV',
                                                                              'ARRIVEDEPART'], how='left')

    # Create traffic design weight used within GES weighting
    values = df_aux_variables.SHIFT_WT * df_aux_variables.NON_RESPONSE_WT * df_aux_variables.MINS_WT
    df_aux_variables['trafDesignWeight'] = values

    # Create input to pass into GES weighting
    df_r_ges_input = df_aux_variables.sort_values(['SERIAL'])
    df_r_ges_input = df_r_ges_input[~df_r_ges_input['T1'].isnull()]
    df_r_ges_input['SERIAL'] = df_r_ges_input.SERIAL.astype(np.float64)
    df_r_ges_input = df_r_ges_input[['SERIAL', 'ARRIVEDEPART', 'PORTROUTE', 'SAMP_PORT_GRP_PV', 'SHIFT_WT',
                                     'NON_RESPONSE_WT', 'MINS_WT', 'trafDesignWeight', 'T1']]

    # Export dataframes to CSV
    df_r_ges_input.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\df_r_ges_input.csv",
                          index=False)

    return df_r_ges_input


# Prepare population totals to create AUX lookup variables
def r_population_input(df_survey_input, df_tr_totals, lookup):
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates population data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
                   df_tr_totals - A data frame containing population information for
                   processing year
                   lookup - A data frame containing references to each port routing
                   for processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Load data
    df_survey_input = df_survey_input

    # Sort input values
    sort1 = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['SAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

    df_tr_totals = df_tr_totals

    df_tr_totals['SAMP_PORT_GRP_PV'] = df_tr_totals['SAMP_PORT_GRP_PV'].str.decode("utf-8")

    # Sort input values
    df_pop_totals = df_tr_totals.sort_values(sort1)

    # Cleanse data
    df_pop_totals = df_pop_totals[~df_pop_totals['SAMP_PORT_GRP_PV'].isnull()]
    df_pop_totals = df_pop_totals[~df_pop_totals['ARRIVEDEPART'].isnull()]

    # Create unique list of items from survey input
    items = df_survey_input_sorted['SAMP_PORT_GRP_PV'].tolist()
    unique = []
    [unique.append(x) for x in items if x not in unique]

    df_pop_totals_match = df_pop_totals[df_pop_totals['SAMP_PORT_GRP_PV'].isin(unique)]

    # Create traffic totals
    df_pop_totals_match = df_pop_totals_match.sort_values(['ARRIVEDEPART','SAMP_PORT_GRP_PV'])
    df_traffic_totals = df_pop_totals_match.groupby(['SAMP_PORT_GRP_PV',
                                                    'ARRIVEDEPART']).agg({"TRAFFICTOTAL": 'sum'}).reset_index()

    lookup = lookup

    # Create population totals for current survey data - Cleanse data and merge
    lookup_dataframe_aux = lookup[['SAMP_PORT_GRP_PV','ARRIVEDEPART', 'T1']]
    lookup_dataframe_aux['T1'] = lookup_dataframe_aux.T1.astype(np.int64)

    df_mod_totals = pd.merge(df_traffic_totals, lookup_dataframe_aux, on=['SAMP_PORT_GRP_PV',
                                                                          'ARRIVEDEPART'], how='left')
    df_mod_totals['C_group'] = 1
    df_mod_totals = df_mod_totals.drop(columns=['ARRIVEDEPART', 'SAMP_PORT_GRP_PV'])
    df_mod_pop_totals = df_mod_totals.pivot_table(index='C_group',
                                                  columns='T1',
                                                  values='TRAFFICTOTAL')
    df_mod_pop_totals = df_mod_pop_totals.add_prefix('T_')
    df_mod_pop_totals['C_group'] = 1
    cols = ['C_group'] + [col for col in df_mod_pop_totals if col != 'C_group']
    df_mod_pop_totals = df_mod_pop_totals[cols]

    # Export dataframes to CSV
    df_mod_pop_totals.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\df_pop_totals.csv",
                             index=False)

    return df_mod_pop_totals







