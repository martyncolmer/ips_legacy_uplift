'''
Created on 1 Jun 2018

@author: poweld2
'''

import pandas as pd


# Prepare survey data
def r_survey_input(df_survey_input, df_ges_input):
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
    sort1 = ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART']
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['UNSAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]
    df_survey_input_sorted.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['UNSAMP_PORT_GRP_PV',
                                                                              'UNSAMP_REGION_GRP_PV',
                                                                              'ARRIVEDEPART'], how='left')

    # Load GES data
    df_ges_input = df_ges_input

    # Cleanse data
    df_ges_input = df_ges_input.rename(index=str, columns={"GES_serial": "SERIAL"})

    # Sort values ready for merge
    df_survey_input_serial = df_survey_input.sort_values(['SERIAL'])
    df_ges_input["SERIAL"] = pd.to_numeric(df_ges_input["SERIAL"])
    df_ges_input_sorted = df_ges_input.sort_values(['SERIAL'])
    df_aux_variables_sorted = df_aux_variables.sort_values(['SERIAL'])

    # Cleanse and merge data ready for input into R GES
    df_aux_variables_column = df_aux_variables_sorted[['SERIAL', 'T1']]

    df_trafdesignwt = pd.merge(df_survey_input_serial, df_ges_input_sorted, on=['SERIAL'], how='left')
    df_trafdesignwt = df_trafdesignwt.drop(columns=['ARRIVEDEPART_y', 'UNSAMP_PORT_GRP_PV_y', 'UNSAMP_REGION_GRP_PV_y'])
    df_trafdesignwt = df_trafdesignwt.rename(index=str, columns={"UNSAMP_PORT_GRP_PV_x": "UNSAMP_PORT_GRP_PV"})
    df_trafdesignwt = df_trafdesignwt.rename(index=str, columns={"UNSAMP_REGION_GRP_PV_x": "UNSAMP_REGION_GRP_PV"})
    df_trafdesignwt = df_trafdesignwt.rename(index=str, columns={"ARRIVEDEPART_x": "ARRIVEDEPART"})

    df_r_ges_input = pd.merge(df_trafdesignwt, df_aux_variables_column, on=['SERIAL'], how='left')
    df_r_ges_input = df_r_ges_input[~df_r_ges_input['T1'].isnull()]
    df_r_ges_input = df_r_ges_input[['SERIAL', 'ARRIVEDEPART', 'PORTROUTE', 'UNSAMP_PORT_GRP_PV', 'SHIFT_WT',
                                     'NON_RESPONSE_WT', 'MINS_WT', 'OOHDesignWeight', 'T1']]
    df_r_ges_input['UNSAMP_PORT_GRP_PV'] = df_r_ges_input['UNSAMP_PORT_GRP_PV'].str.decode("utf-8")

    # Export dataframes to CSV
    df_r_ges_input.to_csv(
        r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\unsampled weight\df_r_ges_input.csv", index=False)

    return df_r_ges_input


# Prepare population totals to create AUX lookup variables
def r_population_input(survey_input, ustotals):
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates population data that feeds into the R GES weighting
    Parameters   : ustotals - A data frame containing the population data for
                   processing month
                   survey_input - A data frame containing the survey data for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Load data
    df_survey_input = survey_input
    df_pop_totals = ustotals


    # Export dataframes to CSV
    df_modpop_totals.to_csv(
        r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\unsampled weight\df_modpop_totals.csv")

    return df_modpop_totals
