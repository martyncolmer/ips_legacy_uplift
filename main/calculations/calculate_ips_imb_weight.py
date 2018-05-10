'''
Created on 7 Feb 2018

@author: thorne1
'''
import inspect

import pandas as pd
import survey_support

from main.io import CommonFunctions as cf

OUTPUT_TABLE_NAME = "SAS_IMBALANCE_WT"
SUMMARY_TABLE_NAME = "SAS_PS_IMBALANCE"
PORTROUTE_COLUMN = "PORTROUTE"
FLOW_COLUMN = "FLOW"
DIRECTION_COLUMN = "ARRIVEDEPART"
PG_FACTOR_COLUMN = "IMBAL_PORT_FACT_PV"
CG_FACTOR_COLUMN = "IMBAL_CTRY_FACT_PV"
PRIOR_SUM_COLUMN = "SUM_PRIOR_WT"
POST_SUM_COLUMN = "SUM_IMBAL_WT"
ELIGIBLE_FLAG_COLUMN = "IMBAL_ELIGIBLE_PV"


def do_ips_imbweight_calculation(df_survey_data, var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight
                                 , var_trafficWeight, var_OOHWeight, var_imbalanceWeight):
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       : Calculates imbalance weight    
    Parameters    : CURRENTLY - df_survey_data = "SAS_SURVEY_SUBSAMPLE"
                               , var_serialNum = "SERIAL"
                               , var_shiftWeight = "SHIFT_WT"
                               , var_NRWeight = "NON_RESPONSE_WT"
                               , var_minWeight = "MINS_WT"
                               , var_trafficWeight = "TRAFFIC_WT"
                               , var_OOHWeight = "UNSAMP_TRAFFIC_WT"
                               , var_imbalanceWeight = "IMBAL_WT"
    Returns       : Output and Summary dataframes  
    """

    # Do some initial setup and selection
    df_output_data = df_survey_data.copy()
    df_survey_data.drop(df_output_data[df_output_data[ELIGIBLE_FLAG_COLUMN]
                                       == 1.0].index, inplace=True)
    df_output_data.drop(df_output_data[df_output_data[ELIGIBLE_FLAG_COLUMN]
                                       != 1.0].index, inplace=True)
    df_output_data.loc[df_output_data[ELIGIBLE_FLAG_COLUMN] == 1.0,
                       var_imbalanceWeight] = 1.0

    # Create total traffic dataframe
    df_total_traffic = df_output_data[[ELIGIBLE_FLAG_COLUMN,
                                       PORTROUTE_COLUMN,
                                       FLOW_COLUMN]].copy()
    df_total_traffic.sort_values(by=[PORTROUTE_COLUMN, FLOW_COLUMN])
    df_total_traffic["TOT_NI_TRAFFIC"] = (df_output_data[var_shiftWeight]
                                          * df_output_data[var_NRWeight]
                                          * df_output_data[var_minWeight]
                                          * df_output_data[var_trafficWeight]
                                          * df_output_data[var_OOHWeight])
    df_total_traffic = df_total_traffic.groupby([PORTROUTE_COLUMN, FLOW_COLUMN]) \
        .agg({"TOT_NI_TRAFFIC": 'sum'})
    df_total_traffic.reset_index(inplace=True)

    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_output_data[FLOW_COLUMN] == 1) | (df_output_data[FLOW_COLUMN] == 5)
    arrivedepart_condition = df_output_data[DIRECTION_COLUMN] == 2
    df_output_data.loc[flow_condition & arrivedepart_condition,
                       var_imbalanceWeight] = df_output_data[PG_FACTOR_COLUMN]

    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_output_data[FLOW_COLUMN] == 3) | (df_output_data[FLOW_COLUMN] == 7)
    arrivedepart_condition = df_output_data[DIRECTION_COLUMN] == 1
    df_output_data.loc[flow_condition & arrivedepart_condition,
                       var_imbalanceWeight] = df_output_data[PG_FACTOR_COLUMN]

    # Update overseas departures with country imbalance
    flow_condition = (df_output_data[FLOW_COLUMN] == 1) | (df_output_data[FLOW_COLUMN] == 5)
    df_output_data.loc[flow_condition, var_imbalanceWeight] = (df_output_data[var_imbalanceWeight]
                                                               * df_output_data[CG_FACTOR_COLUMN])

    # Calculate the pre and post sums for overseas residents
    df_prepost = df_output_data.copy()
    prepost_flow_range = [1, 3, 5, 7]
    df_prepost = df_prepost[df_prepost[FLOW_COLUMN].isin(prepost_flow_range)]
    df_prepost["PRE_IMB_WEIGHTS"] = (df_prepost[var_shiftWeight]
                                     * df_prepost[var_NRWeight]
                                     * df_prepost[var_minWeight]
                                     * df_prepost[var_trafficWeight]
                                     * df_prepost[var_OOHWeight])
    df_prepost["POST_IMB_WEIGHTS"] = (df_prepost[var_imbalanceWeight]
                                      * df_prepost[var_shiftWeight]
                                      * df_prepost[var_NRWeight]
                                      * df_prepost[var_minWeight]
                                      * df_prepost[var_trafficWeight]
                                      * df_prepost[var_OOHWeight])

    # Summarise. Group by PORTROUTE & FLOW, & total the pre & post imbalanace weights
    df_prepost.sort_values(by=[PORTROUTE_COLUMN, FLOW_COLUMN])
    df_overseas_residents = df_prepost.groupby([PORTROUTE_COLUMN, FLOW_COLUMN]).agg({
        'PRE_IMB_WEIGHTS': 'sum', 'POST_IMB_WEIGHTS': 'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
    df_overseas_residents = df_overseas_residents[[PORTROUTE_COLUMN,
                                                   FLOW_COLUMN,
                                                   "PRE_IMB_WEIGHTS",
                                                   "POST_IMB_WEIGHTS"]]

    # Calculate the difference between pre & post imbalance weighting for departures  
    # & calculate the ratio of the difference for departures at each port.
    df_calc_departures = df_overseas_residents.copy()
    df_calc_departures[FLOW_COLUMN + 'Extra'] = df_calc_departures[FLOW_COLUMN] + 1
    df_calc_departures = df_calc_departures.merge(df_total_traffic,
                                                  left_on=[PORTROUTE_COLUMN, FLOW_COLUMN + 'Extra'],
                                                  right_on=[PORTROUTE_COLUMN, FLOW_COLUMN])
    df_calc_departures["DIFFERENCE"] = (df_calc_departures["POST_IMB_WEIGHTS"]
                                        - df_calc_departures["PRE_IMB_WEIGHTS"])
    df_calc_departures["RATIO"] = (df_calc_departures["DIFFERENCE"]
                                   / df_calc_departures["TOT_NI_TRAFFIC"])

    # Cleanse
    df_calc_departures.drop(["PRE_IMB_WEIGHTS", "POST_IMB_WEIGHTS", FLOW_COLUMN + "_y",
                             "TOT_NI_TRAFFIC", FLOW_COLUMN + 'Extra'],
                            axis=1, inplace=True)
    df_calc_departures.rename(columns={FLOW_COLUMN + "_x": FLOW_COLUMN}, inplace=True)

    # Calculate the imbalance weight
    new_val = df_output_data[[var_serialNum, PORTROUTE_COLUMN, FLOW_COLUMN]].copy()
    new_val[FLOW_COLUMN + 'Extra'] = new_val[FLOW_COLUMN] - 1
    new_val = new_val.merge(df_calc_departures,
                            left_on=[PORTROUTE_COLUMN, FLOW_COLUMN + 'Extra'],
                            right_on=[PORTROUTE_COLUMN, FLOW_COLUMN])

    # Append Ratio to df and cleanse
    df_output_data = df_output_data.merge(new_val, left_on=var_serialNum,
                                          right_on=var_serialNum, how='left')
    df_output_data.loc[df_output_data["RATIO"].notnull(),
                       var_imbalanceWeight] = (1.0 - df_output_data["RATIO"])
    df_output_data.rename(columns={PORTROUTE_COLUMN + "_x": PORTROUTE_COLUMN},
                          inplace=True)
    df_output_data.drop([PORTROUTE_COLUMN + "_y"], axis=1, inplace=True)

    # Append the imbalance weight to the input and cleanse
    df_survey_data_concat = pd.concat([df_survey_data, df_output_data], ignore_index=True)
    df_survey_data = df_survey_data_concat.reindex(df_survey_data.columns, axis=1)
    df_survey_data.loc[df_survey_data[var_imbalanceWeight].isnull(), var_imbalanceWeight] = 1

    # Create the summary output
    df_survey_data[PRIOR_SUM_COLUMN] = pd.Series(df_survey_data[var_shiftWeight]
                                                 * df_survey_data[var_NRWeight]
                                                 * df_survey_data[var_minWeight]
                                                 * df_survey_data[var_trafficWeight]
                                                 * df_survey_data[var_OOHWeight])
    df_survey_data[POST_SUM_COLUMN] = pd.Series(df_survey_data[var_imbalanceWeight]
                                                * df_survey_data[var_shiftWeight]
                                                * df_survey_data[var_NRWeight]
                                                * df_survey_data[var_minWeight]
                                                * df_survey_data[var_trafficWeight]
                                                * df_survey_data[var_OOHWeight])

    df_sliced = df_survey_data[df_survey_data[POST_SUM_COLUMN] > 0]
    df_sliced[var_imbalanceWeight] = df_sliced[var_imbalanceWeight].astype('float').round(decimals=3)
    df_summary_data = df_sliced.groupby([FLOW_COLUMN]).agg({
        PRIOR_SUM_COLUMN: 'sum', POST_SUM_COLUMN: 'sum'})
    df_summary_data = df_summary_data.reset_index()

    # Cleanse dataframes before returning
    df_survey_data.sort_values(by=[var_serialNum], inplace=True)
    df_survey_data.drop([POST_SUM_COLUMN, PRIOR_SUM_COLUMN], axis=1, inplace=True)

    return df_survey_data, df_summary_data


def calculate(SurveyData, var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight
              , var_trafficWeight, var_OOHWeight, var_imbalanceWeight):
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       : Function called to setup and initiate the calculation  
    Parameters    : Performs the setup required for the calculation function, then
                    calls the function
    Returns       : N/A  
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    logger = cf.database_logger()

    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    # Import data via SAS
    # This method works for all data sets but is slower
    # df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    # This method is untested with a range of data sets but is faster
    df_survey_data = pd.read_sas(path_to_survey_data)
    df_survey_data.columns = df_survey_data.columns.str.upper()

    df_imbalance_calculated = do_ips_imbweight_calculation(df_survey_data, var_serialNum, var_shiftWeight,
                                                           var_NRWeight, var_minWeight, var_trafficWeight,
                                                           var_OOHWeight, var_imbalanceWeight)

    # Extract the two data sets returned from do_ips_imbweight_calculation
    df_survey_data = df_imbalance_calculated[0]
    df_summary_data = df_imbalance_calculated[1]

    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, df_survey_data[[var_serialNum, var_imbalanceWeight]])
    cf.insert_dataframe_into_table(SUMMARY_TABLE_NAME, df_summary_data)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Imbalance Weight calculation: %s()" % function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    logger.info("SUCCESS - Completed Imbalance Weight Calculation.")
    cf.commit_to_audit_log("Create", "ImbalanceWeight", audit_message)
