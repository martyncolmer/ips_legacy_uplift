'''
Created on 7 Feb 2018

@author: thorne1
'''
import pandas as pd
import inspect
import sys

import survey_support
from main.io import CommonFunctions as cf
from pprint import pprint

OUTPUT_TABLE_NAME = "SAS_IMBALANCE_WT"
SUMMARY_TABLE_NAME = "SAS_PS_IMBALANCE"


def do_ips_imbweight_calculation(df_survey_data, var_serialNum
                                 , var_shiftWeight, var_NRWeight, var_minWeight
                                 , var_trafficWeight, var_OOHWeight
                                 , var_imbalanceWeight, var_portroute, var_flow
                                 , var_direction, var_pgFactor, var_cgFactor
                                 , var_priorSum, var_postSum, var_eligible_flag):
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       : Calculates imbalance weight
    Parameters    : CURRENTLY - df_survey_data = "SAS_SURVEY_SUBSAMPLE"
                               , OutputData = "SAS_IMBALANCE_WT"
                               , SummaryData = "SAS_PS_IMBALANCE"
                               , var_serialNum = "SERIAL"
                               , var_shiftWeight = "SHIFT_WT"
                               , var_NRWeight = "NON_RESPONSE_WT"
                               , var_minWeight = "MINS_WT"
                               , var_trafficWeight = "TRAFFIC_WT"
                               , var_OOHWeight = "UNSAMP_TRAFFIC_WT"
                               , var_imbalanceWeight = "IMBAL_WT"
                               , var_portroute = "PORTROUTE"
                               , var_flow = "FLOW"
                               , var_direction = "ARRIVEDEPART"
                               , var_pgFactor = "IMBAL_PORT_FACT_PV"
                               , var_cgFactor = "IMBAL_CTRY_FACT_PV"
                               , var_priorSum = "SUM_PRIOR_WT"
                               , var_postSum = "SUM_IMBAL_WT"
                               , var_eligible_flag = "IMBAL_ELIGIBLE_PV"
    Returns       : Output and Summary dataframes
    """

    # Do some initial setup and selection
    df_output_data = df_survey_data.copy()
    df_survey_data.drop(df_output_data[df_output_data[var_eligible_flag]
                                       == 1.0].index, inplace=True)
    df_output_data.drop(df_output_data[df_output_data[var_eligible_flag]
                                       != 1.0].index, inplace=True)
    df_output_data.loc[df_output_data[var_eligible_flag] == 1.0
    , var_imbalanceWeight] = 1.0

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("in_1", "in_1.sas7bdat", df_output_data, ["SERIAL", var_eligible_flag, var_imbalanceWeight])
    # ===========================================================================
    # ===========================================================================

    # Create total traffic dataframe
    df_total_traffic = df_output_data[[var_eligible_flag
        , var_portroute
        , var_flow]].copy()
    df_total_traffic.sort_values(by=[var_portroute, var_flow])
    df_total_traffic["TOT_NI_TRAFFIC"] = (df_output_data[var_shiftWeight]
                                          * df_output_data[var_NRWeight]
                                          * df_output_data[var_minWeight]
                                          * df_output_data[var_trafficWeight]
                                          * df_output_data[var_OOHWeight])
    df_total_traffic = df_total_traffic.groupby([var_portroute
                                                    , var_flow])["TOT_NI_TRAFFIC"] \
        .agg({"TOT_NI_TRAFFIC": 'sum'})
    df_total_traffic.reset_index(inplace=True)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("temp1", "temp1.sas7bdat", df_total_traffic)
    # ===========================================================================
    # ===========================================================================

    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    arrivedepart_condition = df_output_data[var_direction] == 2
    df_output_data.loc[(flow_condition)
                       & (arrivedepart_condition)
    , var_imbalanceWeight] = df_output_data[var_pgFactor]

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("in_update1", "in_update1.sas7bdat", df_output_data,
                   col_list=["SERIAL", var_flow, var_direction, var_eligible_flag, var_imbalanceWeight, var_pgFactor])
    # ===========================================================================
    # ===========================================================================

    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_output_data[var_flow] == 3) | (df_output_data[var_flow] == 7)
    arrivedepart_condition = df_output_data[var_direction] == 1
    df_output_data.loc[(flow_condition)
                       & (arrivedepart_condition)
    , var_imbalanceWeight] = df_output_data[var_pgFactor]

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("in_update2", "in_update2.sas7bdat", df_output_data,
                   col_list=["SERIAL", var_flow, var_direction, var_eligible_flag, var_imbalanceWeight, var_pgFactor])
    # ===========================================================================
    # ===========================================================================

    # Update overseas departures with country imbalance
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    df_output_data.loc[(flow_condition)
    , var_imbalanceWeight] = (df_output_data[var_imbalanceWeight]
                              * df_output_data[var_cgFactor])

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("in_update3", "in_update3.sas7bdat", df_output_data,
                   col_list=["SERIAL", var_flow, var_direction, var_eligible_flag, var_imbalanceWeight, var_pgFactor])
    # ===========================================================================
    # ===========================================================================

    # Calculate the pre and post sums for overseas residents
    df_prepost = df_output_data.copy()
    prepost_flow_range = [1, 3, 5, 7]
    df_prepost = df_prepost[df_prepost[var_flow].isin(prepost_flow_range)]
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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("prepost", "prepost.sas7bdat", df_prepost,
                   col_list=["SERIAL", "AGE", "APORTLATDEG", "APORTLATMIN", "SHIFT_WT", var_imbalanceWeight])
    #    cf.beep()
    #    sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Summarise. Group by PORTROUTE & FLOW, & total the pre & post imbalanace weights
    df_prepost.sort_values(by=[var_portroute, var_flow])
    df_overseas_residents = df_prepost.groupby([var_portroute, var_flow]).agg({ \
        'PRE_IMB_WEIGHTS': 'sum'
        , 'POST_IMB_WEIGHTS': 'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
    df_overseas_residents = df_overseas_residents[[var_portroute
        , var_flow
        , "PRE_IMB_WEIGHTS"
        , "POST_IMB_WEIGHTS"]]

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("temp2", "temp2.sas7bdat", df_overseas_residents)
    #    cf.beep()
    #    sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Calculate the difference between pre & post imbalance weighting for departures
    # & calculate the calculate_departures_ratio of the difference for departures at each port.
    df_calc_departures = df_overseas_residents.copy()
    df_calc_departures[var_flow + 'Extra'] = df_calc_departures[var_flow] + 1
    df_calc_departures = df_calc_departures.merge(df_total_traffic,
                                                  left_on=[var_portroute
                                                      , var_flow + 'Extra']
                                                  , right_on=[var_portroute
            , var_flow])

    # Caluclate
    df_calc_departures["DIFFERENCE"] = (df_calc_departures["POST_IMB_WEIGHTS"]
                                        - df_calc_departures["PRE_IMB_WEIGHTS"])
    df_calc_departures["RATIO"] = (df_calc_departures["DIFFERENCE"]
                                   / df_calc_departures["TOT_NI_TRAFFIC"])

    # Cleanse
    df_calc_departures.drop(["PRE_IMB_WEIGHTS"
                                , "POST_IMB_WEIGHTS"
                                , "FLOW_y"
                                , "TOT_NI_TRAFFIC"
                                , var_flow + 'Extra']
                            , axis=1, inplace=True)
    df_calc_departures.rename(columns={"FLOW_x": "FLOW"}, inplace=True)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("temp3", "temp3.sas7bdat", df_calc_departures)
    #    cf.beep()
    #    sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Find Ratio
    new_val = df_output_data[[var_serialNum, var_portroute, var_flow]].copy()
    new_val[var_flow + 'Extra'] = new_val[var_flow] - 1
    new_val = new_val.merge(df_calc_departures
                            , left_on=[var_portroute, var_flow + 'Extra']
                            , right_on=[var_portroute, var_flow])

    # Append Ratio to df
    df_output_data = df_output_data.merge(new_val
                                          , left_on=var_serialNum
                                          , right_on='SERIAL'
                                          , how='left')
    df_output_data.loc[df_output_data["RATIO"].notnull()
    , var_imbalanceWeight] = (1.0 - df_output_data["RATIO"])

    # Append the imbalance weight to the input and cleanse
    df_survey_data_concat = pd.concat([df_survey_data, df_output_data]
                                      , ignore_index=True)
    df_survey_data = df_survey_data_concat.reindex_axis(df_survey_data.columns, axis=1)
    df_survey_data.loc[df_survey_data[var_imbalanceWeight].isnull()
    , var_imbalanceWeight] = 1

    #    #===========================================================================
    #    #===========================================================================
    from sas7bdat import SAS7BDAT
    sas = SAS7BDAT(
        r"S:\CASPA\IPS\Testing\Oct Data\Imbalance Weight\surveydata_merge_in.sas7bdat").to_data_frame()
    sas.columns = sas.columns.str.upper()
    sas.to_csv(r"S:\CASPA\IPS\Testing\Integration\Oct\surveydata_merge_in_SAS.csv")
    df_survey_data.to_csv(
        r"S:\CASPA\IPS\Testing\Integration\Oct\surveydata_merge_in_PY.csv")
    #    cf.beep()
    #    sys.exit()
    #    #===========================================================================
    #    #===========================================================================

    # Create the summary output
    df_survey_data[var_priorSum] = pd.Series(df_survey_data[var_shiftWeight]
                                             * df_survey_data[var_NRWeight]
                                             * df_survey_data[var_minWeight]
                                             * df_survey_data[var_trafficWeight]
                                             * df_survey_data[var_OOHWeight])
    df_survey_data[var_postSum] = pd.Series(df_survey_data[var_imbalanceWeight]
                                            * df_survey_data[var_shiftWeight]
                                            * df_survey_data[var_NRWeight]
                                            * df_survey_data[var_minWeight]
                                            * df_survey_data[var_trafficWeight]
                                            * df_survey_data[var_OOHWeight])

    df_sliced = df_survey_data[df_survey_data[var_postSum] > 0]
    df_sliced[var_imbalanceWeight] = df_sliced[var_imbalanceWeight].apply(lambda x: round(x, 3))
    df_summary_data = df_sliced.groupby([var_flow]).agg({ \
        var_priorSum: 'sum'
        , var_postSum: 'sum'})
    df_summary_data = df_summary_data.reset_index()

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("summary_final", "summary_final.sas7bdat", df_summary_data)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    return (df_survey_data, df_summary_data)


def calculate_imbalance_weight(surveyData, OutputData, SummaryData, var_serialNum
                               , var_shiftWeight, var_NRWeight, var_minWeight
                               , var_trafficWeight, var_OOHWeight
                               , var_imbalanceWeight, var_portroute, var_flow
                               , var_direction, var_pgFactor, var_cgFactor
                               , var_priorSum, var_postSum, var_eligible_flag):
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

    df_imbalance_calculated = do_ips_imbweight_calculation(df_survey_data, OutputData
                                                           , SummaryData, var_serialNum, var_shiftWeight
                                                           , var_NRWeight, var_minWeight, var_trafficWeight
                                                           , var_OOHWeight, var_imbalanceWeight, var_portroute
                                                           , var_flow, var_direction, var_pgFactor, var_cgFactor
                                                           , var_priorSum, var_postSum, var_eligible_flag)

    # Extract the two data sets returned from do_ips_imbweight_calculation
    df_survey_data = df_imbalance_calculated[0]
    df_summary_data = df_imbalance_calculated[1]

    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData
                              , df_survey_data[[var_serialNum, var_imbalanceWeight]])
    cf.insert_into_table_many(SummaryData, df_summary_data)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Imbalance Weight calculation: %s()" % function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    logger.info("SUCCESS - Completed Imbalance Weight Calculation.")
    cf.commit_to_audit_log("Create", "ImbalanceWeight", audit_message)


if __name__ == "__main__":
    df_survey_data = pd.read_csv(r'S:\CASPA\IPS\Testing\Oct Data\Imbalance Weight\surveydata.csv')
    var_serialNum = 'SERIAL'
    var_shiftWeight = 'SHIFT_WT'
    var_NRWeight = 'NON_RESPONSE_WT'
    var_minWeight = 'MINS_WT'
    var_trafficWeight = 'TRAFFIC_WT'
    var_OOHWeight = 'UNSAMP_TRAFFIC_WT'
    var_imbalanceWeight = 'IMBAL_WT'

    var_portroute = "PORTROUTE"
    var_flow = "FLOW"
    var_direction = "ARRIVEDEPART"
    var_pgFactor = "IMBAL_PORT_FACT_PV"
    var_cgFactor = "IMBAL_CTRY_FACT_PV"
    var_priorSum = "SUM_PRIOR_WT"
    var_postSum = "SUM_IMBAL_WT"
    var_eligible_flag = "IMBAL_ELIGIBLE_PV"

    do_ips_imbweight_calculation(df_survey_data, var_serialNum
                                 , var_shiftWeight, var_NRWeight, var_minWeight
                                 , var_trafficWeight, var_OOHWeight
                                 , var_imbalanceWeight, var_portroute, var_flow
                                 , var_direction, var_pgFactor, var_cgFactor
                                 , var_priorSum, var_postSum, var_eligible_flag)
