'''
Created on 7 Feb 2018

@author: thorne1
'''

import pandas as pd
import numpy as np
import inspect

import survey_support
from IPSTransformation import CommonFunctions as cf

def do_ips_imbweight_calculation(surveyData, OutputData, SummaryData, var_serialNum
                               , var_shiftWeight, var_NRWeight, var_minWeight
                               , var_trafficWeight, var_OOHWeight
                               , var_imbalanceWeight, var_portroute, var_flow 
                               , var_direction, var_pgFactor, var_cgFactor 
                               , var_priorSum, var_postSum, var_eligible_flag):
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       :    
    Parameters    : CURRENTLY - surveyData = "SAS_SURVEY_SUBSAMPLE"
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
                  : HOWEVER likely to change
    Returns       : Output and Summary dataframes  
    """
    
    df_output_data = gldf_survey_data.copy() 
    
    # Change column names to upper case (just in case)
    df_output_data.columns = df_output_data.columns.str.upper()

    # Do some initial setup and selection
    df_output_data.drop(df_output_data[df_output_data[var_eligible_flag] != 1.0].index, inplace=True)
    df_output_data.loc[df_output_data[var_eligible_flag] == 1, var_imbalanceWeight] = 1.0
    
    # Create total traffic df
    df_total_traffic = df_output_data[[var_portroute, var_flow]].copy()
    df_total_traffic["TOT_NI_TRAFFIC"] = pd.Series(df_output_data[var_NRWeight] 
                                                   * df_output_data[var_minWeight]            
                                                   * df_output_data[var_trafficWeight]        
                                                   * df_output_data[var_OOHWeight])
    df_total_traffic = df_total_traffic.groupby([var_portroute, var_flow])\
            ["TOT_NI_TRAFFIC"].agg({\
            "TOT_NI_TRAFFIC" : 'prod'})
    df_total_traffic.reset_index(inplace=True)
    
    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    arrivedepart_condition = df_output_data[var_direction] == 2
    df_output_data.loc[(flow_condition) & (arrivedepart_condition)
                         , var_imbalanceWeight] = df_output_data[var_pgFactor]
    
    # Update output with provisional imbalance weight for overseas arrivals
#    print df_survey_data[["FLOW","ARRIVEDEPART","IMBAL_WT","IMBAL_PORT_FACT_PV"]]
    flow_condition = (df_output_data[var_flow] == 3) | (df_output_data[var_flow] == 7)
    arrivedepart_condition = df_output_data[var_direction] == 1
    df_output_data.loc[(flow_condition) & (arrivedepart_condition)
                         , var_imbalanceWeight] = df_output_data[var_pgFactor]
       
    # Update overseas departures with country imbalance
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    df_output_data.loc[(flow_condition)
                         , var_imbalanceWeight] = (df_output_data[var_imbalanceWeight]
                                           * df_output_data[var_cgFactor])
    
    # Calculate the pre and post sums for overseas residents
    df_prepost = df_output_data.copy()    
    prepost_flow_range = [1,3,5,7]
    df_prepost = df_prepost[df_prepost[var_flow].isin(prepost_flow_range)]
    df_prepost["PRE_IMB_WEIGHTS"] = pd.Series(df_prepost[var_shiftWeight]
                                                 * df_prepost[var_NRWeight]
                                                 * df_prepost[var_minWeight]
                                                 * df_prepost[var_trafficWeight]
                                                 * df_prepost[var_OOHWeight])
    df_prepost["POST_IMB_WEIGHTS"] = pd.Series(df_prepost[var_imbalanceWeight]
                                               * df_prepost[var_shiftWeight]
                                               * df_prepost[var_NRWeight]
                                               * df_prepost[var_minWeight]
                                               * df_prepost[var_trafficWeight]
                                               * df_prepost[var_OOHWeight])
    
    # Summarise - group by PORTROUTE and FLOW, and total the pre and post imbalanace weights
    df_overseas_residents = df_prepost.groupby([var_portroute, var_flow]).agg({\
            'PRE_IMB_WEIGHTS':'sum'
            ,'POST_IMB_WEIGHTS':'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
    
    # Calculate the difference between pre & post imbalance weighting for departures  
    # and calculate the ratio of the difference for departures at each port.  
    # Set first condition
    portroute_condition = df_total_traffic[var_portroute].isin(df_overseas_residents[var_portroute])
    
    # Set second condition
    total_traffic_flow_range = [2,4,6,8]
    overseas_residents_flow_range = [1,3,5,7]
    flow_condition = ((df_total_traffic[var_flow].isin(total_traffic_flow_range)) 
                            | (df_overseas_residents[var_flow].isin(overseas_residents_flow_range)))
    
    # Set third condition
    total_traffic_condition = (df_total_traffic["TOT_NI_TRAFFIC"] != 0)
    
    # Set one condition to rule them all...and in the darkness bind them
    all_conditions = ((portroute_condition)
                           & (flow_condition)
                           & (total_traffic_condition))
    
    # Select data, where conditions
    df_temp = df_overseas_residents.loc[all_conditions]
    df_calc_departures = df_temp.copy()

    # Set "DIFFERENCE" and "RATIO" column and values
    df_calc_departures["DIFFERENCE"] = pd.Series(df_temp["POST_IMB_WEIGHTS"] 
                                                   - df_temp["PRE_IMB_WEIGHTS"])
    df_calc_departures["RATIO"] = pd.Series(df_calc_departures["DIFFERENCE"] 
                                                   / df_total_traffic["TOT_NI_TRAFFIC"])
    
    # Delete unnecessary columns 
    del df_calc_departures["POST_IMB_WEIGHTS"]
    del df_calc_departures["PRE_IMB_WEIGHTS"]

    # Calculate the imbalance weight
    df_output_data.reset_index(drop=True,inplace=True)
    df_output_data["RATIO"] = np.nan
    flow_range = [2,4,6,8]
                       
    df_output_data.loc[((df_output_data.PORTROUTE.isin(df_calc_departures.PORTROUTE)) 
                        & (df_output_data[var_flow].isin(flow_range)))
                       , var_imbalanceWeight] = (1.0 - df_output_data["RATIO"])    
       
    # Append the imbalance weight to the input
    serial_range = list(df_output_data[var_serialNum])
    serial_condition = gldf_survey_data[var_serialNum].isin(serial_range)
    gldf_survey_data.loc[~serial_condition, var_imbalanceWeight] = 1

    # Create the summary output
    gldf_survey_data[var_priorSum] = pd.Series(gldf_survey_data[var_shiftWeight]
                                          * gldf_survey_data[var_NRWeight]
                                          * gldf_survey_data[var_minWeight]
                                          * gldf_survey_data[var_trafficWeight]
                                          * gldf_survey_data[var_OOHWeight])
    gldf_survey_data[var_postSum] = pd.Series(gldf_survey_data[var_imbalanceWeight]
                                           * gldf_survey_data[var_shiftWeight]
                                           * gldf_survey_data[var_NRWeight]
                                           * gldf_survey_data[var_minWeight]
                                           * gldf_survey_data[var_trafficWeight]
                                           * gldf_survey_data[var_OOHWeight])
    
    df_sliced = gldf_survey_data[gldf_survey_data[var_priorSum] > 0]
    df_summary_data = df_sliced.groupby([var_flow]).agg({\
            var_priorSum:'sum'
            , var_postSum:'sum'})
    df_summary_data = df_summary_data.reset_index()
    
    return (df_output_data, df_summary_data)
    
        
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
    
    global gldf_survey_data
    
#    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    logger = cf.database_logger()
    
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
    path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    # This method is untested with a range of data sets but is faster
    gldf_survey_data = pd.read_sas(path_to_survey_data)
    gldf_survey_data.columns = gldf_survey_data.columns.str.upper()
    
    print("Start - Calculate Imbalance Weight")
    df_imbalance_calculated = do_ips_imbweight_calculation(surveyData, OutputData
                                , SummaryData, var_serialNum, var_shiftWeight
                                , var_NRWeight, var_minWeight, var_trafficWeight
                                , var_OOHWeight, var_imbalanceWeight, var_portroute
                                , var_flow, var_direction, var_pgFactor, var_cgFactor 
                                , var_priorSum, var_postSum, var_eligible_flag)
    
    # Extract the two data sets returned from do_ips_imbweight_calculation
    df_survey_data = df_imbalance_calculated[0]
    df_summary_data = df_imbalance_calculated[1]
    
    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData, df_survey_data)
    cf.insert_into_table_many(SummaryData, df_summary_data)
    
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Imbalance Weight calculation: %s()" %function_name
    
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    logger().info("SUCCESS - Completed Imbalance Weight Calculation.")
    cf.commit_to_audit_log("Create", "ImbalanceWeight", audit_message)
    print("Completed - Calculate Imbalance Weight")


if __name__ == "__main__":
    calculate_imbalance_weight(surveyData = "SAS_SURVEY_SUBSAMPLE"
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
                               , var_eligible_flag = "IMBAL_ELIGIBLE_PV")