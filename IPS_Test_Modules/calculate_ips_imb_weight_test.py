'''
Created on 7 Feb 2018

@author: thorne1
'''
import pandas as pd
import inspect
import sys

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
    Purpose       : Calculates imbalance weight    
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
    Returns       : Output and Summary dataframes  
    """
    global df_survey_data
    
    # Do some initial setup and selection
    df_survey_data.loc[df_survey_data[var_eligible_flag] == 1.0
                       , var_imbalanceWeight] = 1.0

    # Create total traffic dataframe
    df_total_traffic = df_survey_data[[var_eligible_flag
                                       , var_portroute
                                       , var_flow]].copy()
    df_total_traffic.drop(df_total_traffic[df_total_traffic[var_eligible_flag] != 1.0].index, inplace=True)
    df_total_traffic.drop([var_eligible_flag], axis=1)
    df_total_traffic.sort_values(by = [var_portroute, var_flow])
    df_total_traffic["TOT_NI_TRAFFIC"] = pd.Series(df_survey_data[var_shiftWeight]
                                                   * df_survey_data[var_NRWeight] 
                                                   * df_survey_data[var_minWeight]            
                                                   * df_survey_data[var_trafficWeight]        
                                                   * df_survey_data[var_OOHWeight])
    df_total_traffic = df_total_traffic.groupby([var_portroute
                                                 , var_flow])["TOT_NI_TRAFFIC"] \
                                                 .agg({"TOT_NI_TRAFFIC" : 'sum'})
    df_total_traffic.reset_index(inplace=True)
    
    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_survey_data[var_flow] == 1) | (df_survey_data[var_flow] == 5)
    arrivedepart_condition = df_survey_data[var_direction] == 2
    eligible_condition = df_survey_data[var_eligible_flag] == 1
    df_survey_data.loc[(flow_condition) 
                       & (arrivedepart_condition) 
                       & (eligible_condition)
                       , var_imbalanceWeight] = df_survey_data[var_pgFactor]
    
    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_survey_data[var_flow] == 3) | (df_survey_data[var_flow] == 7)
    arrivedepart_condition = df_survey_data[var_direction] == 1
    eligible_condition = df_survey_data[var_eligible_flag] == 1
    df_survey_data.loc[(flow_condition) 
                       & (arrivedepart_condition) 
                       & (eligible_condition)
                       , var_imbalanceWeight] = df_survey_data[var_pgFactor]
    
    # Update overseas departures with country imbalance
    flow_condition = (df_survey_data[var_flow] == 1) | (df_survey_data[var_flow] == 5)
    eligible_condition = df_survey_data[var_eligible_flag] == 1
    df_survey_data.loc[(flow_condition) 
                       & (eligible_condition) 
                       , var_imbalanceWeight] = (df_survey_data[var_imbalanceWeight]
                                                 * df_survey_data[var_cgFactor])
    
    # Calculate the pre and post sums for overseas residents
    df_prepost = df_survey_data.copy()
    df_prepost.drop(df_prepost[df_prepost[var_eligible_flag] != 1.0].index
                    , inplace=True)    
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
    
    # Summarise. Group by PORTROUTE & FLOW, & total the pre & post imbalanace weights
    df_prepost.sort_values(by=[var_portroute, var_flow])
    df_overseas_residents = df_prepost.groupby([var_portroute, var_flow]).agg({\
            'PRE_IMB_WEIGHTS':'sum'
            ,'POST_IMB_WEIGHTS':'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
    
    # Calculate the difference between pre & post imbalance weighting for departures  
    # & calculate the calculate_departures_ratio of the difference for departures at each port.  
    global df_calc_departures
    df_calc_departures = pd.DataFrame()
    
    
    def calculate_departures_ratio(row):
        """
        Author        : thorne1
        Date          : 3 Mar 2018
        Purpose       : Calculate the calculate_departures_ratio of the difference for 
                      : departures at each port   
        """
        global df_calc_departures
        
        # Conditionally retrieves rows
        temp = df_total_traffic[(df_total_traffic[var_portroute]==row[var_portroute])
                               &(df_total_traffic[var_flow]==row[var_flow]+1)
                               &(df_total_traffic['TOT_NI_TRAFFIC'] != 0)]
        
        # Creates series of required values and inserts to dataframe
        if not(temp.empty):
            rec = pd.DataFrame([[row[var_portroute]
                                 , row[var_flow]
                                 , row['POST_IMB_WEIGHTS'] - row['PRE_IMB_WEIGHTS']
                                 , ((row['POST_IMB_WEIGHTS'] 
                                     - row['PRE_IMB_WEIGHTS'])
                                     /temp['TOT_NI_TRAFFIC'].values[0])]])
            if(df_calc_departures.empty):
                df_calc_departures = rec
            else:
                df_calc_departures = pd.concat([df_calc_departures, rec]
                                               , axis=0)
                
        return row
    
    
    # Apply function to each row
    df_overseas_residents.apply(calculate_departures_ratio,axis = 1)
    
    # Cleanse dataframe
    df_calc_departures.columns = [var_portroute
                                  , var_flow
                                  , "DIFFERENCE"
                                  , "RATIO"]
    df_calc_departures.index = range(0,len(df_calc_departures))
    df_calc_departures.drop(1, inplace=True)
    df_calc_departures.reset_index()
    
    # Calculate the imbalance weight
    global new_val
    new_val = pd.DataFrame()
    
    
    def get_ratio(row):
        """
        Author        : thorne1
        Date          : 3 Mar 2018
        Purpose       : Calculates the imbalanace weight 
        """
        global new_val
        
        # Conditionally retrieves rows
        temp = df_calc_departures[(df_calc_departures[var_portroute]==row[var_portroute])
                               &(df_calc_departures[var_flow]==row[var_flow]-1)]
        # Creates series of required values and inserts to dataframe
        if not(temp.empty):
            rec = pd.DataFrame([[row[var_serialNum]
                                 , temp['RATIO'].values[0]]])
            if(new_val.empty):
                new_val = rec
            else:
                new_val = pd.concat([new_val, rec], axis=0)
        
        return row


    df_survey_data.reset_index(inplace = True)
    df_survey_data.apply(get_ratio, axis = 1)

    # Cleanse dataframe
    new_val.columns = ["TEMP_SERIAL","TEMP_RATIO"]
    new_val.index = range(0,len(new_val))
    new_val.reset_index()
    
    # Append the imbalance weight to the input
    df_survey_data = df_survey_data.merge(new_val
                                          , left_on=var_serialNum
                                          , right_on='TEMP_SERIAL'
                                          , how='outer')
    df_survey_data.loc[df_survey_data["TEMP_RATIO"].notnull()
                       , var_imbalanceWeight] = (1.0 - df_survey_data["TEMP_RATIO"])
    df_survey_data.drop('TEMP_SERIAL', axis=1, inplace=True)
    df_survey_data.drop("TEMP_RATIO", axis=1, inplace=True)
    df_survey_data.loc[(df_survey_data[var_eligible_flag] != 1.0)
                       , var_imbalanceWeight] = 1
    
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
    df_summary_data = df_sliced.groupby([var_flow]).agg({\
            var_priorSum:'sum'
            , var_postSum:'sum'})
    df_summary_data = df_summary_data.reset_index()
    
    # Cleanse dataframes before returning
    df_survey_data.drop('index', axis=1, inplace=True)
    df_survey_data.drop('SUM_IMBAL_WT', axis=1, inplace=True)
    df_survey_data.drop('SUM_PRIOR_WT', axis=1, inplace=True)

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
    
    global df_survey_data
    df_survey_data = surveyData
    
    # Call JSON configuration file for error logger setup
#    survey_support.setup_logging('IPS_logging_config_debug.json')
#    logger = cf.database_logger()
    
    # Setup path to the base directory containing data files
#    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
#    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    # This method is untested with a range of data sets but is faster
#    df_survey_data = pd.read_sas(path_to_survey_data)
    df_survey_data.columns = df_survey_data.columns.str.upper()
    
    print("Start - Calculate Imbalance Weight")
    df_imbalance_calculated = do_ips_imbweight_calculation(df_survey_data, OutputData
                                , SummaryData, var_serialNum, var_shiftWeight
                                , var_NRWeight, var_minWeight, var_trafficWeight
                                , var_OOHWeight, var_imbalanceWeight, var_portroute
                                , var_flow, var_direction, var_pgFactor, var_cgFactor 
                                , var_priorSum, var_postSum, var_eligible_flag)
    
    # Extract the two data sets returned from do_ips_imbweight_calculation
    df_survey_data = df_imbalance_calculated[0]
    df_summary_data = df_imbalance_calculated[1]
    
#    # Append the generated data to output tables
#    cf.insert_into_table_many(OutputData
#                              , df_survey_data[[var_serialNum, var_imbalanceWeight]])
#    cf.insert_into_table_many(SummaryData, df_summary_data)
    
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
#    function_name = str(inspect.stack()[0][3])
#    audit_message = "Imbalance Weight calculation: %s()" %function_name
#    
#    # Log success message in SAS_RESPONSE and AUDIT_LOG
#    logger.info("SUCCESS - Completed Imbalance Weight Calculation.")
#    cf.commit_to_audit_log("Create", "ImbalanceWeight", audit_message)
    print("Completed - Calculate Imbalance Weight")
    
    return (df_survey_data, df_summary_data)


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