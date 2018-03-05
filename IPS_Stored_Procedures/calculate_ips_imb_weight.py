'''
Created on 7 Feb 2018

@author: thorne1
'''

import pandas as pd
import numpy as np
import inspect
import sys

import survey_support
from IPSTransformation import CommonFunctions as cf

import winsound         # for sound  
import time             # for sleep

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
    Returns       : Output and Summary dataframes  
    """
    global gldf_survey_data
    df_output_data = gldf_survey_data.copy() 
    
    # Test SurveyData
#    compare_dfs("surveydata", "surveydata.sas7bdat", df_output_data)
#    sys.exit()
    
    # Do some initial setup and selection
#    df_output_data.drop(df_output_data[df_output_data[var_eligible_flag] != 1].index, inplace=True)
    df_output_data.loc[df_output_data[var_eligible_flag] == 1.0, var_imbalanceWeight] = 1.0

    # Compare with in_1.sas7bdat
#    compare_dfs("in_1", "in_1.sas7bdat", df_output_data, ["SERIAL", var_eligible_flag, var_imbalanceWeight])
    
    # Create total traffic df
    df_total_traffic = df_output_data[[var_eligible_flag, var_portroute, var_flow]].copy()
    df_total_traffic.drop(df_total_traffic[df_total_traffic[var_eligible_flag] != 1.0].index, inplace=True)
    df_total_traffic.drop([var_eligible_flag], axis=1)
    df_total_traffic.sort_values(by = [var_portroute, var_flow])
    df_total_traffic["TOT_NI_TRAFFIC"] = pd.Series(df_output_data[var_shiftWeight]
                                                   * df_output_data[var_NRWeight] 
                                                   * df_output_data[var_minWeight]            
                                                   * df_output_data[var_trafficWeight]        
                                                   * df_output_data[var_OOHWeight])
    df_total_traffic = df_total_traffic.groupby([var_portroute
                                                 , var_flow])["TOT_NI_TRAFFIC"] \
                                                 .agg({"TOT_NI_TRAFFIC" : 'sum'})
    df_total_traffic.reset_index(inplace=True)
    
#    compare_dfs("temp1", "temp1.sas7bdat", df_total_traffic)
#    beep()
#    sys.exit()
    
    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    arrivedepart_condition = df_output_data[var_direction] == 2
    eligible_condition = df_output_data[var_eligible_flag] == 1
    df_output_data.loc[(flow_condition) 
                       & (arrivedepart_condition) 
                       & (eligible_condition)
                       , var_imbalanceWeight] = df_output_data[var_pgFactor]
    
#    compare_dfs("in_update1", "in_update1.sas7bdat", df_output_data, col_list =["SERIAL", var_flow, var_direction, var_eligible_flag, var_imbalanceWeight, var_pgFactor])
#    beep()
#    sys.exit()
                         
    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_output_data[var_flow] == 3) | (df_output_data[var_flow] == 7)
    arrivedepart_condition = df_output_data[var_direction] == 1
    eligible_condition = df_output_data[var_eligible_flag] == 1
    df_output_data.loc[(flow_condition) 
                       & (arrivedepart_condition) 
                       & (eligible_condition)
                       , var_imbalanceWeight] = df_output_data[var_pgFactor]
    
#    compare_dfs("in_update2", "in_update2.sas7bdat", df_output_data, col_list =["SERIAL", var_flow, var_direction, var_eligible_flag, var_imbalanceWeight, var_pgFactor])
#    sys.exit()
    
    # Update overseas departures with country imbalance
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    eligible_condition = df_output_data[var_eligible_flag] == 1
    df_output_data.loc[(flow_condition) 
                       & (eligible_condition) 
                       , var_imbalanceWeight] = (df_output_data[var_imbalanceWeight]
                                                 * df_output_data[var_cgFactor])
    
#    compare_dfs("in_update3", "in_update3.sas7bdat", df_output_data, col_list=["SERIAL", var_flow, var_direction, var_eligible_flag, var_imbalanceWeight, var_pgFactor])
#    sys.exit()
                    
    # Calculate the pre and post sums for overseas residents
    df_prepost = df_output_data.copy()
    df_prepost.drop(df_prepost[df_prepost[var_eligible_flag] != 1.0].index, inplace=True)    
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

#    compare_dfs("prepost", "prepost.sas7bdat", df_prepost, col_list=["SERIAL", "AGE", "APORTLATDEG", "APORTLATMIN", "SHIFT_WT", var_imbalanceWeight])
#    sys.exit()
    
    # Summarise - group by PORTROUTE and FLOW, and total the pre and post imbalanace weights
    df_prepost.sort_values(by=[var_portroute, var_flow])
    df_overseas_residents = df_prepost.groupby([var_portroute, var_flow]).agg({\
            'PRE_IMB_WEIGHTS':'sum'
            ,'POST_IMB_WEIGHTS':'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
    
#    compare_dfs("temp2", "temp2.sas7bdat", df_overseas_residents)
#    sys.exit()
    
    # Calculate the difference between pre & post imbalance weighting for departures  
    # and calculate the ratio of the difference for departures at each port.  
    global df_calc_departures
    df_calc_departures = pd.DataFrame()
    
    
    def ratio(row):
        """
        Author        : thorne1
        Date          : 3 Mar 2018
        Purpose       : Conditionally retrieves value from one dataframe and 
                      : inserts to other dataframe   
        """
        global df_calc_departures
        
        # Conditionally retrieves rows
        temp = df_total_traffic[(df_total_traffic[var_portroute]==row[var_portroute])
                               &(df_total_traffic[var_flow]==row[var_flow]+1)
                               &(df_total_traffic['TOT_NI_TRAFFIC'] != 0)]
        
        # Creates series of required values and inserts to dataframe
        if not(temp.empty):
            rec = pd.DataFrame([[row[var_portroute],
                                row[var_flow],
                                row['POST_IMB_WEIGHTS'] - row['PRE_IMB_WEIGHTS'],
                                ((row['POST_IMB_WEIGHTS'] 
                                  - row['PRE_IMB_WEIGHTS'])
                                 /temp['TOT_NI_TRAFFIC'].values[0])
                                ]])
            if(df_calc_departures.empty):
                df_calc_departures = rec
            else:
                df_calc_departures = pd.concat([df_calc_departures, rec], axis=0)
                
        return row
    
    
    # Apply function to each row
    df_overseas_residents.apply(ratio,axis = 1)
    
    # Cleanse dataframe
    df_calc_departures.columns = [var_portroute, var_flow, "DIFFERENCE", "RATIO"]
    df_calc_departures.index = range(0,len(df_calc_departures))
    df_calc_departures.drop(1, inplace=True)
    df_calc_departures.reset_index()
    
##     Compare with temp3.sas7bdat
#    compare_dfs("temp3", "temp3.sas7bdat", df_calc_departures)
#    beep()
#    sys.exit()
    
    # Calculate the imbalance weight
    global new_val
    new_val = pd.DataFrame()
    
    
    def get_ratio(row):
        """
        Author        : thorne1
        Date          : 3 Mar 2018
        Purpose       : Conditionally retrieves value from one dataframe and 
                      : inserts to other dataframe   
        """
        global new_val
        
        # Conditionally retrieves rows
        temp = df_calc_departures[(df_calc_departures[var_portroute]==row[var_portroute])
                               &(df_calc_departures[var_flow]==row[var_flow]-1)]
        # Creates series of required values and inserts to dataframe
        if not(temp.empty):
            rec = pd.DataFrame([[row[var_serialNum]
                                 ,temp['RATIO'].values[0]
                               ]])
            if(new_val.empty):
                new_val = rec
            else:
                new_val = pd.concat([new_val, rec], axis=0)
        
        return row


    df_output_data.reset_index(inplace = True)
    df_output_data.apply(get_ratio, axis = 1)

    # Cleanse dataframe
    new_val.columns = ["TEMP_SERIAL","TEMP_RATIO"]
    new_val.index = range(0,len(new_val))
    new_val.drop(1, inplace=True)
    new_val.reset_index()
    
    # Append the imbalance weight to the input
    df_output_data = df_output_data.merge(new_val
                                          , left_on=var_serialNum
                                          , right_on='TEMP_SERIAL'
                                          , how='outer')
    df_output_data.loc[df_output_data["TEMP_RATIO"].notnull()
                       , var_imbalanceWeight] = (1.0 - df_output_data["TEMP_RATIO"])
    df_output_data.drop('TEMP_SERIAL', axis=1, inplace=True)
    df_output_data.drop("TEMP_RATIO", axis=1, inplace=True)
    df_output_data.loc[(df_output_data[var_eligible_flag] != 1.0), var_imbalanceWeight] = 1
    
    # WILL NOT CONVERT FROM SAS TO DATAFRAME TO CSV
    # DECIMAL POINTS ARE OUT
    np.round(df_output_data[var_imbalanceWeight], decimals=4)
#    df_output_data[["SERIAL", var_eligible_flag, var_imbalanceWeight]].to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\surveydata_merge_in_py2.csv")

#    compare_dfs("surveydata_merge_in", "surveydata_merge_in.sas7bdat", df_output_data)
#    beep()    
#    sys.exit()
    
    # Create the summary output
    df_output_data[var_priorSum] = pd.Series(df_output_data[var_shiftWeight]
                                          * df_output_data[var_NRWeight]
                                          * df_output_data[var_minWeight]
                                          * df_output_data[var_trafficWeight]
                                          * df_output_data[var_OOHWeight])
    df_output_data[var_postSum] = pd.Series(df_output_data[var_imbalanceWeight]
                                           * df_output_data[var_shiftWeight]
                                           * df_output_data[var_NRWeight]
                                           * df_output_data[var_minWeight]
                                           * df_output_data[var_trafficWeight]
                                           * df_output_data[var_OOHWeight])
    
    df_sliced = df_output_data[df_output_data[var_priorSum] > 0]
    df_summary_data = df_sliced.groupby([var_flow]).agg({\
            var_priorSum:'sum'
            , var_postSum:'sum'})
    df_summary_data = df_summary_data.reset_index()
    
    #    # Compare with summary_final.sas7bdat
#    compare_dfs("summary_final", "summary_final.sas7bdat", df_summary_data)
##    beep()
#    sys.exit()

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
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

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


def compare_dfs(test_name, sas_file, df, serial_no = True, col_list = False):
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
    print sas_root + "\\" + sas_file
    csv = pd.read_sas(sas_root + "\\" + sas_file)
    
    fdir = r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare"
    sas = "_sas.csv"
    py = "_py.csv"
    
    print("TESTING " + test_name)
    
    if col_list == False:
        csv.to_csv(fdir+"\\"+test_name+sas)
        df.to_csv(fdir+"\\"+test_name+py)
    else:
        csv[col_list].to_csv(fdir+"\\"+test_name+sas)
        df[col_list].to_csv(fdir+"\\"+test_name+py)
    
    print(test_name + " COMPLETE")
    print("")


def compare_dfs2(test_name, sas_file, py_df):
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
    sas_df = pd.read_sas(sas_root + "\\" + sas_file)
    
    print("TESTING " + test_name)
    
    try:
        pd.testing.assert_frame_equal(sas_df, py_df, check_names = False)
    except Exception as err:
        print test_name + " failed.  Details below.."
        print err
    else:
        print test_name + " SUCCESS"

def beep():
    winsound.Beep(440, 250) # frequency, duration
    time.sleep(0.25)        # in seconds (0.25 is 250ms)
    
    winsound.Beep(600, 250)
    time.sleep(0.25)
    
    
def bbeep():
    winsound.Beep(600, 250)
    time.sleep(0.25)
    
    winsound.Beep(440, 250) # frequency, duration
    time.sleep(0.25)        # in seconds (0.25 is 250ms)
    
    

    
    
if __name__ == "__main__":
#    csv = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight\surveydata_merge_in.sas7bdat")
#    csv.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\surveydata_merge_in_sas.csv")
#    
#    csv = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight\temp3_insert.sas7bdat")
#    csv.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\temp3_insert.csv")
#    
#    csv = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight\in_update4.sas7bdat")
#    csv[["SERIAL",var_imbalanceWeight, var_portroute, var_flow]].to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\OUTPUT.csv")

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
