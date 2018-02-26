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
    
    df_output_data = gldf_survey_data.copy() 
    
    
    # Test SurveyData
#    cf.compare_datasets("SurveyData", r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight\surveydata.sas7bdat", df_output_data)
    
    
    # Do some initial setup and selection
    df_output_data.drop(df_output_data[df_output_data[var_eligible_flag] != 1.0].index, inplace=True)
    df_output_data.loc[df_output_data[var_eligible_flag] == 1, var_imbalanceWeight] = 1.0
    
    
    # Compare with in_1.sas7bdat
#    compare_dfs("in_1", "in_1.sas7bdat", df_output_data, ["SERIAL", "IMBAL_ELIGIBLE_PV", "IMBAL_WT"])
#    sys.exit()
    
    
    # Create total traffic df
    df_total_traffic = df_output_data[[var_portroute, var_flow]].copy()
    df_total_traffic.sort_values(by = [var_portroute, var_flow])
    df_total_traffic["TOT_NI_TRAFFIC"] = pd.Series(df_output_data[var_shiftWeight]
                                                   * df_output_data[var_NRWeight] 
                                                   * df_output_data[var_minWeight]            
                                                   * df_output_data[var_trafficWeight]        
                                                   * df_output_data[var_OOHWeight])
    df_total_traffic = df_total_traffic.groupby([var_portroute, var_flow])["TOT_NI_TRAFFIC"].agg({"TOT_NI_TRAFFIC" : 'sum'})
    df_total_traffic.reset_index(inplace=True)
    
    
#    print df_total_traffic
#    sys.exit()
    

    # TEST temp1.sas7bdat
#    compare_dfs("temp1", "temp1.sas7bdat", df_total_traffic)
#    sys.exit()
    
    
    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    arrivedepart_condition = df_output_data[var_direction] == 2
    df_output_data.loc[(flow_condition) & (arrivedepart_condition)
                         , var_imbalanceWeight] = df_output_data[var_pgFactor]


#    # TEST in_update1.sas7bdat
#    compare_dfs("in_update1", "in_update1.sas7bdat", df_output_data, ["SERIAL", "FLOW","ARRIVEDEPART","IMBAL_WT", "IMBAL_PORT_FACT_PV"])
#    sys.exit()  

  
    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_output_data[var_flow] == 3) | (df_output_data[var_flow] == 7)
    arrivedepart_condition = df_output_data[var_direction] == 1
    df_output_data.loc[(flow_condition) & (arrivedepart_condition)
                         , var_imbalanceWeight] = df_output_data[var_pgFactor]
    
    
    # Compare with in_update2.sas7bdat
#    compare_dfs("in_update2", "in_update2.sas7bdat", df_output_data, ["SERIAL", "FLOW","ARRIVEDEPART","IMBAL_WT","IMBAL_PORT_FACT_PV"])
#    sys.exit()
    
    
    # Update overseas departures with country imbalance
    flow_condition = (df_output_data[var_flow] == 1) | (df_output_data[var_flow] == 5)
    df_output_data.loc[(flow_condition)
                         , var_imbalanceWeight] = (df_output_data[var_imbalanceWeight]
                                           * df_output_data[var_cgFactor])
                         
    # Compare with in_update3.sas7bdat
#    compare_dfs("in_update3", "in_update3.sas7bdat", df_output_data, ["SERIAL", "FLOW","ARRIVEDEPART","IMBAL_WT","IMBAL_PORT_FACT_PV"])
#    sys.exit()
    
    
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


##    # Compare with prepost.sas7bdat
#    compare_dfs("prepost", "prepost.sas7bdat", df_prepost, ["SERIAL", "AGE", "APORTLATDEG", "APORTLATMIN", "SHIFT_WT", "IMBAL_WT"])
#    sys.exit()

    
    # Summarise - group by PORTROUTE and FLOW, and total the pre and post imbalanace weights
    df_prepost.sort_values(by=[var_portroute, var_flow])

    df_overseas_residents = df_prepost.groupby([var_portroute, var_flow]).agg({\
            'PRE_IMB_WEIGHTS':'sum'
            ,'POST_IMB_WEIGHTS':'sum'})
    df_overseas_residents = df_overseas_residents.reset_index()
#    print df_overseas_residents
#    sys.exit()
    
    
    # Compare with temp2.sas7bdat
#    print "Testing temp2/df_overseas_residents"
#    print ""
#    print df_overseas_residents
#    compare_dfs("temp2", "temp2.sas7bdat", df_overseas_residents)
    
    # Calculate the difference between pre & post imbalance weighting for departures  
    # and calculate the ratio of the difference for departures at each port.  

    global df_calc_departures
    df_calc_departures = pd.DataFrame()
    
    def ratio(row):
        global df_calc_departures
        temp = df_total_traffic[(df_total_traffic['PORTROUTE']==row['PORTROUTE'])
                               &(df_total_traffic['FLOW']==row['FLOW']+1)
                               &(df_total_traffic['TOT_NI_TRAFFIC'] != 0)]
        if not(temp.empty):
            rec = pd.DataFrame([[row['PORTROUTE'],
                                row['FLOW'],
                                row['POST_IMB_WEIGHTS'] - row['PRE_IMB_WEIGHTS'],
                                ((row['POST_IMB_WEIGHTS'] - row['PRE_IMB_WEIGHTS'])/temp['TOT_NI_TRAFFIC'].values[0])
                                ]])
            if(df_calc_departures.empty):
                df_calc_departures = rec
            else:
                df_calc_departures = pd.concat([df_calc_departures, rec], axis=0)
                
        return row
    
    df_overseas_residents.apply(ratio,axis = 1)
    
    # Cleanse dataframe
    df_calc_departures.columns = ["PORTROUTE", "FLOW", "DIFFERENCE", "RATIO"]
    df_calc_departures.index = range(0,len(df_calc_departures))
    df_calc_departures.drop(1, inplace=True)
    df_calc_departures.reset_index()
    
##     Compare with temp3.sas7bdat
#    print df_calc_departures
#    compare_dfs("temp3", "temp3.sas7bdat", df_calc_departures)
#    sys.exit()

    # Calculate the imbalance weight
    df_output_data.reset_index(drop=True,inplace=True)
    df_output_data["RATIO"] = np.nan
    flow_range = [2,4,6,8]
                       
    df_output_data.loc[((df_output_data.PORTROUTE.isin(df_calc_departures.PORTROUTE)) 
                        & (df_output_data[var_flow].isin(flow_range)))
                       , var_imbalanceWeight] = (1.0 - df_output_data["RATIO"])
    
    
    # Compare with in_update4.sas7bdat
#    print df_calc_departures
#    sys.exit()
    compare_dfs("in_update4", "in_update4.sas7bdat", df_output_data, ["SERIAL","IMBAL_WT", "PORTROUTE", "FLOW"])
    sys.exit()
    
    
    # Append the imbalance weight to the input
    serial_range = list(df_output_data[var_serialNum])
    serial_condition = gldf_survey_data[var_serialNum].isin(serial_range)
    gldf_survey_data.loc[~serial_condition, var_imbalanceWeight] = 1


##    # Compare with surveydata_merge_in.sas7bdat
    print "Testing surveydata_merge_in/gldf_survey_data"
    print ""
    print gldf_survey_data
#    sys.exit()


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


#    # Compare with summary_final.sas7bdat
    compare_dfs("summary_final", "summary_final.sas7bdat", df_summary_data)
    sys.exit()
    
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


def compare_dfs(test_name, sas_file, df, col_list = False):
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
