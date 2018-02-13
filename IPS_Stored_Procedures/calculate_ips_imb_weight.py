'''
Created on 7 Feb 2018

@author: thorne1
'''

import pandas as pd
import numpy as np
import inspect
import sys

from pprint import pprint
from sas7bdat import SAS7BDAT

import survey_support
from IPSTransformation import CommonFunctions as cf

def do_ips_imbweight_calculation():
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       :    
    Parameters    : 
    Returns       :   
    Requirements  :
    """
    
#    print df_survey_data[["NON_RESPONSE_WT"
#                         , "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT"
#                         , "imbal_wt".upper(), "shift_wt".upper()
#                         , "non_response_wt".upper(), "mins_wt".upper()
#                         , "traffic_wt".upper(), "unsamp_traffic_wt".upper()
#                         , "FLOW", "ARRIVEDEPART"]].head()
#    print df_survey_data.info()
#    sys.exit()
    
    # Change column names to upper case (just in case)
    df_survey_data.columns = df_survey_data.columns.str.upper()
    
    # Do some initial setup and selection  
    df_survey_data.loc[df_survey_data["IMBAL_ELIGIBLE_PV"] == 1, "IMBAL_WT"] = 1.0
    
    # FUDGE SOME VALUES - TO BE REMOVED WHEN TEST DATASET IS COMPLETE
#    print df_survey_data[["NON_RESPONSE_WT", "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT", "imbal_wt".upper(), "shift_wt".upper(), "non_response_wt".upper(), "mins_wt".upper(), "traffic_wt".upper(), "unsamp_traffic_wt".upper(), "FLOW", "ARRIVEDEPART"]]
    df_survey_data["NON_RESPONSE_WT"] = 1    
    df_survey_data["MINS_WT"] = 2
    df_survey_data["TRAFFIC_WT"] = 3
    df_survey_data["UNSAMP_TRAFFIC_WT"] = 4
    df_survey_data["ARRIVEDEPART"] = np.random.randint(1,3, df_survey_data.shape[0])
    df_survey_data["IMBAL_CTRY_FACT_PV"] = np.around(np.random.rand(df_survey_data.shape[0]), 2)
#    df_survey_data["imbal_wt".upper()] = 5 
#    df_survey_data["shift_wt".upper()] = 6
#    df_survey_data["IMBAL_PORT_FACT_PV"] = 0.99
#    df_survey_data["FLOW"] = np.random.randint(1,10, df_survey_data.shape[0])         
    # END OF FUDGE
    
#    print df_survey_data[["NON_RESPONSE_WT"
#                         , "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT"
#                         , "imbal_wt".upper(), "shift_wt".upper()
#                         , "non_response_wt".upper(), "mins_wt".upper()
#                         , "traffic_wt".upper(), "unsamp_traffic_wt".upper()
#                         , "FLOW", "ARRIVEDEPART"]].head()
#    df_survey_data.info()
#    sys.exit()

    # Create total traffic df
    df_total_traffic = df_survey_data[["PORTROUTE", "FLOW"]].copy()
    df_total_traffic["TOT_NI_TRAFFIC"] = pd.Series(df_survey_data["NON_RESPONSE_WT"] 
                                                   * df_survey_data["MINS_WT"]            
                                                   * df_survey_data["TRAFFIC_WT"]        
                                                   * df_survey_data["UNSAMP_TRAFFIC_WT"])
#    print "df_total_traffic"
#    print df_total_traffic.info()
#    sys.exit()

    # Update output with provisional imbalance weight for overseas departures
    flow_condition = (df_survey_data["FLOW"] == 1) | (df_survey_data["FLOW"] == 5)
    arrivedepart_condition = df_survey_data["ARRIVEDEPART"] == 2
    df_survey_data.loc[(flow_condition) & (arrivedepart_condition)
                         , "IMBAL_WT"] = df_survey_data["IMBAL_PORT_FACT_PV"]                               
#    print df_survey_data[["FLOW","ARRIVEDEPART","IMBAL_WT", "IMBAL_PORT_FACT_PV"]].head()
#    print df_survey_data.info()
#    sys.exit()
    
    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (df_survey_data["FLOW"] == 3) | (df_survey_data["FLOW"] == 7)
    arrivedepart_condition = df_survey_data["ARRIVEDEPART"] == 1
    df_survey_data.loc[(flow_condition) & (arrivedepart_condition)
                         , "IMBAL_WT"] = df_survey_data["IMBAL_PORT_FACT_PV"]
#    print df_survey_data[["FLOW","ARRIVEDEPART","IMBAL_WT","IMBAL_PORT_FACT_PV"]].head()
#    print df_survey_data.info()
#    sys.exit()
    
    # Update overseas departures with country imbalance
    flow_condition = (df_survey_data["FLOW"] == 1) | (df_survey_data["FLOW"] == 5)
    df_survey_data.loc[(flow_condition)
                         , "IMBAL_WT"] = (df_survey_data["IMBAL_WT"]
                                           * df_survey_data["IMBAL_CTRY_FACT_PV"])
#    print df_survey_data[["FLOW","IMBAL_WT","IMBAL_CTRY_FACT_PV"]].head()
#    print df_survey_data.info()
#    sys.exit()

    # Calculate the pre and post sums for overseas residents
    df_prepost = df_survey_data.copy()
#    print df_prepost[["FLOW"
#                      , "imbal_wt".upper()
#                      , "shift_wt".upper()
#                      , "mins_wt".upper()
#                      , "traffic_wt".upper()
#                      , "unsamp_traffic_wt".upper()
#                      , "non_response_wt".upper()
#                      , "unsamp_traffic_wt".upper()]].head()
    prepost_flow_range = [1,3,5,7]
    flow_condition = ((df_prepost["FLOW"].isin(prepost_flow_range)))
    pre_imb_weights_calc = (df_prepost["shift_wt".upper()] 
                       * df_prepost["non_response_wt".upper()]
                       * df_prepost["mins_wt".upper()]
                       * df_prepost["traffic_wt".upper()]
                       * df_prepost["unsamp_traffic_wt".upper()])
    post_imb_weights_calc = (df_prepost["imbal_wt".upper()] 
                            * df_prepost["shift_wt".upper()]
                            * df_prepost["non_response_wt".upper()]
                            * df_prepost["mins_wt".upper()]
                            * df_prepost["traffic_wt".upper()]
                            * df_prepost["unsamp_traffic_wt".upper()])
    df_prepost.loc[(flow_condition), "PRE_IMB_WEIGHTS"] = pre_imb_weights_calc
    df_prepost.loc[(flow_condition), "POST_IMB_WEIGHTS"] = post_imb_weights_calc
#    print df_prepost[["FLOW"
#                      , "imbal_wt".upper()
#                      , "shift_wt".upper()
#                      , "mins_wt".upper()
#                      , "traffic_wt".upper()
#                      , "unsamp_traffic_wt".upper()
#                      , "non_response_wt".upper()
#                      , "unsamp_traffic_wt".upper()
#                      , "PRE_IMB_WEIGHTS"
#                      , "POST_IMB_WEIGHTS"]].head()
#    print df_prepost.info()
#    sys.exit()
    
    # Summarise - group by PORTROUTE and FLOW, 
    # and total the pre and post imbalanace weights
    df_overseas_residents = df_prepost.groupby(["PORTROUTE", "FLOW"]).agg({\
            'PRE_IMB_WEIGHTS':'sum'
            ,'POST_IMB_WEIGHTS':'sum'})
    df_overseas_residents = df_overseas_residents.reset_index() 
#    print df_overseas_residents 
#    print df_overseas_residents.info()
#    sys.exit() 
    
    # Calculate the difference between pre and post imbalance
    # weighting for departures and calculate the ratio  
    # of the difference for departures at each port
    
    # Set first condition
    portroute_condition = df_total_traffic["PORTROUTE"].isin(df_overseas_residents["PORTROUTE"])
    
    # Set second condition
    total_traffic_flow_range = [2,4,6,8]
    overseas_residents_flow_range = [1,3,5,7]
    flow_condition = ((df_total_traffic["FLOW"].isin(total_traffic_flow_range)) 
                            | (df_overseas_residents["FLOW"].isin(overseas_residents_flow_range)))
    
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
#
#    print df_calc_departures
#    sys.exit()

    # Calculate the imbalance weight

    
    # Set conditions of first and second inner nested select
    flow_range1 = [2,4,6,8]
    flow_range2 = [1,3,5,7]
    condition1 = df_survey_data["PORTROUTE"].isin(df_calc_departures["PORTROUTE"])
    condition2 = ((df_survey_data["FLOW"].isin(flow_range1)) | (df_calc_departures["FLOW"].isin(flow_range2)))
    nested_condition = ((condition1) & (condition2))
    
    
        
    
#    UPDATE df_survey_data
#    SET df_survey_data['IMBAL_WT'] = 1.0 - (SELECT df_calc_departures["RATIO"] 
#                                            FROM df_calc_departures
        #                                        WHERE df_survey_data["PORTROUTE"] = df_calc_departures["PORTROUTE"]
        #                                              & ((df_survey_data["FLOW"] = 2 & df_calc_departures["FLOW"] = 1) |
        #                                                   (df_survey_data["FLOW"] = 4 & df_calc_departures["FLOW"] = 3) |
        #                                                    (df_survey_data["FLOW"] = 6 & df_calc_departures["FLOW"] = 5) |
        #                                                   (df_survey_data["FLOW"] = 8 & df_calc_departures["FLOW"] = 7)))
#    WHERE EXISTS (SELECT * FROM df_calc_departures
#                    WHERE df_survey_data["PORTROUTE"] = df_calc_departures["PORTROUTE"]
#                          & ((df_survey_data["FLOW"] = 2 & df_calc_departures["FLOW"] = 1) |
#                               (df_survey_data["FLOW"] = 4 & df_calc_departures["FLOW"] = 3) |
#                                (df_survey_data["FLOW"] = 6 & df_calc_departures["FLOW"] = 5) |
#                               (df_survey_data["FLOW"] = 8 & df_calc_departures["FLOW"] = 7)));
    

    
    

def calc_imb_weight(oracle_dataset = True):
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       : Function called to setup and initiate the calculation  
    Parameters    : 
    Returns       :   
    Requirements  :
    Dependencies  :
    """
    
    global logger
    global parameters
    global output_data_df
    global summary_data_df
    global df_survey_data
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    logger = cf.database_logger()
    
    # Unload parameter list
    parameters = cf.unload_parameters(271)
#    pprint(parameters)
#    sys.exit()
    
    output_data_df = cf.get_table_values(parameters["OutputData"])
    summary_data_df = cf.get_table_values(parameters["SummaryData"])
    
    # CHOOSE MY PREFERRED DATASET
    if oracle_dataset == True:
        df_survey_data = cf.get_table_values(parameters["SurveyData"])
    else:
        # Load SAS files into dataframes (this  
        # data will come from Oracle eventually)
        root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
        path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
        df_survey_data = pd.read_sas(path_to_survey_data)
    # END OF PREFERECNE FUDGE
    
    do_ips_imbweight_calculation()
    # do_ips_imbweight_calculation(loads of variables)
    
    # Append the output to the empty output table
    
    # Append the summary to the empty summary table
    
    # Log / audit it
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
#    function_name = str(inspect.stack()[0][3])
#    audit_message = "Load Imbalance Weight calculation: %s()" % function_name
#    
#    logger.info("SUCCESS - Completed Imbalance Weight calculation.")
#    cf.commit_to_audit_log("Something", "Imbalance", audit_message)
#    print("Completed - Calculate Imbalance Weight")
    

if __name__ == "__main__":
    calc_imb_weight(False)
    # DAVE'S DATASET
#    calc_imb_weight(False)
#    do_ips_imbweight_calculation()