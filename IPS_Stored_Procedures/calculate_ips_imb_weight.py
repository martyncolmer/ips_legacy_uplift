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
    
#    print survey_data_df[["NON_RESPONSE_WT"
#                         , "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT"
#                         , "imbal_wt".upper(), "shift_wt".upper()
#                         , "non_response_wt".upper(), "mins_wt".upper()
#                         , "traffic_wt".upper(), "unsamp_traffic_wt".upper()
#                         , "FLOW", "ARRIVEDEPART"]].head()
#    print survey_data_df.info()
#    sys.exit()
    
    # Change column names to upper case (just in case)
    survey_data_df.columns = survey_data_df.columns.str.upper()
    
    # If col IMBAL_ELIGIBLE_PV == 1.0 then col IMBAL_WT = 1.0
    survey_data_df.loc[survey_data_df["IMBAL_ELIGIBLE_PV"] == 1, "IMBAL_WT"] = 1.0
    
    # FUDGE SOME VALUES - TO BE REMOVED WHEN TEST DATASET IS COMPLETE
#    print survey_data_df[["NON_RESPONSE_WT", "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT", "imbal_wt".upper(), "shift_wt".upper(), "non_response_wt".upper(), "mins_wt".upper(), "traffic_wt".upper(), "unsamp_traffic_wt".upper(), "FLOW", "ARRIVEDEPART"]]
    survey_data_df["NON_RESPONSE_WT"] = 1    
    survey_data_df["MINS_WT"] = 2
    survey_data_df["TRAFFIC_WT"] = 3
    survey_data_df["UNSAMP_TRAFFIC_WT"] = 4
    survey_data_df["ARRIVEDEPART"] = np.random.randint(1,3, survey_data_df.shape[0])
    survey_data_df["IMBAL_CTRY_FACT_PV"] = np.around(np.random.rand(survey_data_df.shape[0]), 2)
#    survey_data_df["imbal_wt".upper()] = 5 
#    survey_data_df["shift_wt".upper()] = 6
#    survey_data_df["IMBAL_PORT_FACT_PV"] = 0.99
#    survey_data_df["FLOW"] = np.random.randint(1,10, survey_data_df.shape[0])         
    # END OF FUDGE
    
#    print survey_data_df[["NON_RESPONSE_WT"
#                         , "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT"
#                         , "imbal_wt".upper(), "shift_wt".upper()
#                         , "non_response_wt".upper(), "mins_wt".upper()
#                         , "traffic_wt".upper(), "unsamp_traffic_wt".upper()
#                         , "FLOW", "ARRIVEDEPART"]].head()
#    survey_data_df.info()
#    sys.exit()

    # Create total traffic df
    total_traffic_df = survey_data_df[["PORTROUTE", "FLOW"]].copy()
    total_traffic_df["TOT_NI_TRAFFIC"] = pd.Series(survey_data_df["NON_RESPONSE_WT"] 
                                                   * survey_data_df["MINS_WT"]            
                                                   * survey_data_df["TRAFFIC_WT"]        
                                                   * survey_data_df["UNSAMP_TRAFFIC_WT"])
#    print "total_traffic_df"
#    print total_traffic_df.info()
#    sys.exit()

    # Update output with provisional imbalance weight for overseas departures
    overseas_deps_df = survey_data_df.copy()
    flow_condition = (overseas_deps_df["FLOW"] == 1) | (overseas_deps_df["FLOW"] == 5)
    arrivedepart_condition = overseas_deps_df["ARRIVEDEPART"] == 2
    overseas_deps_df.loc[(flow_condition) & (arrivedepart_condition)
                         , "IMBAL_WT"] = overseas_deps_df["IMBAL_PORT_FACT_PV"]                               
#    print overseas_deps_df[["FLOW","ARRIVEDEPART","IMBAL_WT", "IMBAL_PORT_FACT_PV"]].head()
#    print overseas_deps_df.info()
#    sys.exit()
    
    # Update output with provisional imbalance weight for overseas arrivals
    flow_condition = (overseas_deps_df["FLOW"] == 3) | (overseas_deps_df["FLOW"] == 7)
    arrivedepart_condition = overseas_deps_df["ARRIVEDEPART"] == 1
    overseas_deps_df.loc[(flow_condition) & (arrivedepart_condition)
                         , "IMBAL_WT"] = overseas_deps_df["IMBAL_PORT_FACT_PV"]
#    print overseas_deps_df[["FLOW","ARRIVEDEPART","IMBAL_WT","IMBAL_PORT_FACT_PV"]].head()
#    print overseas_deps_df.info()
#    sys.exit()
    
    # Update overseas departures with country imbalance
    flow_condition = (overseas_deps_df["FLOW"] == 1) | (overseas_deps_df["FLOW"] == 5)
    overseas_deps_df.loc[(flow_condition)
                         , "IMBAL_WT"] = (overseas_deps_df["IMBAL_WT"]
                                           * overseas_deps_df["IMBAL_CTRY_FACT_PV"])
#    print overseas_deps_df[["FLOW","IMBAL_WT","IMBAL_CTRY_FACT_PV"]].head()
#    print overseas_deps_df.info()
#    sys.exit()

    # Calculate the pre and post sums for overseas residents
    prepost_df = survey_data_df.copy()
#    print prepost_df[["FLOW"
#                      , "imbal_wt".upper()
#                      , "shift_wt".upper()
#                      , "mins_wt".upper()
#                      , "traffic_wt".upper()
#                      , "unsamp_traffic_wt".upper()
#                      , "non_response_wt".upper()
#                      , "unsamp_traffic_wt".upper()]].head()
    flow_condition = ((prepost_df["FLOW"] == 1) 
                      | (prepost_df["FLOW"] == 3) 
                         | (prepost_df["FLOW"] == 5) 
                            | (prepost_df["FLOW"] == 7))
    pre_imb_weights_calc = (prepost_df["shift_wt".upper()] 
                       * prepost_df["non_response_wt".upper()]
                       * prepost_df["mins_wt".upper()]
                       * prepost_df["traffic_wt".upper()]
                       * prepost_df["unsamp_traffic_wt".upper()])
    post_imb_weights_calc = (prepost_df["imbal_wt".upper()] 
                            * prepost_df["shift_wt".upper()]
                            * prepost_df["non_response_wt".upper()]
                            * prepost_df["mins_wt".upper()]
                            * prepost_df["traffic_wt".upper()]
                            * prepost_df["unsamp_traffic_wt".upper()])
    prepost_df.loc[(flow_condition), "PRE_IMB_WEIGHTS"] = pre_imb_weights_calc
    prepost_df.loc[(flow_condition), "POST_IMB_WEIGHTS"] = post_imb_weights_calc
#    print prepost_df[["FLOW"
#                      , "imbal_wt".upper()
#                      , "shift_wt".upper()
#                      , "mins_wt".upper()
#                      , "traffic_wt".upper()
#                      , "unsamp_traffic_wt".upper()
#                      , "non_response_wt".upper()
#                      , "unsamp_traffic_wt".upper()
#                      , "PRE_IMB_WEIGHTS"
#                      , "POST_IMB_WEIGHTS"]].head()
#    print prepost_df.info()
#    sys.exit()
    
    # Sort data by &var_portroute &var_flow;
    prepost_df = prepost_df.sort_values(by=["PORTROUTE", "FLOW"])
#    print "prepost_df"
#    print prepost_df.info()
#    print ""
#    print prepost_df.head()
#    sys.exit()
    
    # Summarise
    # SELECT sum(PREPOST_DF[pre_imb_weights]) as pre_imb_weights
    #      , sum(PREPOST_DF[post_imb_weights]) as post_imb_weights 
    # FROM PREPOST_DF
    # GROUP BY PREPOST_DF[&var_portroute]
    #      , PREPOST_DF[&var_flow]
    temp2_df = prepost_df.groupby(["PORTROUTE", "FLOW"]).agg({\
            'PRE_IMB_WEIGHTS':'sum'
            ,'POST_IMB_WEIGHTS':'sum'})
    temp2_df = temp2_df.reset_index() 
#    print temp2_df 
#    print temp2_df.info()
#    sys.exit() 
    
    # Calculate the difference between pre and post imbalance  
    # weighting for departures and calculate the ratio of the 
    # difference for departures at each port
    portroute_condition = total_traffic_df["PORTROUTE"].isin(temp2_df["PORTROUTE"])
    
    total_traffic_flow_range = [2,4,6,8]
    temp2_flow_range = [1,3,5,7]
    flow_condition = ((total_traffic_df["FLOW"].isin(total_traffic_flow_range)) 
                            | (temp2_df["FLOW"].isin(temp2_flow_range)))
    
    total_traffic_condition = (total_traffic_df["TOT_NI_TRAFFIC"] != 0)
    
    all_conditions = ((portroute_condition)
                           & (flow_condition)
                           & (total_traffic_condition))
    
    temp3_df = temp2_df.loc[all_conditions]
    temp4_df = temp3_df.copy()

    temp4_df["DIFFERENCE"] = pd.Series(temp3_df["POST_IMB_WEIGHTS"] 
                                                   - temp3_df["PRE_IMB_WEIGHTS"])
    temp4_df["RATIO"] = pd.Series(temp4_df["DIFFERENCE"] 
                                                   / total_traffic_df["TOT_NI_TRAFFIC"])
    del temp4_df["POST_IMB_WEIGHTS"]
    del temp4_df["PRE_IMB_WEIGHTS"]

    print temp4_df
    sys.exit()
    

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
    global survey_data_df
    
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
        survey_data_df = cf.get_table_values(parameters["SurveyData"])
    else:
        # Load SAS files into dataframes (this  
        # data will come from Oracle eventually)
        root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
        path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
        survey_data_df = pd.read_sas(path_to_survey_data)
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