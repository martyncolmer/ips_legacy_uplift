'''
Created on 7 Feb 2018

@author: thorne1
'''

import pandas as pd
import numpy as np
import sys

from pprint import pprint

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
    Dependencies  :
    """
    
    # Load SAS files into dataframes (this  
    # data will come from Oracle eventually)
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Imbalance Weight"
    path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
    
    # Get surveyData dataframe 
    input_data_df = pd.read_sas(path_to_survey_data)
    
    # Change column names to upper case (just in case)
    input_data_df.columns = input_data_df.columns.str.upper()
    
    # If col IMBAL_ELIGIBLE_PV == 1.0 then col IMBAL_WT = 1.0
    input_data_df.loc[input_data_df["IMBAL_ELIGIBLE_PV"] == 1, "IMBAL_WT"] = 1.0
    
    # FUDGE SOME VALUES - TO BE REMOVED WHEN TEST DATASET IS COMPLETE
#    print input_data_df[["NON_RESPONSE_WT", "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT", "FLOW", "ARRIVEDEPART"]]
    input_data_df["NON_RESPONSE_WT"] = 1    
    input_data_df["MINS_WT"] = 2            
    input_data_df["TRAFFIC_WT"] = 3        
    input_data_df["UNSAMP_TRAFFIC_WT"] = 4
    input_data_df["imbal_wt".upper()] = 5
    input_data_df["shift_wt".upper()] = 6
    input_data_df["non_response_wt".upper()] = 7
    input_data_df["mins_wt".upper()] = 8
    input_data_df["traffic_wt".upper()] = 9 
    input_data_df["unsamp_traffic_wt".upper()] = 10
    input_data_df["FLOW"] = np.random.randint(1,10, input_data_df.shape[0])
    input_data_df["ARRIVEDEPART"] = np.random.randint(1,3, input_data_df.shape[0])
#    print input_data_df[["NON_RESPONSE_WT", "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT", "FLOW", "ARRIVEDEPART"]]
    # END OF FUDGE
    
    # Create total traffic df from input_data_df 
    total_traffic_df = input_data_df[["PORTROUTE", "FLOW"]].copy()
    total_traffic_df["TOT_NI_TRAFFIC"] = pd.Series(input_data_df["NON_RESPONSE_WT"] 
                                                   * input_data_df["MINS_WT"]            
                                                   * input_data_df["TRAFFIC_WT"]        
                                                   * input_data_df["UNSAMP_TRAFFIC_WT"])
    
    # Update output with provisional imbalance weight for overseas departures
    # Set imbal_wt to imbal_port_fact_pv
    # where flow in range 1:5
    # and arrivedepart = 2
    overseas_deps_df = input_data_df.copy()
    flow_condition = (overseas_deps_df["FLOW"] == 1) | (overseas_deps_df["FLOW"] == 5)
    arrivedepart_condition = overseas_deps_df["ARRIVEDEPART"] == 2
    overseas_deps_df.loc[(flow_condition) & (arrivedepart_condition)
                         , "IMBAL_WT"] = overseas_deps_df["IMBAL_PORT_FACT_PV"]                               
#    print overseas_deps_df[["FLOW","ARRIVEDEPART","IMBAL_WT, "IMBAL_PORT_FACT_PV""]].head()
    
    # Update output with provisional imbalance weight for overseas arrivals
    # Set imbal_wt to imbal_port_fact_pv
    # where flow in range 3:7
    # and arrivedepart = 1
    flow_condition = (overseas_deps_df["FLOW"] == 3) | (overseas_deps_df["FLOW"] == 7)
    arrivedepart_condition = overseas_deps_df["ARRIVEDEPART"] == 1
    overseas_deps_df.loc[(flow_condition) & (arrivedepart_condition)
                         , "IMBAL_WT"] = overseas_deps_df["IMBAL_PORT_FACT_PV"]
#    print overseas_deps_df[["FLOW","ARRIVEDEPART","IMBAL_WT","IMBAL_PORT_FACT_PV"]].head()
    
    # Update overseas departures with country imbalance
    # set imbal_wt to imbal_wt * imbal_ctry_fact_pv
    # where flow in range 1:5
    flow_condition = (overseas_deps_df["FLOW"] == 1) | (overseas_deps_df["FLOW"] == 5)
    overseas_deps_df.loc[(flow_condition)
                         , "IMBAL_WT"] = (overseas_deps_df["IMBAL_WT"]
                                           * overseas_deps_df["IMBAL_CTRY_FACT_PV"])
#    print overseas_deps_df[["FLOW","IMBAL_WT","IMBAL_CTRY_FACT_PV"]].head()
    
    # Calculate the pre and post sums for overseas residents
    # if prepost_df["FLOW"] in (1,3,5,7):
    # prepost_df["PRE_IMB_WEIGHTS"] = CALC
    # prepost_df["POST_IMB_WEIGHTS"] = CALC
    prepost_df = input_data_df.copy()
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
    
    # Sort data by &var_portroute &var_flow;
    prepost_df = prepost_df.sort_values(by=["PORTROUTE", "FLOW"])
    
    ### proc summary data=prepost; ###
    

    
                         
def calc_imb_weight():
    """
    Author        : thorne1
    Date          : 8 Feb 2018
    Purpose       : Function called to setup and initiate the calculation  
    Parameters    : 
    Returns       :   
    Requirements  :
    Dependencies  :
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    # Connect to Oracle and unload parameter list
    conn = cf.get_oracle_connection()
    global parameters
    parameters = cf.unload_parameters(271)
    
    # output_data_df = something
    # summary_data_df = something
    
    # do_ips_imbweight_calculation(loads of variables)
    
    # Append the output to the empty output table
    
    # Append the summary to the empty summary table
    
    # Log / audit it
    

if __name__ == "__main__":
#    calc_imb_weight()
    do_ips_imbweight_calculation()