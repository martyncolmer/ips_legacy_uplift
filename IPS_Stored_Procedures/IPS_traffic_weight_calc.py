import sys
# sys.path.insert(0, r'C:\Git_projects\Survey_Support')
# sys.path.insert(0, r'C:\Git_projects\IPS_Legacy_Uplift\IPSTransformation')

import os
import logging
import inspect
import numpy as np
import pandas as pd
from sas7bdat import SAS7BDAT
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict

import survey_support
from IPSTransformation import CommonFunctions as cf

# Call JSON configuration file for error logger setup
survey_support.setup_logging('IPS_logging_config_debug.json')

# Connect to Oracle and unload parameter list
conn = cf.get_oracle_connection()
global parameters
parameters = cf.unload_parameters(205)

def calculate(): #def calculate_ips_traffic_weight(schema, dbase, gparam_table_name, parameter_set_id): # calculate()
    """
    Author       : Nassir Mohammad
    Date         : 19 Feb 2018
    Purpose      : Calculates the IPS traffic weight using GES
    Parameters   : 
    Returns      : The calculated factor value (float), or a None value.
    Requirements : NA
    Dependencies : NA
    """

    # Load SAS files into dataframes (this data will come from Oracle eventually)
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"    
    path_to_SurveyData = root_data_path + r"\step_1_surveydata_1.sas7bdat" #.. edit
    path_to_OutputData = root_data_path + r"\surveydata_1.sas7bdat" #.. edit
    path_to_SummaryData = root_data_path + r"\surveydata_1.sas7bdat" #.. edit
    path_to_PopTotals = root_data_path + r"\sas_traffic_data.sas7bdat" #..eidt
    
    # create a SAS dataset from the survey data
     
    # empty right now
    #df_surveydata = cf.get_table_values("sas_survey_subsample")
    #print(df_surveydata)
     
    df_survey = pd.read_sas(path_to_SurveyData)
    #df_surveydata = SAS7BDAT(path_to_SurveyData).to_data_frame() # using existing method?  

    # create a SAS dataset from the population totals 
    df_trtotals = pd.read_sas(path_to_PopTotals)
    #df_trtotals = SAS7BDAT(path_to_PopTotals).to_data_frame() # using existing method?  
    #print(df_trtotals)

    # These variables are passed into SAS but not required, we also pass them in for now
    summary = "summary"
    output = "output"
     
    StrataDef = ["samp_port_grp_pv".upper(), "arrivedepart".upper()] 
    
    do_ips_trafweight_calculation(df_survey, summary, "serial", "shift_wt".upper(), 
                                    "non_response_wt".upper(), "mins_wt".upper(),StrataDef, df_trtotals, 
                                    "traffictotal".upper(), "512".upper(), "C_group".upper(), output, "traffic_wt".upper(), 
                                    "G", "", "1.0", "1E-8",
                                    "50", "1E-8", "cases", "traffictotal".upper(), 
                                    "sum_traffic_wt".upper(), "30")

    # append the weights to the output table 
#     proc append base=&OutputData data=output force;
#     run;

#     df_OutputData = pd.read_sas(path_to_OutputData)
#     df_OutputData.append(df_output)    

    # append summary information to summary table
#     proc append base=&summaryData data=summary force;
#     run;

#     df_SummaryData = pd.read_sas(path_to_SummaryData)
#     df_SummaryData.append(df_summary)

    # commit the response information to the response table
#     %commit_ips_sas_response;

def do_ips_trafweight_calculation(df_survey, summary, var_serialNum, var_shiftWeight, var_NRWeight,
                                     var_minWeight, StrataDef, PopTotals, TotalVar,      
                                     MaxRuleLength, ModelGroup, output, GWeightVar, 
                                     GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
                                     GESMaxIter, GESMaxDist, var_count, var_trafficTotal, 
                                     var_postSum, minCountThresh):
    
    """
    Author       : Nassir Mohammad
    Date         : 19 Feb 2018
    Purpose      : Calculates the IPS traffic weight using GES
    Parameters   : TODO
    Returns      : TODO
    Requirements : TODO
    Dependencies : TODO
    """
    
    # Generate the design weights    
    trafDesignWeight = "trafDesignWeight".upper()        
    df_survey[trafDesignWeight] = df_survey[var_shiftWeight] * df_survey[var_shiftWeight] * df_survey[var_shiftWeight]   
    
    # Summarise the population totals over the strata
    PopTotals = PopTotals.sort_values(StrataDef)
    
    df_popTotals = PopTotals.groupby(StrataDef)[TotalVar].agg({TotalVar: 'sum'}).reset_index()
    df_popTotals.index = range(df_popTotals.shape[0]) # required?   
    
    # Call the GES weighting macro        
    CalWeight = None # this is passed in by SAS, but probably should not be in future code
    
    CalWeight = do_ips_ges_weighting(df_survey, var_serialNum, trafDesignWeight, StrataDef, df_popTotals,
                         TotalVar, MaxRuleLength, ModelGroup, output, GWeightVar, CalWeight,
                         GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
                         GESMaxIter, GESMaxDist,) 
    
    # Generate the summary table    
    generate_ips_tw_summary(df_survey, output, summary, StrataDef, var_count,
                            var_serialNum, GWeightVar, trafDesignWeight,
                            var_trafficTotal, df_popTotals, var_postSum, minCountThresh)
    
    # Round the weights to 3dp
    df_output =  df_output[GWeightVar] = round(GWeightVar, 3)
    
def do_ips_ges_weighting():
    pass

# do_ips_ges_weighting calls the following function:

# ips_check_ges_totals()
# ips_setup_ges_auxvars()
# ips_assign_ges_auxiliaries()
# ips_get_population_totals()

# ----------------------------------
#    
# generate_ips_tw_summary()
#
# ----------------------------------
 
def generate_ips_tw_summary():
    pass

calculate()