import sys
sys.path.insert(0, r'C:\Git_projects\Survey_Support')
sys.path.insert(0, r'C:\Git_projects\IPS_Legacy_Uplift\IPSTransformation')

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

print(type(parameters))
#     calculate_ips_traffic_weight()

def calculate_ips_traffic_weight(schema, dbase, gparam_table_name, parameter_set_id):
    """
    Author       : Nassir Mohammad
    Date         : 31 Jan 2018
    Purpose      : Calculates the IPS traffic weight using GES
    Parameters   : 
    Returns      : The calculated factor value (float), or a None value.
    Requirements : NA
    Dependencies : NA
    """

#     Load SAS files into dataframes (this data will come from Oracle eventually)
    
#     %let SurveyData = ora_data.&SurveyData;
#     %let OutputData = ora_data.&OutputData;
#     %let summaryData = ora_data.&summaryData;
#     %let PopTotals = ora_data.&PopTotals;

    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_IPS_Shift_Weight"    
    path_to_SurveyData = root_data_path + r"\SurveyData.sas7bdat" #.. edit
    path_to_OutputData = root_data_path + r"\OutputData.sas7bdat" #.. edit
    path_to_SummaryData = root_data_path + r"\SummaryData.sas7bdat" #.. edit
    path_to_PopTotals = root_data_path + r"\PopTotals.sas7bdat" #..eidt
        
#     /* create a SAS dataset from the survey data */
#     data survey;
#     set &SurveyData;
#     run;     
    
    df_survey = pd.read_sas(path_to_SurveyData)

#     /* create a SAS dataset from the population totals */
#     data trtotals;
#     set &PopTotals;
#     run;

    df_trtotals = pd.read_sas(path_to_PopTotals)

#     %do_ips_trafweight_calculation(survey, summary, &var_serialNum, &var_shiftWeight, 
#                                    &var_NRWeight, &var_minWeight, &StrataDef, trtotals, 
#                                    &TotalVar, &MaxRuleLength, &ModelGroup, output, &GWeightVar, 
#                                    &GESBoundType, &GESUpperBound, &GESLowerBound, &GESMaxDiff,
#                                    &GESMaxIter, &GESMaxDist, &var_count, &var_trafficTotal, 
#                                    &var_postSum, &minCountThresh);

#     /* append the weights to the output table */
#     proc append base=&OutputData data=output force;
#     run;

    df_OutputData = pd.read_sas(path_to_OutputData)
    df_OutputData.append(df_output)    

#     /* append summary information to summary table */
#     proc append base=&summaryData data=summary force;
#     run;

    df_SummaryData = pd.read_sas(path_to_SummaryData)
    df_SummaryData.append(df_summary)

#     /* commit the response information to the response table */
#     %commit_ips_sas_response;

####################################
#
# do_ips_trafweight_calculation
#
####################################

def do_ips_trafweight_calculation():
    pass

# do_ips_ges_weighting()

def do_ips_ges_weighting():
    pass

# do_ips_ges_weighting calls the following function:

# ips_check_ges_totals()
# ips_setup_ges_auxvars()
# ips_assign_ges_auxiliaries()
# ips_get_population_totals()

 # generate_ips_tw_summary()
 
def generate_ips_tw_summary():
    pass
