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

# TODO:

# 1. Assert SAS output and python outputs match
# 2. do_ips_ges_weighting()
# 3. generate_ips_tw_summary()
            
#def calculate_ips_traffic_weight(schema, dbase, gparam_table_name, parameter_set_id): # calculate()              
def calculate(SurveyData, OutputData, SummaryData, ResponseTable, var_serialNum,
              var_shiftWeight, var_NRWeight, var_minWeight, StrataDef, PopTotals,
              TotalVar, MaxRuleLength, ModelGroup, GWeightVar,
              GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
              GESMaxIter, GESMaxDist, var_count, var_trafficTotal,
              var_postSum, minCountThresh): 
    
    """
    Author       : Nassir Mohammad
    Date         : 19 Feb 2018
    Purpose      : Calculates the IPS traffic weight using GES
    Parameters   : SurveyData = the IPS survey records for the period                        
                   summaryData = Oracle table to hold the summary data                        
                   responseTable = Oracle table to hold response information (status etc.)    
                   var_serialNum = variable holding the record serial number (UID)            
                   var_shiftWeight     = variable holding the shift weight field name             
                   var_NRWeight = variable holding the non-response weight field name        
                   var_minWeight = variable holding the minimum weight field name            
                   StrataDef = List of classificatory variables                            
                   PopTotals = Population totals file                                         
                   TotalVar = Variable that holds the population totals                    
                   MaxRuleLength = maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup = Variable that will hold the model group number                
                   OutputData = output file (holds weights)                                
                   GWeightVar = Variable that will hold the traffic weights                
                   GESBoundType = GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                    weights bound                            
                   GESUpperBound = GES parameter : upper bound for weights (can be null)    
                   GESLowerBound = GES parameter : lower bound for weights (can be null)    
                   GESMaxDiff = GES parameter - maximum difference (e.g. 1E-8)                
                   GESMaxIter = GES parameter - maximum number of iterations (e.g. 50)        
                   GESMaxDist = GES parameter - maximum distance (e.g. 1E-8)                
                   var_count =  Variable holding the name of the case count field            
                   var_trafficTotal = Variable holding the name of the traffic total output|;
                   var_postSum = Variable holding the name of the post traffic weight sum     
                   minCountThresh = The minimum cell count threshold
    Returns      : TODO
    Requirements : TODO
    Dependencies : TODO
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    # following code only required when connecting to Oracle database
    # Connect to Oracle and unload parameter list
    # conn = cf.get_oracle_connection()
    # global parameters
    # parameters = cf.unload_parameters(205)

    # Load SAS files into dataframes (this data will come from Oracle eventually)
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"    
    path_to_SurveyData = root_data_path + r"\survey_input.sas7bdat"
    path_to_PopTotals = root_data_path + r"\trtotals.sas7bdat" 
    
    #path_to_OutputData = root_data_path + r"\surveydata_1.sas7bdat" # not used yet
    #path_to_SummaryData = root_data_path + r"\surveydata_1.sas7bdat" # not used yet
        
    # ##########################################
    # create a SAS dataset from the survey data
    # ##########################################
    
    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_SurveyData).to_data_frame() 
    
    # This method is untested with a range of data sets but is faster
    df_surveydata = pd.read_sas(path_to_SurveyData)  
     
    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)                

    # ##########################################
    # create a SAS dataset from the population 
    # totals
    # ##########################################
    
    # Import data via SAS
    # This method works for all data sets but is slower
    #df_trtotals = SAS7BDAT(path_to_PopTotals).to_data_frame()
    
    # This method is untested with a range of data sets but is faster
    df_trtotals = pd.read_sas(path_to_PopTotals)   
    
    # Import data via SQL
    #df_trtotals = cf.get_table_values(PopTotals)  

    # upper case all column names as column names are case sensitive
    df_surveydata.columns = df_surveydata.columns.str.upper()    
    
    # These variables are passed into SAS but not required, we also pass them in for now
    summary = "summary"
    output = "output"    

    do_ips_trafweight_calculation(df_surveydata, summary, var_serialNum, var_shiftWeight, 
                                        var_NRWeight, var_minWeight,StrataDef, df_trtotals, 
                                        TotalVar, MaxRuleLength, ModelGroup, output, GWeightVar, 
                                        GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
                                        GESMaxIter, GESMaxDist, var_count, var_trafficTotal, 
                                        var_postSum, minCountThresh)

# ###################################################
# TODO - finish off macro once we have datasets
# ###################################################              

#     append the weights to the output table 
#     proc append base=&OutputData data=output force;
#     run;
#
#     df_OutputData = pd.read_sas(path_to_OutputData)
#     df_OutputData.append(df_output)    
#
#    # append summary information to summary table
#     proc append base=&summaryData data=summary force;
#     run;
#
#     df_SummaryData = pd.read_sas(path_to_SummaryData)
#     df_SummaryData.append(df_summary)
#
#    # commit the response information to the response table
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
    Parameters   : in = the IPS survey records for the period                                
                   summary = Oracle table to hold the summary data                            
                   var_serialNum = variable holding the record serial number (UID)            
                   var_shiftWeight     = variable holding the shift weight field name             
                   var_NRWeight = variable holding the non-response weight field name        
                   var_minWeight = variable holding the minimum weight field name            
                   StrataDef = List of classificatory variables                            
                   PopTotals = Population totals file                                         
                   TotalVar = Variable that holds the population totals                    
                   MaxRuleLength = maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup = Variable that will hold the model group number                
                   output = output dataset                                                 
                   GWeightVar = Variable that will hold the traffic weights                
                   GESBoundType = GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                   weights bound                            
                   GESUpperBound = GES parameter : upper bound for weights (can be null)    
                   GESLowerBound = GES parameter : lower bound for weights (can be null)    
                   ESMaxDiff = GES parameter - maximum difference (e.g. 1E-8)                
                   GESMaxIter = GES parameter - maximum number of iterations (e.g. 50)        
                   GESMaxDist = GES parameter - maximum distance (e.g. 1E-8)                
                   var_count =  Variable holding the name of the case count field            
                   var_trafficTotal = Variable holding the name of the traffic total output
                   var_postSum = Variable holding the name of the post traffic weight sum     
                   minCountThresh = The minimum cell count threshold
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

if (__name__ == '__main__'):
    calculate(SurveyData = 'sas_survey_subsample'
              ,OutputData = 'sas_traffic_wt'
              ,SummaryData = 'sas_ps_traffic'
              ,ResponseTable = 'sas_response'
              ,var_serialNum = 'serial'
              ,var_shiftWeight = 'shift_wt'
              ,var_NRWeight = 'non_response_wt'
              ,var_minWeight = 'mins_wt'
              ,StrataDef = ['samp_port_grp_pv'.upper(),
                            'arrivedepart'.upper()]                            
              ,PopTotals = 'sas_traffic_data'
              ,TotalVar = 'traffictotal'
              ,MaxRuleLength = '512'
              ,ModelGroup = 'C_group'
              ,GWeightVar = 'traffic_wt'
              ,GESBoundType = 'G'
              ,GESUpperBound = ''
              ,GESLowerBound = '1.0'
              ,GESMaxDiff = '1E-8'
              ,GESMaxIter = '50'
              ,GESMaxDist = '1E-8'
              ,var_count = 'cases'
              ,var_trafficTotal = 'traffictotal'
              ,var_postSum = 'sum_traffic_wt'
              ,minCountThresh = '30')
    
#   do_ips_trafweight_calculation(df_survey, summary, "serial", "shift_wt".upper(), 
#                                     "non_response_wt".upper(), "mins_wt".upper(),StrataDef, df_trtotals, 
#                                     "traffictotal".upper(), "512".upper(), "C_group".upper(), output, "traffic_wt".upper(), 
#                                     "G", "", "1.0", "1E-8",
#                                     "50", "1E-8", "cases", "traffictotal".upper(), 
#                                     "sum_traffic_wt".upper(), "30")
