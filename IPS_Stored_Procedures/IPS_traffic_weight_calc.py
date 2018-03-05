import sys

# for jupyter notebooks
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
# 2. do_ips_ges_weighting()
# 3. generate_ips_tw_summary()
# 4. do the testing of the functions
# Note use SAS7BDAT(path_to_SurveyData).to_data_frame()
            
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
    df_survey = SAS7BDAT(path_to_SurveyData).to_data_frame()
    
    # This method is untested with a range of data sets but is faster
    # seems to produce error results when comparing with SAS input/output
    #df_survey = pd.read_sas(path_to_SurveyData)
              
    # upper case all columns 
    df_survey.columns = df_survey.columns.str.upper() 
     
    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)                

    # ##########################################
    # create a SAS dataset from the population 
    # totals
    # ##########################################
    
    # Import data via SAS
    # This method works for all data sets but is slower
    df_trtotals = SAS7BDAT(path_to_PopTotals).to_data_frame()
    
    # This method is untested with a range of data sets but is faster
    #df_trtotals = pd.read_sas(path_to_PopTotals)   
    
    # Import data via SQL
    #df_trtotals = cf.get_table_values(PopTotals)  

    # upper case all column names as column names are case sensitive
    df_trtotals.columns = df_trtotals.columns.str.upper()    
    
    # These variables are passed into SAS but not required, we also pass them in for now
    summary = "summary"
    output = "output"    

    df_traf = do_ips_trafweight_calculation(df_survey, summary, var_serialNum, var_shiftWeight, 
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
    
    # perform calculation
    trafDesignWeight = "trafDesignWeight"#.upper()        
    df_survey[trafDesignWeight] = df_survey[var_shiftWeight] * df_survey[var_NRWeight] * df_survey[var_minWeight]
    
    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_in_1 = SAS7BDAT(root_data_path + r"\in_1.sas7bdat").to_data_frame()
    df_in_1_renamed = df_in_1.rename(columns={trafDesignWeight: trafDesignWeight}) #rename column so that dtype is not inferred
    assert_frame_equal(df_survey, df_in_1_renamed) # these are equal
    # test code end

    # Summarise the population totals over the strata
    df_PopTotals = PopTotals.sort_values(StrataDef)
    
    # Re-index the data frame
    df_PopTotals.index = range(df_PopTotals.shape[0])
       
    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_trtotals_stratadef_sort = SAS7BDAT(root_data_path + r"\trtotals_stratadef_sort.sas7bdat" ).to_data_frame()
    assert_frame_equal(df_PopTotals, df_trtotals_stratadef_sort)
    # test code end

    df_popTotals = df_PopTotals.groupby(StrataDef)[TotalVar]\
                        .agg([('traffictotal', 'sum')]) \
                        .reset_index()
    
    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_poptotals_summary_1 = SAS7BDAT(root_data_path + r"\poptotals_summary_1.sas7bdat" ).to_data_frame()
    assert_frame_equal(df_popTotals, df_poptotals_summary_1)
    # test code end
    print("Nassir")
            
    # Call the GES weighting macro        
    CalWeight = None # this is passed in by SAS, but probably should not be in future code
     
    # todo: check return
    df_output_merge_final = do_ips_ges_weighting(df_survey, var_serialNum, trafDesignWeight, StrataDef, df_popTotals,
                                                 TotalVar, MaxRuleLength, ModelGroup, output, GWeightVar, CalWeight,
                                                 GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
                                                 GESMaxIter, GESMaxDist) 
     
    # todo: check return
    # Generate the summary table    
    df_output = generate_ips_tw_summary(df_survey, output, summary, StrataDef, var_count,
                                 var_serialNum, GWeightVar, trafDesignWeight,
                                 var_trafficTotal, df_popTotals, var_postSum, minCountThresh)
     
    # to do: check return
    # Round the weights to 3dp
    #df_output =  df_output[GWeightVar] = round(GWeightVar, 3)
    
    return df_output

# ----------------------------------
#
# do_ips_ges_weighting()
#
# ----------------------------------
def do_ips_ges_weighting(df_survey, var_serialNum, trafDesignWeight, StrataDef, df_popTotals,
                         TotalVar, MaxRuleLength, ModelGroup, output, GWeightVar, CalWeight,
                         GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
                         GESMaxIter, GESMaxDist): 
    
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_output_merge_final = SAS7BDAT(root_data_path + r"\output_merge_final.sas7bdat").to_data_frame()
    df_output_merge_final.columns = df_output_merge_final.columns.str.upper() 
    return df_output_merge_final

    # do_ips_ges_weighting calls the following functions:

    # ips_check_ges_totals()
    # ips_setup_ges_auxvars()
    # ips_assign_ges_auxiliaries()
    # ips_get_population_totals()

# ----------------------------------
#    
# generate_ips_tw_summary()
#
# --------------------------------- 
def generate_ips_tw_summary(df_survey, output, summary, StrataDef, var_count,
                            var_serialNum, GWeightVar, trafDesignWeight,
                            var_trafficTotal, df_popTotals, var_postSum, minCountThresh):
    
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_output_merge_final = SAS7BDAT(root_data_path + r"\output_merge_final.sas7bdat").to_data_frame()
    df_output_merge_final.columns = df_output_merge_final.columns.str.upper() 
    return df_output_merge_final

if (__name__ == '__main__'):
    calculate(SurveyData = 'sas_survey_subsample'
              ,OutputData = 'sas_traffic_wt'
              ,SummaryData = 'sas_ps_traffic'
              ,ResponseTable = 'sas_response'
              ,var_serialNum = 'serial'.upper()
              ,var_shiftWeight = 'shift_wt'.upper()
              ,var_NRWeight = 'non_response_wt'.upper()
              ,var_minWeight = 'mins_wt'.upper()
              ,StrataDef = ['samp_port_grp_pv'.upper(),
                            'arrivedepart'.upper()]                            
              ,PopTotals = 'sas_traffic_data'
              ,TotalVar = 'traffictotal'.upper()
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

# delete later
#   do_ips_trafweight_calculation(df_survey, summary, "serial", "shift_wt".upper(), 
#                                     "non_response_wt".upper(), "mins_wt".upper(),StrataDef, df_trtotals, 
#                                     "traffictotal".upper(), "512".upper(), "C_group".upper(), output, "traffic_wt".upper(), 
#                                     "G", "", "1.0", "1E-8",
#                                     "50", "1E-8", "cases", "traffictotal".upper(), 
#                                     "sum_traffic_wt".upper(), "30")
