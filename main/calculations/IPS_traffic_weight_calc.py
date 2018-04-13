import sys
import os
import logging
import inspect
import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict
import survey_support
from main.io import CommonFunctions as cf
from sas7bdat import SAS7BDAT

# TODO:
# 1. do_ips_ges_weighting() - plug-in real solution once done

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
    Returns      : tuple of dataframes - (df_output_merge_final, df_summary_merge_sum_traftot)
    Requirements : TODO
    Dependencies : do_ips_trafweight_calculation(), 
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

    # *********************  remove in future **********************
    # path_to_OutputData = root_data_path + r"\surveydata_1.sas7bdat" # not used
    # path_to_SummaryData = root_data_path + r"\surveydata_1.sas7bdat" # not used

    # ##########################################
    #
    # create a SAS dataset from the survey data
    #
    # ##########################################

    # Import data via SAS
    df_survey = SAS7BDAT(path_to_SurveyData).to_data_frame()
    # df_survey = pd.read_sas(path_to_SurveyData)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)

    # upper case all columns 
    df_survey.columns = df_survey.columns.str.upper()

    # ##############################################
    #
    # Create SAS dataset from the population totals
    #
    # ##############################################

    # Import data via SAS
    df_trtotals = SAS7BDAT(path_to_PopTotals).to_data_frame()
    # df_trtotals = pd.read_sas(path_to_PopTotals)

    # Import data via SQL
    # df_trtotals = cf.get_table_values(PopTotals)

    # upper case all column names as column names are case sensitive
    df_trtotals.columns = df_trtotals.columns.str.upper()

    # These variables are passed into SAS but not required, we also pass them in for now
    summary = None
    output = None

    (df_output_merge_final_rounded, df_summary_merge_sum_traftot) = do_ips_trafweight_calculation(df_survey, summary,
                                                                                                  var_serialNum,
                                                                                                  var_shiftWeight,
                                                                                                  var_NRWeight,
                                                                                                  var_minWeight,
                                                                                                  StrataDef,
                                                                                                  df_trtotals,
                                                                                                  TotalVar,
                                                                                                  MaxRuleLength,
                                                                                                  ModelGroup, output,
                                                                                                  GWeightVar,
                                                                                                  GESBoundType,
                                                                                                  GESUpperBound,
                                                                                                  GESLowerBound,
                                                                                                  GESMaxDiff,
                                                                                                  GESMaxIter,
                                                                                                  GESMaxDist, var_count,
                                                                                                  var_trafficTotal,
                                                                                                  var_postSum,
                                                                                                  minCountThresh)

    # test code for deleting table data before insertion
    # cf.delete_from_table('sas_traffic_wt'.upper())
    # cf.delete_from_table('sas_ps_traffic'.upper())

    # append the weights to the output table
    # cf.insert_into_table_many(OutputData, df_output_merge_final)

    # append summary information to summary table
    # cf.insert_into_table_many(SummaryData, df_summary_merge_sum_traftot)

    print("Completed - Calculate Traffic Weight")

    return (df_output_merge_final_rounded, df_summary_merge_sum_traftot)


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
    Returns      : a dataframe tuple: (df_output_merge_final, df_summary_merge_sum_traftot)
    Dependencies : do_ips_ges_weighting(), generate_ips_tw_summary
    """

    # perform calculation
    trafDesignWeight = "trafDesignWeight".upper()
    df_survey[trafDesignWeight] = df_survey[var_shiftWeight] * df_survey[var_NRWeight] * df_survey[var_minWeight]

    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\in_1.sas7bdat").to_data_frame()
    #df_test = pd.read_sas(root_data_path + r"\in_1.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_survey, df_test, check_column_type=False)
    # test code end

    # Summarise the population totals over the strata
    df_PopTotals = PopTotals.sort_values(StrataDef)

    # Re-index the data frame
    df_PopTotals.index = range(df_PopTotals.shape[0])

    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\trtotals_stratadef_sort.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\trtotals_stratadef_sort.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_PopTotals, df_test, check_column_type=False)
    # test code end

    df_popTotals = df_PopTotals.groupby(StrataDef)[TotalVar] \
        .agg([(TotalVar, 'sum')]) \
        .reset_index()

    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\poptotals_summary_1.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\poptotals_summary_1.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_popTotals, df_test, check_column_type=False)
    # test code end    

    # Call the GES weighting macro        
    CalWeight = None  # this is passed in by SAS, but probably should not be in future code

    (df_output_merge_final, df_survey_serialNum_sort) = do_ips_ges_weighting(df_survey, var_serialNum, trafDesignWeight,
                                                                             StrataDef, df_popTotals,
                                                                             TotalVar, MaxRuleLength, ModelGroup,
                                                                             output, GWeightVar, CalWeight,
                                                                             GESBoundType, GESUpperBound, GESLowerBound,
                                                                             GESMaxDiff,
                                                                             GESMaxIter, GESMaxDist)
    # test start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"

    df_test = SAS7BDAT(root_data_path + r"\output_merge_final.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\output_merge_final.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_output_merge_final, df_test, check_column_type=False)

    df_test = SAS7BDAT(root_data_path + r"\survey_serialNum_sort.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\survey_serialNum_sort.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_survey_serialNum_sort, df_test, check_column_type=False)
    # test end

    # Generate the summary table    
    df_summary_merge_sum_traftot = generate_ips_tw_summary(df_survey, df_output_merge_final, summary, StrataDef,
                                                           var_count,
                                                           var_serialNum, GWeightVar, trafDesignWeight,
                                                           var_trafficTotal, df_popTotals, var_postSum, minCountThresh)

    # test start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\summary_merge_sum_traftot.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\summary_merge_sum_traftot.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, df_test)
    # test end

    # Round the weights to 3dp    
    df_output_merge_final[GWeightVar] = df_output_merge_final[GWeightVar].apply(lambda x: round(x, 3))

    df_output_merge_final_rounded = df_output_merge_final

    # test start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\output_rounded.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\output_rounded.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_output_merge_final_rounded, df_test)
    # test end

    return (df_output_merge_final_rounded, df_summary_merge_sum_traftot)


def do_ips_ges_weighting(df_survey, var_serialNum, trafDesignWeight, StrataDef, df_popTotals,
                         TotalVar, MaxRuleLength, ModelGroup, output, GWeightVar, CalWeight,
                         GESBoundType, GESUpperBound, GESLowerBound, GESMaxDiff,
                         GESMaxIter, GESMaxDist):
    """
    Author       : Nassir Mohammad
    Date         : 08 Mar 2018
    Purpose      : Calculates GES single stage, element weighting. This is used in the IPS
                   traffic and OOH weights.
    Parameters   : Survey = the IPS survey records for the period                            
                   SerialNumVarName = variable holding the record serial number (UID)        
                   DesignWeightVarName = Variable holding the design weights                 
                   StrataDef = List of classificatory variables                            
                   PopTotals = Population totals file                                         
                   TotalVar = Variable that holds the population totals                    
                   MaxRuleLength = maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup = Variable that will hold the model group number               
                   Output = output file (holds weights)                                    
                   GWeightVar = Variable that will hold the output weights                    
                   CalWeightVar = Variable that will hold the calibration weights            
                   GESBoundType = GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                   weights bound                            
                   GESUpperBound = GES parameter : upper bound for weights (can be null)    
                   GESLowerBound = GES parameter : lower bound for weights (can be null)    
                   GESMaxDiff = GES parameter : maximum difference (e.g. 1E-8)                
                   GESMaxIter = GES parameter : maximum number of iterations (e.g. 50)        
                   GESMaxDist = GES parameter : maximum distance (e.g. 1E-8)
    Returns      : (df_output_merge_final, df_survey_serialNum_sort)
    Requirements : TODO
    Dependencies : ips_check_ges_totals(), ips_setup_ges_auxvars(), ips_assign_ges_auxiliaries(),ips_get_population_totals()
    """

    # for now we read the output dataset from disk and return the dataframe result
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"

    df_output_merge_final = SAS7BDAT(root_data_path + r"\output_merge_final.sas7bdat").to_data_frame()
    #df_output_merge_final = pd.read_sas(root_data_path + r"\output_merge_final.sas7bdat")
    df_output_merge_final.columns = df_output_merge_final.columns.str.upper()

    df_survey_serialNum_sort = SAS7BDAT(root_data_path + r"\survey_serialnum_sort.sas7bdat").to_data_frame()
    #df_survey_serialNum_sort = pd.read_sas(root_data_path + r"\survey_serialnum_sort.sas7bdat")
    df_survey_serialNum_sort.columns = df_survey_serialNum_sort.columns.str.upper()

    return (df_output_merge_final, df_survey_serialNum_sort)


def generate_ips_tw_summary(df_survey, df_output_merge_final, summary, StrataDef, var_count,
                            var_serialNum, var_trafficWeight, var_priorWeight,
                            var_trafficTotal, df_popTotals, var_postSum, minCountThresh):
    """
    Author       : Nassir Mohammad
    Date         : 08 Mar 2018
    Purpose      : Calculates IPS Traffic Weight summary
    Parameters   : Survey = survey data set                                                
                   Output = Traffic weights output                                            
                   Summary    = Summary output table
                   StrataDef = List of classificatory variables for summary                
                   var_count = Variable holding the name of the cell count field            
                   var_serialNum = Variable holding the name of the serial number field    
                   var_trafficWeight = Variable holding the name of the traffic wght field    
                   var_priorWeight = Variable holding the name of the prior (design) weight
                   var_trafficTotal = Variable holding the name of the traffic total field 
                   TrafficTotals = Traffic (population) totals dataset                        
                   var_postSum    = Variable holding the name of the post sum field            
                   minCountThresh = The minimum cell count threshold
    Returns      : dataframe - df_summary_merge_sum_traftot
    Requirements : TODO
    Dependencies : TODO
    """

    postWeight = "postWeight".upper()

    # #####################################################
    #
    # calculate the post weight
    # add the traffic weight to the survey data
    #
    # #####################################################

    df_survey_sorted = df_survey.sort_values(var_serialNum)

    # drop duplicate column (with 'None' values) across both tables before merging
    df_survey_sorted_dropped = df_survey_sorted.drop(var_trafficWeight, 1)

    # merge tables
    df_summary_tmp = df_survey_sorted_dropped.merge(df_output_merge_final, on=var_serialNum, how='outer')

    # only keep rows where var_priorWeight > 0
    df_summary_tmp = df_summary_tmp[df_summary_tmp[var_priorWeight] > 0]

    # calculate and add the post weight column
    df_summary_tmp[postWeight] = df_summary_tmp[var_priorWeight] * df_summary_tmp[var_trafficWeight]

    keep_list = [var_serialNum, StrataDef[1],
                 StrataDef[0], var_priorWeight,
                 var_trafficWeight, postWeight]

    # only keep the selected columns
    df_summary = df_summary_tmp[keep_list]

    # # test code start - either change pandas column types or for the test just ignore the column type - prefer the latter
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\_summary.sas7bdat").to_data_frame()
    #df_test = pd.read_sas(root_data_path + r"\_summary.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary, df_test, check_column_type=False)
    # # test code end

    # Summarise the results by strata
    df_summary_sorted = df_summary.sort_values(StrataDef)

    # Re-index the data frame
    df_summary_sorted.index = range(df_summary_sorted.shape[0])

    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\_summary_stratadef_sort.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\_summary_stratadef_sort.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary_sorted, df_test, check_column_type=False)
    # test code end

    # method will possibly be deprecated - may not be an issue
    df_tmp5 = df_summary_sorted.groupby(StrataDef) \
        .agg({
        postWeight: {var_count: 'count', var_postSum: 'sum'},
        var_trafficWeight: {var_trafficWeight: 'mean'},
    })

    # drop the additional column indexes
    df_tmp5.columns = df_tmp5.columns.droplevel()

    # reset indexes to keep them aligned
    df_tmp5 = df_tmp5.reset_index()

    # reorder columns for SAS comparison
    col_order = [StrataDef[0], StrataDef[1], var_count, var_postSum, var_trafficWeight]
    df_summary_varpostweight = df_tmp5[col_order]

    # test code start
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    df_test = SAS7BDAT(root_data_path + r"\summary_varpostweight.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\summary_varpostweight.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary_varpostweight, df_test, check_column_type=False, check_dtype=False)
    # test code end    

    # add in the traffic totals
    df_popTotals_stratadef_sort = df_popTotals.sort_values(StrataDef)

    # Re-index the data frame
    df_popTotals_stratadef_sort.index = range(df_popTotals_stratadef_sort.shape[0])

    # test code start
    df_test = SAS7BDAT(root_data_path + r"\popTotals_stratadef_sort.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\popTotals_stratadef_sort.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_popTotals_stratadef_sort, df_test, check_column_type=False)
    # test code end

    df_merged = pd.merge(df_popTotals_stratadef_sort, df_summary_varpostweight, on=StrataDef, how='outer')

    df_merged[var_trafficWeight] = df_merged[var_trafficWeight].apply(lambda x: round(x, 3))
    df_merged[var_postSum] = df_merged[var_postSum].apply(lambda x: round(x, 3))

    # # reorder columns for SAS comparison
    col_order = [StrataDef[0], StrataDef[1], var_count, var_trafficTotal, var_postSum, var_trafficWeight]
    df_summary_merge_sum_traftot = df_merged[col_order]
    # df_summary_merge_sum_traftot

    # test code start
    df_test = SAS7BDAT(root_data_path + r"\summary_merge_sum_traftot.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\summary_merge_sum_traftot.sas7bdat")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, df_test, check_column_type=False)
    # test code end

    # perform checks and log
    df_sum = df_summary_merge_sum_traftot
    df_sum_check = df_sum[(df_sum[var_count].isnull()) | (df_sum[var_count] < minCountThresh)]
    df_sum_check = df_sum_check[StrataDef]

    if len(df_sum_check) > 0:
        threshold_string_cap = 4000
        warningStr = "Respondent count below minimum threshold for";

        # Loop over classificatory variables
        threshold_string = ""
        for index, record in df_sum_check.iterrows():
            threshold_string += \
                warningStr + " " + StrataDef[0] + " = " + str(record[0]) \
                + " " + StrataDef[1] + "=" + str(record[1]) + "\n"

        threshold_string_capped = threshold_string[:threshold_string_cap]

        print(threshold_string_capped)
        cf.database_logger().warning("WARNING: " + threshold_string_capped)

    return df_summary_merge_sum_traftot


if (__name__ == '__main__'):
    calculate(SurveyData='sas_survey_subsample'
              , OutputData='sas_traffic_wt'.upper()
              , SummaryData='sas_ps_traffic'.upper()
              , ResponseTable='sas_response'
              , var_serialNum='serial'.upper()
              , var_shiftWeight='shift_wt'.upper()
              , var_NRWeight='non_response_wt'.upper()
              , var_minWeight='mins_wt'.upper()
              , StrataDef=['samp_port_grp_pv'.upper(),
                           'arrivedepart'.upper()]
              , PopTotals='sas_traffic_data'
              , TotalVar='traffictotal'.upper()
              , MaxRuleLength='512'
              , ModelGroup='C_group'
              , GWeightVar='traffic_wt'.upper()
              , GESBoundType='G'
              , GESUpperBound=''
              , GESLowerBound='1.0'
              , GESMaxDiff='1E-8'
              , GESMaxIter='50'
              , GESMaxDist='1E-8'
              , var_count='cases'.upper()
              , var_trafficTotal='traffictotal'.upper()
              , var_postSum='sum_traffic_wt'.upper()
              , minCountThresh=30)
