import sys
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

# Place holder function. This is being used until the actual GES weighting function is complete.
def do_ips_ges_weighting(input, SerialNumVarName, DesignWeightVarName, 
                      StrataDef, PopTotals, TotalVar, MaxRuleLength, 
                      ModelGroup, Output, GWeightVar, CalWeightVar, 
                      GESBoundType, GESUpperBound, GESLowerBound, 
                      GESMaxDiff, GESMaxIter, GESMaxDist):
    pass


def do_ips_unsampled_weight_calculation(inn, summary, var_serialNum, var_shiftWeight, var_NRWeight,
                                    var_minWeight, var_trafficWeight, OOHStrataDef, PopTotals, 
                                    var_totals, MaxRuleLength, var_modelGroup, output, 
                                    var_OOHWeight, var_caseCount, var_priorWeightSum, 
                                    var_OOHWeightSum, GESBoundType, GESUpperBound, GESLowerBound, 
                                    GESMaxDiff, GESMaxIter, GESMaxDist, minCountThresh):  
    """
    Author       : Thomas Mahoney
    Date         : 16 / 02 / 2018
    Purpose      : Performs calculations to determine the unsampled weight values
                   of the imported dataset.
    Parameters   : inn - the IPS survey records for the period                                
                   summary - Oracle table to hold the summary data                            
                   var_serialNum - variable holding the record serial number (UID)            
                   var_shiftWeight - variable holding the shift weight field name                
                   var_NRWeight - variable holding the non-response weight field name        
                   var_minWeight - variable holding the minimum weight field name            
                   var_trafficWeight - variable holding the traffic weight field name        
                   OOHStrataDef - List of classificatory variables                            
                   PopTotals - Population totals file                                         
                   var_totals - Variable that holds the population totals                    
                   MaxRuleLength - maximum length of an auxiliary rule (e.g. 512)            
                   var_modelGroup - Variable that will hold the model group number            
                   output - output dataset                                                 
                   var_OOHWeight - Variable that will hold the OOH weights                    
                   var_caseCount -  Variable holding the name of the case count field        
                   var_priorWeightSum - Variable that will hold the prior weight summary    
                   var_OOHWeightSum - Variable that will hold the post weight sum            
                   GESBoundType - GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                   weights bound                            
                   GESUpperBound - GES parameter : upper bound for weights (can be null)    
                   GESLowerBound - GES parameter : lower bound for weights (can be null)    
                   GESMaxDiff - GES parameter : maximum difference (e.g. 1E-8)                
                   GESMaxIter - GES parameter : maximum number of iterations (e.g. 50)        
                   GESMaxDist - GES parameter : maximum distance (e.g. 1E-8)                
                   minCountThresh - The minimum cell count threshold                        
    Returns      : df_summary(dataframe containing random sample of rows)
                   df_output(dataframe containing serial number and calculated unsampled weight)
    Requirements : NA
    Dependencies : NA
    
    
    NOTES        : Currently GES weighing has not been written. Therefore the current solution
                   does not generate the output data frame. Once the function is written and we
                   are aware of what is being returned from the GES weighting function as well
                   as what is actually needed to be *sent* passed to the function we will rewrite the 
                   function call and implement its return functionality 
                   be rewriting the 
    """

    # Create new column for design weights (Generate the design weights)
    OOHDesignWeight = 'OOHDESIGNWEIGHT'
    inn[OOHDesignWeight] = \
        inn[var_shiftWeight] * inn[var_NRWeight] * inn[var_minWeight] * inn[var_trafficWeight];
    
    
    # Summarise the uplift totals over the strata
    PopTotals = PopTotals.sort_values(by = OOHStrataDef)        
    popTotals = PopTotals.groupby(OOHStrataDef)[var_totals].agg({'UPLIFT' : 'sum'})
    
    
    # Summarise the previous totals over the strata
    # Only use values where the OODesignWeight is greater than zero
    inn = inn.sort_values(by = OOHStrataDef)
    prevTotals = inn.loc[inn[OOHDesignWeight] > 0]
    prevTotals = prevTotals.groupby(OOHStrataDef)[OOHDesignWeight].agg({'PREVTOTAL' : 'sum'})
    
    popTotals = popTotals.sort_values(by = OOHStrataDef)
    
    # Generate the lifted totals data set from the two sets created
    liftedTotals = pd.merge(prevTotals,popTotals,on = OOHStrataDef, how = 'left')
    
    # Fill blank uplift and prevtotal values with zero
    liftedTotals['UPLIFT'].fillna(0, inplace=True)
    liftedTotals['PREVTOTAL'].fillna(0, inplace=True)
    
    # Calculate the totals column from the prevtotal and uplift values
    liftedTotals[var_totals] = liftedTotals['PREVTOTAL'] + liftedTotals['UPLIFT']
    
    # Remove any records where var_totals value is not greater than zero
    liftedTotals = liftedTotals[liftedTotals[var_totals] > 0]
      
    # Call the GES weighting macro
    do_ips_ges_weighting(inn, var_serialNum, OOHDesignWeight, 
                      OOHStrataDef, liftedTotals, var_totals, MaxRuleLength, 
                      var_modelGroup, output, var_OOHWeight, 'CalWeight', GESBoundType, 
                      GESUpperBound, GESLowerBound, GESMaxDiff, GESMaxIter, GESMaxDist)
    
    # Sort inn dataframe before merge
    inn = inn.sort_values(by = var_serialNum)
    

    # Merge the inn and output data frame to generate the summary table
    inn = pd.merge(inn,output, on = [var_serialNum,var_OOHWeight], how = 'left')
    
    # Fill blank UNSAMP_TRAFFIC_WT values with 1.0
    inn[var_OOHWeight].fillna(1.0, inplace=True)
    
    # Generate POSTWEIGHT values from the UNSAMP_TRAFFIC_WT and OOHDesignWeight values
    inn['POSTWEIGHT'] = inn[var_OOHWeight] + inn[OOHDesignWeight]    
    inn = inn.sort_values(by = OOHStrataDef)
    
    # Create the summary data frame from the sample with OOHDesignWeight not equal to zero
    df_summary = inn[inn[OOHDesignWeight] != 0]
    df_summary = df_summary.groupby(OOHStrataDef)[var_OOHWeight].agg({\
            OOHDesignWeight : 'sum' ,'POSTWEIGHT' : 'sum', var_OOHWeight : 'mean'})
    
    # Flattens the column structure after adding the new OOHDesignWeight and POSTWEIGHT columns
    df_summary = df_summary.reset_index()
    df_summary = df_summary.rename(columns = {OOHDesignWeight : var_priorWeightSum,
                                              'POSTWEIGHT' : var_OOHWeightSum})
    
        
    # Identify groups where the total has been uplifted but the
    # respondent count is below the threshold.
    
    # Create unsampled data set for rows outside of the threshold
    df_unsampled_thresholds_check = \
        df_summary[(df_summary[var_OOHWeightSum] > df_summary[var_priorWeightSum]) \
                   & (df_summary[var_caseCount] < df_summary[minCountThresh])]
    
    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_unsampled_thresholds_check.iterrows():
        threshold_string += "___||___" \
                         + df_unsampled_thresholds_check.columns[0] + " : " + str(record[0]) + " | "\
                         + df_unsampled_thresholds_check.columns[1] + " : " + str(record[1]) + " | "\
                         + df_unsampled_thresholds_check.columns[2] + " : " + str(record[2]) + " | "\
                         + df_unsampled_thresholds_check.columns[3] + " : " + str(record[3])
                         
    # Output the values outside of the threshold to the logger
    if len(df_unsampled_thresholds_check) > 0:
        cf.database_logger().warning('WARNING: Respondent count below minimum threshold for: ' + threshold_string)
    
    
    # Return the generated data frames to be appended to oracle
    return (output, df_summary)


def calculate(SurveyData, OutputData, SummaryData, ResponseTable, var_serialNum, 
              var_shiftWeight, var_NRWeight, var_minWeight, var_trafficWeight,
              OOHStrataDef, PopTotals, var_totals, MaxRuleLength, ModelGroup,
              var_modelGroup, var_OOHWeight, GESBoundType, GESUpperBound,
              GESLowerBound, GESMaxDiff, GESMaxIter, GESMaxDist, var_caseCount,
              var_OOHWeightSum, var_priorWeightSum, minCountThresh):
    """
    Author       : Thomas Mahoney
    Date         : 16 / 02 / 2018
    Purpose      : Imports the required data sets for performing the unsampled
                   weight calculation. This function also triggers the unsmapled
                   weight calculation function using the imported data. Once 
                   complete it will append the newly generated data frames to the 
                   specified oracle database tables. 
    Parameters   : SurveyData - the IPS survey records for the period                             
                   SummaryData - the weighting summary output table                        
                   responseTable - Oracle table to hold response information (status etc.)    
                   var_serialNum - variable holding the record serial number (UID)            
                   var_shiftWeight - variable holding the shift weight field name                
                   var_NRWeight - variable holding the non-response weight field name        
                   var_minWeight - variable holding the minimum weight field name            
                   var_trafficWeight - variable holding the traffic weight field name        
                   var_designWeight - Variable holding the design weight field name         
                   OOHStrataDef - List of classificatory variables                            
                   PopTotals - OOH population totals file                                    
                   var_totals - Variable that holds the population totals                    
                   MaxRuleLength - maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup - Model definition file                                    
                   var_modelGroup - Variable that will hold the model group number            
                   OutputData - output file (holds weights)                                
                   var_OOHWeight - Variable that will hold the OOH weight                     
                   var_caseCount - Variable that will hold the number of cases by strata     
                   var_priorWeightSum - Variable that will hold the prior weight summary    
                   var_OOHWeightSum - Variable that will hold the post weight sum            
                   GESBoundType - GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                    weights bound                            
                   GESUpperBound - GES parameter : upper bound for weights (can be null)    
                   GESLowerBound - GES parameter : lower bound for weights (can be null)    
                   GESMaxDiff - GES parameter : maximum difference (e.g. 1E-8)                
                   GESMaxIter - GES parameter : maximum number of iterations (e.g. 50)        
                   GESMaxDist - GES parameter : maximum distance (e.g. 1E-8)                
                   minCountThresh - The minimum cell count threshold                        
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    global survey
    global ustotals  

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    #df_nonresponsedata = SAS7BDAT(path_to_nonresponse_data).to_data_frame()
    # This method is untested with a range of data sets but is faster
    #df_surveydata = pd.read_sas(path_to_survey_data)
    #df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)
    
    # Import data via SQL
    survey = cf.get_table_values(SurveyData)
    ustotals = cf.get_table_values(PopTotals)
    
    survey.columns = survey.columns.str.upper()
    ustotals.columns = ustotals.columns.str.upper()


    # Calculate the unsampled weights of the imported dataset.
    print("Start - Calculate UnSampled Weight.")     
    weight_calculated_dataframes = do_ips_unsampled_weight_calculation(survey, 'summary', var_serialNum, var_shiftWeight, var_NRWeight, 
                                        var_minWeight, var_trafficWeight, OOHStrataDef, ustotals, 
                                        var_totals, MaxRuleLength, var_modelGroup, 'output', 
                                        var_OOHWeight, var_caseCount, var_priorWeightSum, 
                                        var_OOHWeightSum, GESBoundType, GESUpperBound, GESLowerBound, 
                                        GESMaxDiff, GESMaxIter, GESMaxDist, minCountThresh)
    
    # Extract the two data sets returned from do_ips_nrweight_calculation
    output_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]
     
    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData, output_dataframe)
    cf.insert_into_table_many(SummaryData, summary_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load UnSampled Weight calculation: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed UnSampled weight calculation.")
    cf.commit_to_audit_log("Create", "UnSampled", audit_message)
    print("Completed - Calculate UnSampled Weight")
    

if __name__ == '__main__':
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE',
              OutputData = 'SAS_UNSAMPLED_OOH_WT', 
              SummaryData = 'SAS_PS_UNSAMPLED_OOH', 
              ResponseTable = 'SAS_RESPONSE', 
              var_serialNum = 'SERIAL', 
              var_shiftWeight = 'SHIFT_WT', 
              var_NRWeight = 'NON_RESPONSE_WT', 
              var_minWeight = 'MINS_WT',
              var_trafficWeight = 'TRAFFIC_WT',
              OOHStrataDef = ['UNSAMP_PORT_GRP_PV', 
                              'UNSAMP_REGION_GRP_PV',
                              'ARRIVEDEPART'],
              PopTotals = 'SAS_UNSAMPLED_OOH_DATA',
              var_totals = 'UNSAMP_TOTAL',
              MaxRuleLength = '512',
              ModelGroup = 'C_GROUP',
              var_modelGroup = 'C_GROUP',
              var_OOHWeight = 'UNSAMP_TRAFFIC_WT',
              GESBoundType = 'G',
              GESUpperBound = '',
              GESLowerBound = '1.0',
              GESMaxDiff = '1E-8',
              GESMaxIter = '50',
              GESMaxDist = '1E-8',
              var_caseCount = 'CASES',
              var_OOHWeightSum = 'SUM_UNSAMP_TRAFFIC_WT',
              var_priorWeightSum = 'SUM_PRIOR_WT',
              minCountThresh = '30')