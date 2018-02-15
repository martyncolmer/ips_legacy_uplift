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
from IPS_Unallocated_Modules import ips_impute



def do_ips_stay_imputation(input, output, var_serialNum, varStem, threshStem,
                            numLevels, donorVar, outputVar, measure,
                            var_eligibleFlag, var_impFlag,var_impLevel):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
    Purpose      : Imputes stay for the IPS system.
    Parameters   : input - the IPS survey dataset.
                   output - dataframe holding imputed records
                   var_serialNum - the serial number field name
                   var_stem - stem of the imputation variables parameters
                   num_levels - number of imputation levels
                   donor_var - name of the donor variable
                   output_var - name of the output variable
                   measure - measure function, such as mean
                   var_eligible_flag - the imputation eligibility field name for
                   both donor and recipient
                   var_imp_flag - the imputation required trigger/flag field name
                   var_imp_level - the imputation level field name
    Returns      : Dataframe - df_output_final
    Requirements : NA
    Dependencies : NA
    """
    df_input = input
    
    # Ensure imputation only occurs on eligible rows
    df_eligible = df_input.where(df_input['var_eligibleFlag'] == True)
    
    df_output_final = ips_impute.ips_impute(df_eligible, output, var_serialNum
                                            , varStem, threshStem, numLevels
                                            , donorVar,outputVar, measure
                                            , var_impFlag, var_impLevel)
    
    # Round output column to nearest integer
    df_output_final['output_var'] = df_output_final['output_var'].round()
    
    return df_output_final


def ips_stay_imp(SurveyData,OutputData,ResponseTable,var_serialNum,varStem
                 ,threshStem,numLevels,donorVar,outputVar,measure
                 ,var_eligibleFlag,var_impFlag,var_impLevel):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
    Purpose      : Generates segments for use within IPS imputation.
    Parameters   : input - dataframe holding both donors and recipients
                   output - dataframe holding imputed records
                   var_serialNum - variable holding the serial number
                   strata_base - stem for the strata variables
                   thresh_base - stem for the threshold variables
                   num_levels - number of imputation levels (run from 0 to num-1)
                   impute_var - variable to be imputed
                   var_value - variable holding the name of the output value field
                   impute_function - imputation cluster measure function (e.g. mean)
                   var_impute_flag - name of the imputation flag variable
                   var_impute_level - name of the imputation level output field
    Returns      : Dataframe containing the imputed records
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_IPS_Shift_Weight"
    
    # This commented out to be changed when data is availabe for this step
    #path_to_survey_data = root_data_path + r"\surveydatasmall.sas7bdat"
    
    # Create dataframe to store output
    global df_surveydata
    
    # Import data via SQL
    df_surveydata = cf.get_table_values(SurveyData)
    
    df_surveydata.columns = df_surveydata.columns.str.upper()
    
    print("Start - Calculate Stay Imputation")
    df_output = do_ips_stay_imputation(SurveyData, OutputData, var_serialNum
                                       , varStem, threshStem, numLevels, donorVar
                                       , outputVar, measure, var_eligibleFlag
                                       , var_impFlag, var_impLevel)
    
    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData, df_output)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Stay Imputation calculation: %s()" %function_name
    
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Stay imputation.")
    cf.commit_to_audit_log("Create", "StayImputation", audit_message)
    
    print("Completed - Calculate Stay Imputation")
    
    
if __name__ == '__main__':
    ips_stay_imp(SurveyData = 'SAS_SURVEY_SUBSAMPLE',OutputData = 'SAS_STAY_IMP'
                 ,ResponseTable = 'SAS_RESPONSE',var_serialNum = 'SERIAL'
                 ,varStem = 'VARS',threshStem = 'THRESH',numLevels = 1
                 ,donorVar = 'NUMNIGHTS',outputVar = 'STAY',measure = 'MEAN'
                 ,var_eligibleFlag = 'STAY_IMP_ELIGIBLE_PV'
                 ,var_impFlag = 'STAY_IMP_FLAG_PV',var_impLevel = 'STAYK')
    
    