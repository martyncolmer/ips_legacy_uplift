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


def do_ips_fares_imputation(input, output, var_serial_num, var_stem, thresh_stem
                            , num_levels, donor_var, output_var, measure
                            , var_eligible_flag, var_imp_flag,var_imp_level
                            , var_fare_age, var_baby_fare, var_child_fare, var_apd
                            , var_package, var_fare_discount, var_QMfare
                            , var_package_cost, var_discounted_package_cost
                            , var_persons, var_expenditure, var_befaf, var_spend
                            , var_spend_reason_key, var_duty_free, var_old_package):
    """
    Author       : James Burr
    Date         : 19 Feb 2018
    Purpose      : Imputes fares for the IPS system.
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
                   var_fare_age - the fare age field name
                   var_baby_fare - the baby fare factor field name
                   var_child_fare - the child fare factor field name
                   var_apd - the airport duty field name
                   var_package - the package type field name
                   var_fare_discount - the fare discount factor field name
                   var_QMFare - the Queen Mary Fare field name
                   var_package_cost - the package cost field name
                   var_discounted_package_cost - the discounted package factor field
                   var_persons - the number of persons field name
                   var_expenditure - the expenditure field name
                   var_befaf - the before/after field name
                   var_spend - the spend field name
                   var_spend_reason_key - field indicating that spend should be 
                                          imputed
                   var_duty_free - the duty free field name
    Returns      : Dataframe - df_output_final
    Requirements : NA
    Dependencies : NA
    """
    df_input = input
    
    # Setup thresh and strata base nested lists
    strata_base_list = [['INTMONTH', 'TYPE_PV', 'UKPORT1_PV', 'OSPORT1_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT2_PV', 'OSPORT1_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT1_PV', 'OSPORT2_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT2_PV', 'OSPORT2_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT3_PV', 'OSPORT2_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT3_PV', 'OSPORT3_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT4_PV', 'OSPORT3_PV', 'OPERA_PV']
                        ,['INTMONTH', 'TYPE_PV', 'UKPORT4_PV', 'OSPORT4_PV']
                        ,['INTMONTH', 'TYPE_PV', 'OSPORT4_PV']]
    
    thresh_base_list = [3, 3, 3, 3, 3, 3, 3, 0, 0]
    
    # Ensure imputation only occurs on eligible rows
    df_eligible = df_input.where(df_input[var_eligible_flag] == True)
    
    df_output = ips_impute.ips_impute(df_eligible, output, var_serial_num
                                            , strata_base_list, thresh_base_list
                                            , num_levels, donor_var,output_var
                                            , measure, var_imp_flag
                                            , var_imp_level)
    
    # Merge df_output_final and df_input by var_serial_num
    df_output.sort_values(var_serial_num, inplace = True)
    print(df_output)
    sys.exit()
    
    # Compute spend based on fare
    df_compute = df_output[[var_serial_num, var_spend
                                       , var_spend_reason_key, output_var 
                                       , var_imp_level]]
    
    if df_output[var_imp_flag] or df_output[var_eligible_flag] == True:
        df_compute[output_var] = donor_var
    
    # Sort out baby/child fares
    
    
    # Round output column to nearest integer
    df_output[output_var] = df_output[output_var].round()
    
    return df_output


def ips_fares_imp(SurveyData, OutputData, ResponseTable, var_serial_num, varStem
                  , threshStem, num_levels, donor_var, output_var, measure, var_eligible_flag
                  , var_imp_flag, var_imp_level, var_fare_age, var_baby_fare, var_child_fare
                  , var_apd, var_package, var_fare_discount, var_QM_fare, var_package_cost
                  , var_discounted_package_cost, var_persons, var_expenditure, var_befaf
                  , var_spend, var_spend_reason_key, var_duty_free, var_old_package):
    """
    Author       : James Burr
    Date         : 19 Feb 2018
    Purpose      : Imputes fares for IPS system
    Parameters   : SurveyData - the IPS survey dataset
                   OutputData - the output
                   var_serial_num - the serial number field name
                   var_stem - stem of the imputation variables parameters
                   thresh_stem - stem of the imputation threshold parameters
                   num_levels - number of imputation levels
                   donor_var - Name of the donor variable
                   output_var - Name of the output variable
                   measure - Measure function (e.g. mean)
                   var_eligible_flag - the imputation eligibility (donor+recipient) field name
                   var_imp_flag - the imputation required trigger/flag field name
                   var_imp_fevel - the imputation level field name
                   var_fare_age - the fare age field name
                   var_baby_fare - the baby fare factor field name
                   var_child_fare - the child fare factor field name
                   var_apd - the airport duty field name
                   var_package - the package type field name
                   var_fare_discount - the fare discount factor field name
                   var_QMFare - the Queen Mary Fare field name
                   var_package_cost - the package cost field name
                   var_discounted_package_cost - the discounted package factor field
                   var_persons - the number of persons field name
                   var_expenditure - the expenditure field name
                   var_befaf - the before/after field name
                   var_spend - the spend field name
                   var_spend_reason_key - field indicating that spend should be imputed
                   var_duty_free - the duty free field name    
    Returns      : Dataframe containing the imputed records
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Fares Imputation"
    
    # This commented out to be changed when data is availabe for this step
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"
    
    # Create dataframe to store output
    global df_surveydata
    
    # Import data
    # This method is untested with a range of data sets but is faster
    df_surveydata = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)
    
    df_surveydata.columns = df_surveydata.columns.str.upper()
    
    print("Start - Calculate Fares Imputation")
    
    df_output = do_ips_fares_imputation(df_surveydata, OutputData, var_serial_num
                                      , varStem, threshStem, num_levels, donor_var
                                      , output_var, measure, var_eligible_flag
                                      , var_imp_flag,var_imp_level, var_fare_age
                                      , var_baby_fare, var_child_fare, var_apd
                                      , var_package, var_fare_discount, var_QM_fare
                                      , var_package_cost, var_discounted_package_cost
                                      , var_persons, var_expenditure, var_befaf
                                      , var_spend, var_spend_reason_key
                                      , var_duty_free, var_old_package)
    
    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData, df_output)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Fares Imputation calculation: %s()" %function_name
    
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Fares Imputation.")
    cf.commit_to_audit_log("Create", "FaresImputation", audit_message)
    
    
    
    print("Completed - Calculate Fares Imputation")
    
    
if __name__ == '__main__':
    ips_fares_imp(SurveyData = 'SAS_SURVEY_SUBSAMPLE',OutputData = 'SAS_FARES_IMP'
                 ,ResponseTable = 'SAS_RESPONSE', var_serial_num = 'SERIAL'
                 ,varStem = 'VARS',threshStem = 'THRESH',num_levels = 9
                 ,donor_var = 'DVFARE',output_var = 'FARE',measure = 'mean'
                 ,var_eligible_flag = 'FARES_IMP_ELIGIBLE_PV'
                 ,var_imp_flag = 'FARES_IMP_FLAG_PV',var_imp_level = 'FAREK'
                 ,var_fare_age = 'FAGE_PV', var_baby_fare = 'BABYFARE'
                 , var_child_fare = 'CHILDFARE', var_apd = 'APD_PV'
                 , var_package = 'DVPACKAGE', var_fare_discount = 'DISCNT_F2_PV'
                 ,var_QM_fare = 'QMFARE_PV', var_package_cost = 'DVPACKCOST'
                 ,var_discounted_package_cost = 'DISCNT_PACKAGE_COST_PV'
                 , var_persons = 'DVPERSONS', var_expenditure = 'DVEXPEND'
                 ,var_befaf = 'BEFAF', var_spend = 'SPEND'
                 , var_spend_reason_key = 'SPENDIMPREASON', var_duty_free = 'DUTY_FREE_PV'
                 , var_old_package = 'PACKAGE')