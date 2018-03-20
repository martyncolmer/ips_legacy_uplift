import sys
import inspect
import pandas as pd
import survey_support
from IPSTransformation import CommonFunctions as cf
from IPS_Unallocated_Modules import ips_impute

# Measure should be passed through in LOWER CASE. This variable should contain
# a function name which can only be used in lower case.
def do_ips_stay_imputation(df_input, output, var_serial_num, var_stem, thresh_stem,
                            num_levels, donor_var, output_var, measure,
                            var_eligible_flag, var_imp_flag,var_imp_level):
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
    # Ensure imputation only occurs on eligible rows
    df_eligible = df_input.where(df_input[var_eligible_flag] == True)
    
    strata_base_list = [['STAYIMPCTRYLEVEL4_PV', 'STAY_PURPOSE_GRP_PV']]
    
    thresh_base_list = [[1]]
    
    df_output_final = ips_impute.ips_impute(df_eligible, var_serial_num
                                            , strata_base_list, thresh_base_list
                                            , num_levels, donor_var,output_var
                                            , measure, var_imp_flag, var_imp_level)
    
    # Round output column to nearest integer
    df_output_final[output_var] = df_output_final[output_var].round()
    
    return df_output_final


def ips_stay_imp(SurveyData,OutputData,ResponseTable,var_serialNum,varStem
                 ,threshStem,numLevels,donorVar,outputVar,measure
                 ,var_eligibleFlag,var_impFlag,var_impLevel):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
    Purpose      : Produces imputed values for step 8, stay_imputation
    Parameters   : SurveyData - the IPS survey dataset
                   OutputData - the output
                   ResponseTable - the SAS response table
                   var_serialNum - the serial number field name
                   varStem - stem of the imputation variables parameters
                   threshStem - stem of the imputation threshold parameters
                   numLevels - number of imputation levels
                   donorVar - Name of the donor variable
                   outputVar - Name of the output variable    
                   var_eligibleFlag - the imputation eligibility (donor+recipient) field name
                   var_impFlag - the imputation required trigger/flag field name
                   var_impLevel - the imputation level field name
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Stay Imputation"
    
    # This commented out to be changed when data is availabe for this step
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"
    
    # Import data
    df_surveydata = pd.read_sas(path_to_survey_data)
    
    df_surveydata.columns = df_surveydata.columns.str.upper()

    df_output = do_ips_stay_imputation(df_surveydata, OutputData, var_serialNum
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
    
    
if __name__ == '__main__':
    ips_stay_imp(SurveyData = 'SAS_SURVEY_SUBSAMPLE',OutputData = 'SAS_STAY_IMP'
                 ,ResponseTable = 'SAS_RESPONSE',var_serialNum = 'SERIAL'
                 ,varStem = 'VARS',threshStem = 'THRESH',numLevels = 1
                 ,donorVar = 'NUMNIGHTS',outputVar = 'STAY',measure = 'mean'
                 ,var_eligibleFlag = 'STAY_IMP_ELIGIBLE_PV'
                 ,var_impFlag = 'STAY_IMP_FLAG_PV',var_impLevel = 'STAYK')
    