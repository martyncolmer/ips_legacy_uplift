'''
Created on 7 Mar 2018

@author: thorne1
'''
import inspect
import numpy as np
import pandas as pd
import survey_support

from main.io import CommonFunctions as cf
from main.calculations import ips_impute as imp

def do_ips_spend_imputation(df_survey_data, OutputData, var_serialNum, varStem
                            , threshStem, numLevels, donorVar, outputVar
                            , measure, var_eligibleFlag, var_impFlag
                            , var_impLevel, var_stay):
    """
    Author        : thorne1
    Date          : 7 Mar 2018
    Purpose       : Imputes spend for IPS system  
    Parameters    : df_survey_data = the IPS survey dataset
                    OutputData = the output
                    var_serialNum = the serial number field name
                    varStem = stem of the imputation variables parameters
                    threshStem = stem of the imputation threshold parameters
                    numLevels = number of imputation levels
                    donorVar = Name of the donor variable
                    outputVar = Name of the output variable
                    measure = Measure function (e.g. mean)
                    var_eligibleFlag = the imputation eligibility (donor+recipient) field name
                    var_impFlag = the imputation required trigger/flag field name
                    var_impLevel = the imputation level field name
                    var_stay = the stay variable field name
    Returns       : Dataframe  
    """
    
    # Select only the eligible donors and recipients
    df_eligible = df_survey_data.copy()
    df_eligible["STAYDAYS"] = np.where(df_eligible[var_eligibleFlag] == 1.0
                                          , (df_eligible[var_stay] + 1.0), np.NaN)
    df_eligible.drop(df_eligible[df_eligible[var_eligibleFlag] != 1.0].index
                     , inplace=True)
    
    def selection(row):
        if row[var_impFlag] != 1.0:
            if (row[donorVar] > 0) & (row["STAYDAYS"] > 0):
                row["XPD"] = row[donorVar] / row["STAYDAYS"]
            elif row[donorVar] == 0:
                row["XPD"] = 0
            else:
                row[var_impFlag] = 1 
        return row
    
    df_eligible = df_eligible.apply(selection, axis = 1)

    # Perform the imputation
    df_output = imp.ips_impute(df_eligible, var_serialNum
                               , varStem, threshStem, numLevels, "XPD"
                               , outputVar, measure, var_impFlag, var_impLevel)
    
    # Merge and cleanse
    df_final_output = pd.merge(df_eligible, df_output, on=var_serialNum
                               , how = 'left')
    df_final_output.drop(var_impLevel + "_x", axis=1, inplace=True)
    df_final_output.rename(columns={var_impLevel + "_y": var_impLevel}, inplace = True)
    
    # Create final output with required columns 
    df_final_output = df_final_output[[var_serialNum, outputVar, var_impLevel
                                       , "STAYDAYS"]]
    df_final_output.loc[df_final_output[var_impLevel].notnull()
                       , outputVar] = (df_final_output[outputVar] 
                                                * df_final_output["STAYDAYS"])
    df_final_output[outputVar] =  df_final_output[outputVar].apply(lambda x: round(x, 0))
    
    # Cleanse df before returning
    df_final_output = df_final_output[[var_serialNum, var_impLevel, outputVar]]
    
    return df_final_output

    
def calculate(SurveyData, OutputData, var_serialNum, varStem, threshStem
              , numLevels, donorVar, outputVar, measure, var_eligibleFlag
              , var_impFlag, var_impLevel, var_stay):
    """
    Author        : thorne1
    Date          : 7 Mar 2018
    Purpose       : Imputes spend for IPS system  
    Parameters    : SurveyData = the IPS survey dataset
                    OutputData = the output
                    var_serialNum = the serial number field name
                    varStem = stem of the imputation variables parameters
                    threshStem = stem of the imputation threshold parameters
                    numLevels = number of imputation levels
                    donorVar = Name of the donor variable
                    outputVar = Name of the output variable
                    measure = Measure function (e.g. mean)
                    var_eligibleFlag = the imputation eligibility (donor+recipient) field name
                    var_impFlag = the imputation required trigger/flag field name
                    var_impLevel = the imputation level field name
                    var_stay = the stay variable field name
    Returns       : N/A  
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    logger = cf.database_logger()
    
    # Import data via SAS
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Spend Imputation\surveydata.sas7bdat"
    df_survey_data = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(input_table_name)
    
    # Set all of the columns imported to uppercase
    df_survey_data.columns = df_survey_data.columns.str.upper()

    # Calculate the Air miles values of the imported data set.
    output_dataframe = do_ips_spend_imputation(df_survey_data, OutputData
                                               , var_serialNum, varStem
                                               , threshStem, numLevels
                                               , donorVar, outputVar
                                               , measure, var_eligibleFlag
                                               , var_impFlag, var_impLevel
                                               , var_stay) 
    
    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OutputData, output_dataframe)
    
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Spend Imputation: %s()" % function_name
    
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    logger.info("SUCCESS - Completed Spend Imputation.")
    cf.commit_to_audit_log("Create", "Spend Imputation", audit_message)
    

if __name__ == '__main__':
    calculate(SurveyData = "SAS_SURVEY_SUBSAMPLE"
              , OutputData = "SAS_SPEND_IMP"
              , var_serialNum = "SERIAL"
              , varStem = [["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR1_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR2_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR1_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR2_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR1_PV", "PUR2_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR2_PV", "PUR2_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR2_PV", "PUR2_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR2_PV"]
                        ,["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR3_PV"]
                        ,["UK_OS_PV", "DUR2_PV", "PUR3_PV"]]
              , threshStem = [19, 12, 12, 12, 12, 12, 12, 12, 0, 0]
              , numLevels = 10
              , donorVar = "SPEND"
              , outputVar = "NEWSPEND"
              , measure = "mean"
              , var_eligibleFlag = "SPEND_IMP_ELIGIBLE_PV"
              , var_impFlag = "SPEND_IMP_FLAG_PV"
              , var_impLevel = "SPENDK"
              , var_stay = "STAY") 