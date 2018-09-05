'''
Created on 7 Mar 2018

@author: thorne1
'''
import inspect

import numpy as np
import pandas as pd
# import survey_support

from main.calculations import ips_impute as imp
from main.io import CommonFunctions as cf

OUTPUT_TABLE_NAME = "SAS_SPEND_IMP"
STEM_VARIABLE = [["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR1_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR2_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR1_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR2_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR1_PV", "PUR2_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR2_PV", "PUR2_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR2_PV", "PUR2_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR2_PV"],
                 ["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR3_PV"],
                 ["UK_OS_PV", "DUR2_PV", "PUR3_PV"]]
STEM_THRESHOLD = [19, 12, 12, 12, 12, 12, 12, 12, 0, 0]
DONOR_VARIABLE = "SPEND"
OTHER_DONOR_VARIABLE = "XPD"
OUTPUT_VARIABLE = "NEWSPEND"
ELIGIBLE_FLAG_VARIABLE = "SPEND_IMP_ELIGIBLE_PV"
IMPUTATION_FLAG_VARIABLE = "SPEND_IMP_FLAG_PV"
IMPUTATION_LEVEL_VARIABLE = "SPENDK"
STAY_VARIABLE = "STAY"
STAYDAYS_VARIABLE = "STAYDAYS"


def do_ips_spend_imputation(df_survey_data, var_serial, measure):
    """
    Author        : thorne1
    Date          : 7 Mar 2018
    Purpose       : Imputes spend for IPS system  
    Parameters    : df_survey_data = the IPS survey dataset
                    var_serial = the serial number field name
                    measure = Measure function (e.g. mean)
    Returns       : Dataframe  
    """

    num_levels = len(STEM_THRESHOLD)

    # Select only the eligible donors and recipients
    df_eligible = df_survey_data.copy()
    df_eligible[STAYDAYS_VARIABLE] = np.where(df_eligible[ELIGIBLE_FLAG_VARIABLE] == 1.0,
                                              (df_eligible[STAY_VARIABLE] + 1.0), np.NaN)
    df_eligible.to_csv(r'S:\CASPA\IPS\Testing\Integration\df_eligible.csv')
    df_eligible.drop(df_eligible[df_eligible[ELIGIBLE_FLAG_VARIABLE] != 1.0].index,
                     inplace=True)
    
    def selection(row):
        if row[IMPUTATION_FLAG_VARIABLE] != 1.0:
            if (row[DONOR_VARIABLE] > 0) & (row[STAYDAYS_VARIABLE] > 0):
                row[OTHER_DONOR_VARIABLE] = row[DONOR_VARIABLE] / row[STAYDAYS_VARIABLE]
            elif row[DONOR_VARIABLE] == 0:
                row[OTHER_DONOR_VARIABLE] = 0
            else:
                row[IMPUTATION_FLAG_VARIABLE] = 1 
        return row
    
    df_eligible = df_eligible.apply(selection, axis=1)

    # Perform the imputation
    df_output = imp.ips_impute(df_eligible, var_serial,
                               STEM_VARIABLE, STEM_THRESHOLD, num_levels, OTHER_DONOR_VARIABLE,
                               OUTPUT_VARIABLE, measure, IMPUTATION_FLAG_VARIABLE, IMPUTATION_LEVEL_VARIABLE)
    
    # Merge and cleanse
    df_final_output = pd.merge(df_eligible, df_output, on=var_serial, how='left')
    df_final_output.drop(IMPUTATION_LEVEL_VARIABLE + "_x", axis=1, inplace=True)
    df_final_output.rename(columns={IMPUTATION_LEVEL_VARIABLE + "_y": IMPUTATION_LEVEL_VARIABLE}, inplace=True)
    
    # Create final output with required columns 
    df_final_output = df_final_output[[var_serial, OUTPUT_VARIABLE, IMPUTATION_LEVEL_VARIABLE,
                                       STAYDAYS_VARIABLE]]
    df_final_output.loc[df_final_output[IMPUTATION_LEVEL_VARIABLE].notnull(),
                        OUTPUT_VARIABLE] = (df_final_output[OUTPUT_VARIABLE] * df_final_output[STAYDAYS_VARIABLE])
    df_final_output[OUTPUT_VARIABLE] = df_final_output[OUTPUT_VARIABLE].apply(lambda x: round(x, 0))
    
    # Cleanse df before returning
    df_final_output = df_final_output[[var_serial, IMPUTATION_LEVEL_VARIABLE, OUTPUT_VARIABLE]]
    
    return df_final_output

    
def calculate(survey_data, var_serial, measure):
    """
    Author        : thorne1
    Date          : 7 Mar 2018
    Purpose       : Imputes spend for IPS system  
    Parameters    : survey_data = the IPS survey dataset
                    var_serial = the serial number field name
                    measure = Measure function (e.g. mean)
    Returns       : N/A  
    """
    
    # Call JSON configuration file for error logger setup
    # survey_support.setup_logging('IPS_logging_config_debug.json')
    logger = cf.database_logger()
    
    # Import data via SAS
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Spend Imputation\surveydata.sas7bdat"
    df_survey_data = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    # df_surveydata = cf.get_table_values(input_table_name)
    
    # Set all of the columns imported to uppercase
    df_survey_data.columns = df_survey_data.columns.str.upper()

    # Calculate the Air miles values of the imported data set.
    output_dataframe = do_ips_spend_imputation(df_survey_data, var_serial, measure)
    
    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, output_dataframe)
    
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Spend Imputation: %s()" % function_name
    
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    logger.info("SUCCESS - Completed Spend Imputation.")
    cf.commit_to_audit_log("Create", "Spend Imputation", audit_message)
