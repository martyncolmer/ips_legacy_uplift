import pandas as pd

from main.calculations import ips_impute

# import survey_support

OUTPUT_TABLE_NAME = 'SAS_STAY_IMP'
DONOR_VARIABLE = 'NUMNIGHTS'
OUTPUT_VARIABLE = 'STAY'
ELIGIBLE_FLAG_VARIABLE = 'STAY_IMP_ELIGIBLE_PV'
IMPUTATION_FLAG_VARIABLE = 'STAY_IMP_FLAG_PV'
IMPUTATION_LEVEL_VARIABLE = 'STAYK'
STRATA_BASE_LIST = [['STAYIMPCTRYLEVEL4_PV', 'STAY_PURPOSE_GRP_PV']]
THRESHOLD_BASE_LIST = [[1]]


def do_ips_stay_imputation(df_input, var_serial, num_levels, measure):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
    Purpose      : Imputes stay for the IPS system.
    Parameters   : df_input - the IPS survey dataset.
                   var_serial - the serial number field name
                   num_levels - number of imputation levels
                   measure - This variable should contain a function name which can only be used in lower case
    Returns      : Dataframe - df_output_final
    Requirements : NA
    Dependencies : NA
    """
    # Ensure imputation only occurs on eligible rows
    df_eligible = df_input.where(df_input[ELIGIBLE_FLAG_VARIABLE] == True)

    df_output_final = ips_impute.ips_impute(df_eligible, var_serial,
                                            STRATA_BASE_LIST, THRESHOLD_BASE_LIST,
                                            num_levels, DONOR_VARIABLE, OUTPUT_VARIABLE,
                                            measure, IMPUTATION_FLAG_VARIABLE,
                                            IMPUTATION_LEVEL_VARIABLE)

    # Round output column to nearest integer
    # Amended to cast object dtype to float. 24/09/2018. ET
    df_output_final[OUTPUT_VARIABLE] = pd.to_numeric(df_output_final[OUTPUT_VARIABLE], errors='coerce').round()

    return df_output_final

# Commented out as no longer required. 25/09/2018. ET
# def ips_stay_imp(survey_data, var_serial, num_levels, measure):
#     """
#     Author       : James Burr
#     Date         : 12 Feb 2018
#     Purpose      : Produces imputed values for step 8, stay_imputation
#     Parameters   : survey_data - the IPS survey dataset
#                    var_serial - the serial number field name
#                    num_levels - number of imputation levels
#                    measure - This variable should contain a function name which can only be used in lower case
#     Returns      : NA
#     Requirements : NA
#     Dependencies : NA
#     """
#
#     # Call JSON configuration file for error logger setup
#     # survey_support.setup_logging('IPS_logging_config_debug.json')
#
#     # Setup path to the base directory containing data files
#     root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Stay Imputation"
#
#     # This commented out to be changed when data is availabe for this step
#     path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"
#
#     # Import data
#     df_surveydata = pd.read_sas(path_to_survey_data)
#
#     df_surveydata.columns = df_surveydata.columns.str.upper()
#
#     df_output = do_ips_stay_imputation(df_surveydata, var_serial
#                                        , num_levels, measure)
#
#     # Append the generated data to output tables
#     cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, df_output)
#
#     # Retrieve current function name using inspect:
#     # 0 = frame object, 3 = function name.
#     # See 28.13.4. in https://docs.python.org/2/library/inspect.html
#     function_name = str(inspect.stack()[0][3])
#     audit_message = "Load Stay Imputation calculation: %s()" %function_name
#
#     # Log success message in SAS_RESPONSE and AUDIT_LOG
#     cf.database_logger().info("SUCCESS - Completed Stay imputation.")
#     cf.commit_to_audit_log("Create", "StayImputation", audit_message)
