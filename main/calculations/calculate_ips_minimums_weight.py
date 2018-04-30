import inspect
import numpy as np
import pandas as pd
import survey_support
from main.io import CommonFunctions as cf


OUTPUT_TABLE_NAME = 'SAS_MINIMUMS_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_MINIMUMS'
STRATA = ['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV']
MINIMUM_COUNT_COLUMN = 'MINS_CASES'
FULL_RESPONSE_COUNT_COLUMN = 'FULLS_CASES'
MINIMUM_FLAG_COLUMN = 'MINS_FLAG_PV'
PRIOR_WEIGHT_MINIMUM_COLUMN = 'PRIOR_GROSS_MINS'
PRIOR_WEIGHT_FULL_COLUMN = 'PRIOR_GROSS_FULLS'
PRIOR_WEIGHT_ALL_COLUMN = 'PRIOR_GROSS_ALL'
POST_WEIGHT_COLUMN = 'POST_SUM'
CASES_CARRIED_FORWARD_COLUMN = 'CASES_CARRIED_FWD'


def do_ips_minweight_calculation(df_surveydata, var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs the calculation of minimums weights
    Parameters   : df_surveydata - dataframe containing the survey data
                 : var_serialNum - name of the column containing serial number
                 : var_shiftWeight - name of the column containing calculated shift_wt values
                 : var_NRWeight - name of the column containing calculated non_response_wt values
                 : var_minWeight - name of the column to contain calculated min_wt values
    Returns      : df_out, containing a list of serial numbers with the corresponding calculated mins_wt values
                 : df_summary, containing a summary of supporting variables related to mins_wt.
    Requirements : 
    Dependencies :
    """

    df_surveydata_new = df_surveydata[df_surveydata[var_shiftWeight].notnull()]

    df_surveydata_new = df_surveydata_new[df_surveydata_new[var_NRWeight].notnull()]

    df_surveydata_new["MINS_CTRY_GRP_PV"].fillna(0, inplace=True)

    df_surveydata_new['SWNRwght'] = df_surveydata_new[var_shiftWeight] * df_surveydata_new[var_NRWeight]

    df_surveydata_sorted = df_surveydata_new.sort_values(STRATA)

    # Summarise the minimum responses by the strata
    df_mins = df_surveydata_sorted[df_surveydata_sorted[MINIMUM_FLAG_COLUMN] == 1]

    df_mins.reset_index(inplace=True)

    df_summin = df_mins.groupby(STRATA)['SWNRwght'].agg({
        PRIOR_WEIGHT_MINIMUM_COLUMN: 'sum',
        MINIMUM_COUNT_COLUMN: 'count'})

    df_summin.reset_index(inplace=True)

    # Summarise only full responses by strata
    df_fulls = df_surveydata_sorted[df_surveydata_sorted[MINIMUM_FLAG_COLUMN] == 0]

    df_sumfull = df_fulls.groupby(STRATA)['SWNRwght'].agg({
        PRIOR_WEIGHT_FULL_COLUMN: 'sum',
        FULL_RESPONSE_COUNT_COLUMN: 'count'})

    df_sumfull.reset_index(inplace=True)

    # Summarise the mig slot interviews by the strata
    df_migs = df_surveydata_sorted[df_surveydata_sorted[MINIMUM_FLAG_COLUMN] == 2]

    df_summig = df_migs.groupby(STRATA)['SWNRwght'].agg({"sumPriorWeightMigs": 'sum'})

    df_summig.reset_index(inplace=True)

    # Calculate the minimum weight by the strata
    df_summin.sort_values(STRATA)
    df_sumfull.sort_values(STRATA)
    df_summig.sort_values(STRATA)

    df_summary = pd.merge(df_sumfull, df_summig, on=STRATA, how='outer')

    df_summary = df_summary.merge(df_summin, on=STRATA, how='outer')

    df_check_prior_gross_fulls = df_summary[df_summary[PRIOR_WEIGHT_FULL_COLUMN] <= 0]

    if not df_check_prior_gross_fulls.empty and not df_summig.empty:
        cf.database_logger().error('Error: No complete or partial responses')
    else:
        df_summary[var_minWeight] = np.where(df_summary[PRIOR_WEIGHT_FULL_COLUMN] > 0,
                                             (df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN] +
                                              df_summary[PRIOR_WEIGHT_FULL_COLUMN]) /
                                             df_summary[PRIOR_WEIGHT_FULL_COLUMN],
                                             1)

    # Replace missing values with 0
    df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN].fillna(0, inplace=True)
    df_summary[PRIOR_WEIGHT_FULL_COLUMN].fillna(0, inplace=True)
    df_summary["sumPriorWeightMigs"].fillna(0, inplace=True)

    df_summary[PRIOR_WEIGHT_ALL_COLUMN] = df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN] + \
                                          df_summary[PRIOR_WEIGHT_FULL_COLUMN] + \
                                          df_summary["sumPriorWeightMigs"]

    df_summary = df_summary.sort_values(STRATA)

    df_summary[var_minWeight] = np.where(df_summary[PRIOR_WEIGHT_FULL_COLUMN] > 0,
                                         ((df_summary[PRIOR_WEIGHT_MINIMUM_COLUMN] +
                                           df_summary[PRIOR_WEIGHT_FULL_COLUMN]) / df_summary[PRIOR_WEIGHT_FULL_COLUMN]),
                                         df_summary[var_minWeight])

    df_surveydata_sorted.fillna(0, inplace=True)

    # This merge creates two mins_wt columns, x and y/
    df_out = df_summary.merge(df_surveydata_sorted, on=STRATA, how='outer')

    # Remove empty mins_wt_y column and rename mins_wt_x to mins_wt
    df_out = df_out.drop(var_minWeight + '_y', axis=1)

    df_out.rename(index=str, columns={var_minWeight + '_x': var_minWeight}, inplace=True)

    df_out.sort_values(var_serialNum)

    df_test_pre = pd.DataFrame(columns=[var_minWeight, MINIMUM_FLAG_COLUMN])

    df_test_post_1 = pd.DataFrame(columns=[var_minWeight, MINIMUM_FLAG_COLUMN])

    df_test_post_2 = pd.DataFrame(columns=[var_minWeight, MINIMUM_FLAG_COLUMN])

    df_test_pre[var_minWeight] = df_out[var_minWeight]

    df_test_pre[MINIMUM_FLAG_COLUMN] = df_out[MINIMUM_FLAG_COLUMN]

    # Set mins_wt to either 0 or 1 conditionally, then calculate the postweight value
    df_out[var_minWeight] = np.where(df_out[MINIMUM_FLAG_COLUMN] == 1.0, 0, df_out[var_minWeight])

    df_test_post_1[var_minWeight] = df_out[var_minWeight]

    df_test_post_1[MINIMUM_FLAG_COLUMN] = df_out[MINIMUM_FLAG_COLUMN]

    df_out[var_minWeight] = np.where(df_out[MINIMUM_FLAG_COLUMN] == 2.0, 1, df_out[var_minWeight])

    df_test_post_2[var_minWeight] = df_out[var_minWeight]

    df_test_post_2[MINIMUM_FLAG_COLUMN] = df_out[MINIMUM_FLAG_COLUMN]

    df_out['SWNRMINwght'] = df_out[var_shiftWeight] * \
                            df_out[var_NRWeight] * \
                            df_out[var_minWeight]

    df_out_sliced = df_out[df_out[MINIMUM_FLAG_COLUMN] != 1]
    df_postsum = df_out_sliced.groupby(STRATA)['SWNRMINwght'].agg({
        POST_WEIGHT_COLUMN: 'sum',
        CASES_CARRIED_FORWARD_COLUMN: 'count'})

    df_postsum.reset_index(inplace=True)

    df_postsum.sort_values(STRATA)

    # Merge the updated dataframe with specific columns from GNR.
    df_summary = df_summary.merge(df_postsum, on=STRATA, how='outer')

    df_summary.drop(["sumPriorWeightMigs"], axis=1, inplace=True)

    df_summary.sort_values(STRATA, inplace=True)

    # Perform data validation
    df_fulls_below_threshold = df_summary[df_summary[FULL_RESPONSE_COUNT_COLUMN] < 30]
    df_mins_below_threshold = df_summary[df_summary[MINIMUM_COUNT_COLUMN] > 0]

    df_merged_thresholds = df_fulls_below_threshold.merge(df_mins_below_threshold, how='inner')
    df_merged_thresholds = df_merged_thresholds[STRATA]

    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_merged_thresholds.iterrows():
        threshold_string += "___||___" \
                            + df_merged_thresholds.columns[0] + " : " + str(record[0]) + " | " \
                            + df_merged_thresholds.columns[1] + " : " + str(record[1])
    if len(df_merged_thresholds) > 0:
        cf.database_logger().warning('WARNING: Minimums weight outside thresholds for: '
                                     + threshold_string)

    df_out = df_out[[var_serialNum, var_minWeight]]

    # This block of rounding was largely used to test and to bring the results closer in line with the SAS results.
    # They can be removed if desired in order to produce a new standard test set.
    df_out[var_minWeight] = df_out[var_minWeight].round(3)
    columns_to_round = [PRIOR_WEIGHT_ALL_COLUMN, PRIOR_WEIGHT_FULL_COLUMN, PRIOR_WEIGHT_MINIMUM_COLUMN, var_minWeight,
                        POST_WEIGHT_COLUMN]
    df_summary[columns_to_round] = df_summary[columns_to_round].round(3)

    df_out = df_out.sort_values(var_serialNum)

    return df_out, df_summary


def calculate(SurveyData, var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs the setup required for the calculation function, then
                   calls the function
    Parameters   : SurveyData - name of the table to retrieve survey data from.
                 : MinStratumDef - list containing the names of columns to sort by
                 : var_serialNum - name of the column containing serial number
                 : var_shiftWeight - name of the column containing calculated shift_wt values
                 : var_NRWeight - name of the column containing calculated non_response_wt values
                 : var_minWeight - name of the column to contain calculated min_wt values
    Returns      : N/A
    Requirements : 
    Dependencies :
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate Minimums Weight"
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    # Import data via SAS
    # This method works for all data sets but is slower
    # df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    # This method is untested with a range of data sets but is faster
    df_surveydata = pd.read_sas(path_to_survey_data)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)

    df_surveydata.columns = df_surveydata.columns.str.upper()

    weight_calculated_dataframes = do_ips_minweight_calculation(df_surveydata, var_serialNum, var_shiftWeight,
                                                                var_NRWeight, var_minWeight)

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, surveydata_dataframe)
    cf.insert_dataframe_into_table(SUMMARY_TABLE_NAME, summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Minimums Weight calculation: %s()" % function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Minimums weight calculation.")
    cf.commit_to_audit_log("Create", "MinimumsWeight", audit_message)
