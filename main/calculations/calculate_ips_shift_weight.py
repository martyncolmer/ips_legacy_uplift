import numpy as np
import pandas as pd
import survey_support

from main.io import CommonFunctions as cf

PATH_TO_TEST_DATA = r"../../tests/data/shift_weight"

OUTPUT_TABLE_NAME = 'SAS_SHIFT_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_SHIFT_DATA'
SHIFTS_STRATA = ['SHIFT_PORT_GRP_PV',
                 'ARRIVEDEPART',
                 'WEEKDAY_END_PV',
                 'AM_PM_NIGHT_PV'] 
FLAG_COLUMN = 'SHIFT_FLAG_PV' 
FACTOR_COLUMN = 'SHIFT_FACTOR' 
TOTALS_COLUMN = 'TOTAL' 
SHIFT_NUMBER_COLUMN = 'SHIFTNO'
CROSSING_FLAG_COLUMN = 'CROSSINGS_FLAG_PV'
CROSSING_FACTOR_COLUMN = 'CROSSINGS_FACTOR'
CROSSING_NUMBER_COLUMN = 'SHUTTLE'
MIG_SI_COLUMN = 'MIGSI'
COUNT_COLUMN = 'COUNT_RESPS'
WEIGHT_SUM_COLUMN = 'SUM_SH_WT'
MIN_WEIGHT_COLUMN = 'MIN_SH_WT'
AVERAGE_WEIGHT_COLUMN = 'MEAN_SH_WT'
MAX_WEIGHT_COLUMN = 'MAX_SH_WT'
SUMMARY_KEY_COLUMN = 'SHIFT_PORT_GRP_PV'
SHIFTS_SUB_STRATA = ['SHIFT_PORT_GRP_PV',
                     'ARRIVEDEPART'] 
POSSIBLE_COUNT_COLUMN = 'POSS_SHIFT_CROSS' 
SAMPLED_COUNT_COLUMN = 'SAMP_SHIFT_CROSS' 
MINIMUM_WEIGHT_THRESHOLD = '50' 
MAXIMUM_WEIGHT_THRESHOLD = '5000'


def calculate_factor(row, flag):

    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Calculates the factor of the given row's values.
    Parameters   : row  - This parameter represents the row being manipulated
                          from the dataframe calling the function.
                   flag - Used to filter the rows being manipulated. If the
                          flag is true for the given row, the calculation would
                          be made to determine the factor.
    Returns      : The calculated factor value (float), or a np.nan.
    Requirements : NA
    Dependencies : NA
    """

    if row[flag] != 0:
        return row['NUMERATOR'] / row['DENOMINATOR']
    else:
        return np.nan


def calculate_ips_shift_factor(df_shiftsdata, df_surveydata):
    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Generates the shift factor by taking number of possible shifts over
      			   sampled shifts by stratum.  Uses the imported surveydata and shiftsdata to calculate the
                   data sets records' shift factors. This calculated value is then appended to the original
                   survey data set and used further in the process.
    Parameters   : df_shiftsdata = file holding number of total shifts (and crossings)
				   df_surveydata = survey file
    Returns      : Three data frames that are used to calculate the overall shift
                   weight and build the final output data set.
                        - df_totalsampledshifts
                        - df_possibleshifts
                        - df_surveydata_merge
    Requirements : calculate_factor()
    Dependencies : NA
    """

    print("Started: Calculate IPS Shift Factor")

    # -----------------------------------------
    # Get survey records that are shift based
    # -----------------------------------------

    df_sampledshifts = df_surveydata[df_surveydata[FLAG_COLUMN] == 1]
    df_sampledshifts.dropna()

    # Re-index the data frame
    df_sampledshifts.index = range(df_sampledshifts.shape[0])

    # -----------------------------------------
    # Calculate the number of sampled shifts by
    # strata
    # -----------------------------------------

    # Keep, sort and drop duplicates
    selected_columns = SHIFTS_STRATA + [SHIFT_NUMBER_COLUMN]
    temp_d1 = df_sampledshifts[selected_columns]
    df_sample_sorted_no_dup = temp_d1.sort_values(selected_columns).drop_duplicates()

    # Re-index the data frame
    df_sample_sorted_no_dup.index = range(df_sample_sorted_no_dup.shape[0])

    # Calculate the number of sampled shifts by strata
    df_totalsampledshifts = df_sample_sorted_no_dup.groupby(SHIFTS_STRATA)[SHIFT_NUMBER_COLUMN] \
                                                    .agg([('DENOMINATOR', 'count')]) \
                                                    .reset_index()

    # -----------------------------------------
    # Calculate the number of possible shifts
    # by strata
    # -----------------------------------------

    # Sort the shifts data by the 'SHIFTS_STRATA' column list
    df_possibleshifts_temp = df_shiftsdata.sort_values(SHIFTS_STRATA)

    # Calculate the number of possible shifts by strata
    df_possibleshifts = df_possibleshifts_temp.groupby(SHIFTS_STRATA)\
            [TOTALS_COLUMN].agg([('NUMERATOR', 'sum')])

    # Flattens the column structure after adding the new numerator column
    df_possibleshifts = df_possibleshifts.reset_index()

    # -----------------------------------------
    # Now compute the shift factor
    # -----------------------------------------

    # Sort the sampled shift data by the 'SHIFTS_STRATA' column list
    df_surveydata_sorted = df_surveydata.sort_values(SHIFTS_STRATA)

    left_join_1 = df_surveydata_sorted.merge(df_possibleshifts,
                                             on=SHIFTS_STRATA, how='left') \
        .merge(df_totalsampledshifts,
               on=SHIFTS_STRATA, how='left')

    left_join_1[FACTOR_COLUMN] = left_join_1.apply(calculate_factor,
                                                     axis=1, args=(FLAG_COLUMN,))

    df_surveydata_merge = left_join_1.drop(['NUMERATOR', 'DENOMINATOR'], 1)

    # Return the three dataframes produced
    return df_totalsampledshifts, df_possibleshifts, df_surveydata_merge


def calculate_ips_crossing_factor(df_shiftsdata, df_surveydata):
    """
    Author       : Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Generates the crossings factor by taking total crossings over sampled
    			   crossings by stratum Uses the imported surveydata and shiftsdata to calculate the
                   data sets records' crossings factors. This calculated value is
                   then appended to the original survey data set and used further in the process.
    Parameters   : df_shiftsdata = file holding number of total crossings (and poss shifts)
				   df_surveydata = survey file
    Returns      : Data frames:
                       - df_totalSampledCrossings
                       - df_surveydata_merge
    Requirements : calculate_factor()
    Dependencies : NA
    """

    print("Calculate IPS Crossing Factor")

    # Set the new data frames from the SAS data sets
    df_crossingsData = df_shiftsdata
    df_outputData = df_surveydata

    # --------------------------------------------------
    # Get survey records that are crossings based
    # --------------------------------------------------
    df_sampled_crossings = df_surveydata.loc[df_surveydata[CROSSING_FLAG_COLUMN] == 1]

    # Keep, sort and drop duplicate
    selected_columns = SHIFTS_STRATA + [SHIFT_NUMBER_COLUMN, CROSSING_NUMBER_COLUMN]
    temp_d1 = df_sampled_crossings[selected_columns]
    df_sorted_sampled_crossings = temp_d1.sort_values(selected_columns).drop_duplicates()

    # Re-index the data frame
    df_sorted_sampled_crossings.index = range(df_sorted_sampled_crossings.shape[0])

    # --------------------------------------------------
    # Calculate the number of sampled crossings by strata
    # --------------------------------------------------

    # Require reset_index() here to compose the correctly laid out data frame
    df_totalSampledCrossings = df_sorted_sampled_crossings.groupby(SHIFTS_STRATA)[CROSSING_NUMBER_COLUMN] \
                                                     .agg([('_FREQ_', 'count'),('DENOMINATOR', 'count')]) \
                                                     .reset_index()

    # Not required but incase required in future for similar
    df_totalSampledCrossings.index = range(df_totalSampledCrossings.shape[0])

    # Insert the constant class type in this case as no class specified in SAS proc
    df_totalSampledCrossings.insert(4, "_TYPE_", 0)

    # --------------------------------------------------
    # Calculate the total number of crossings by strata
    # --------------------------------------------------

    # Sort the data
    df_sorted_crossingsData = df_crossingsData.sort_values(SHIFTS_STRATA)

    # Require reset_index() here to compose the correctly laid out data frame
    df_totalCrossings = df_sorted_crossingsData.groupby(SHIFTS_STRATA)[TOTALS_COLUMN]\
                                                     .agg([('_FREQ_', 'count'), ('NUMERATOR', 'sum')]) \
                                                     .reset_index()

    df_totalCrossings.index = range(df_totalCrossings.shape[0])

    # Insert the constant class type in this case as no class specified in SAS proc
    df_totalCrossings.insert(4, "_TYPE_", 0)

    # --------------------------------------------------
    # now compute the crossings factor
    # --------------------------------------------------

    df_sorted_outputData = df_outputData.sort_values(SHIFTS_STRATA)

    df_totalCrossings  = df_totalCrossings[SHIFTS_STRATA + ['NUMERATOR']]
    df_totalSampledCrossings = df_totalSampledCrossings[SHIFTS_STRATA + ['DENOMINATOR']]

    left_join_1 = df_sorted_outputData.merge(df_totalCrossings, on=SHIFTS_STRATA, how='left') \
                                      .merge(df_totalSampledCrossings, on = SHIFTS_STRATA, how='left')

    # Calculate crossings factor
    left_join_1[CROSSING_FACTOR_COLUMN] = left_join_1.apply(calculate_factor, axis=1, args=(CROSSING_FLAG_COLUMN,))

    # Drop numerator and denominator columns
    df_surveydata_merge = left_join_1.drop(['NUMERATOR', 'DENOMINATOR'], 1)

    return df_totalSampledCrossings, df_surveydata_merge


def do_ips_shift_weight_calculation(df_surveydata, df_shiftsdata, var_serialNum, var_shiftWeight):
    
    """
    Author       : Richmond Rice / Nassir Mohammad
    Date         : May 2018
    Purpose      : Generates shift weights (design weights/initial weights) for each type
        		   of IPS traffic.  Runs the shift factor and crossings factor functions.
                   Uses the data frames they return to calculate the surveydata and summary data sets.
    Parameters   : Parameters:	df_surveydata = the IPS survey records for the period.
            					df_shiftsdata = SAS data set holding # of possible shifts / total crossings by stratum													|;
            					var_serialNum = Variable holding the record serial number
					            var_shiftWeight = Variable holding the name of the shift weight field
    Returns      : Data frames: (final_output_data, final_summary_data)
    Requirements : logging
    Dependencies : Function - calculate_ips_shift_factor()
                   Function - calculate_ips_crossing_factor()
    """

    # Calculate the Shift Factor for the given data sets
    df_totsampshifts, df_possshifts, df_surveydata_sf = calculate_ips_shift_factor(df_shiftsdata, df_surveydata)
    # Calculate the Crossings Factor for the given data sets
    df_totsampcrossings,  df_surveydata_merge = calculate_ips_crossing_factor(df_shiftsdata, df_surveydata_sf)

    # The various column sets used for setting columns, sorting columns,
    # aggregating by, merging data frames.
    colset1 = SHIFTS_STRATA + [MIG_SI_COLUMN]

    colset2 = SHIFTS_STRATA

    colset3 = SHIFTS_SUB_STRATA

    colset4 = SHIFTS_STRATA \
              + [MIG_SI_COLUMN] \
              + [POSSIBLE_COUNT_COLUMN] \
              + [SAMPLED_COUNT_COLUMN] \
              + [MIN_WEIGHT_COLUMN] \
              + [AVERAGE_WEIGHT_COLUMN] \
              + [MAX_WEIGHT_COLUMN] \
              + [COUNT_COLUMN] \
              + [WEIGHT_SUM_COLUMN]

    colset5 = [var_serialNum] + [var_shiftWeight]

    # Make all column headers upper case
    df_surveydata_merge.columns = df_surveydata_merge.columns.str.upper()
    df_possshifts.columns = df_possshifts.columns.str.upper()
    df_totsampcrossings.columns = df_totsampcrossings.columns.str.upper()
    df_totsampshifts.columns = df_totsampshifts.columns.str.upper()

    # --------------------------------------------------------------------
    # Check for any missing shift factors by extracting incorrect values
    # --------------------------------------------------------------------
    df_shift_flag = df_surveydata_merge[df_surveydata_merge[FLAG_COLUMN] == 1]

    if(len(df_shift_flag[df_shift_flag[FACTOR_COLUMN].isnull()]) > 0):
        cf.database_logger().error('Error: Case(s) contain no shift factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge[FACTOR_COLUMN].isnull() & \
                                (df_surveydata_merge[FLAG_COLUMN] != 1), FACTOR_COLUMN] = 1
        cf.database_logger().info('Success: Contains shift factor(s)')

    # --------------------------------------------------------------------
    # Check for missing crossings factor by extracting incorrect values
    # --------------------------------------------------------------------
    df_crossings_flag = df_surveydata_merge[df_surveydata_merge[CROSSING_FLAG_COLUMN] == 1]

    if len(df_crossings_flag[df_crossings_flag[CROSSING_FACTOR_COLUMN].isnull()]) > 0:
        cf.database_logger().error('Error: Case(s) contain no crossings factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge[CROSSING_FACTOR_COLUMN].isnull() & \
                                (df_surveydata_merge.CROSSINGS_FLAG_PV != 1), CROSSING_FACTOR_COLUMN] = 1
        cf.database_logger().info('Success: Contains crossings factor(s)')

    # --------------------------------------------------------------------
    # Check for invalid shift data by extracting incorrect values
    # --------------------------------------------------------------------
    df_invalid_shifts = df_surveydata_merge[df_surveydata_merge[FACTOR_COLUMN] < 0]
    if len(df_shift_flag) > 0 & len(df_invalid_shifts) > 0:
        cf.database_logger().error('Error: Case(s) has an invalid number of possible shifts')

    # Check for invalid crossings data by extracting incorrect values.
    df_invalid_crossings = df_surveydata_merge[df_surveydata_merge[CROSSING_FACTOR_COLUMN] < 0]
    if len(df_crossings_flag) > 0 & len(df_invalid_crossings) > 0:
        cf.database_logger().error('Error: Case(s) has an invalid number of total crossings')

    # Check for missing migration sampling intervals by extracting incorrect values.
    df_missing_migsi = df_surveydata_merge[MIG_SI_COLUMN].isnull().sum()
    if df_missing_migsi > 0:
        cf.database_logger().error('Error: Case(s) missing migration sampling interval')

    # --------------------------------------------------------------------
    # Calculate shift weight
    # --------------------------------------------------------------------
    df_surveydata_merge[var_shiftWeight] = df_surveydata_merge[FACTOR_COLUMN] \
                                            * df_surveydata_merge[CROSSING_FACTOR_COLUMN] \
                                            * df_surveydata_merge[MIG_SI_COLUMN]

    # --------------------------------------------------------------------
    # produce shift weight summary output
    # --------------------------------------------------------------------

    # Sort surveydata
    df_surveydata_merge_sorted = df_surveydata_merge.sort_values(colset1)

    # Group by the necessary columns and aggregate df_surveydata_merge shift weight
    df_surveydata_merge_sorted_grouped = df_surveydata_merge_sorted.groupby(SHIFTS_STRATA \
                                                                            + [MIG_SI_COLUMN])[var_shiftWeight].agg({
        COUNT_COLUMN: 'count',
        WEIGHT_SUM_COLUMN: 'sum',
        MIN_WEIGHT_COLUMN: 'min',
        AVERAGE_WEIGHT_COLUMN: 'mean',
        MAX_WEIGHT_COLUMN: 'max'
    })

    # Flatten summary columns to single row after aggregation
    df_surveydata_merge_sorted_grouped = df_surveydata_merge_sorted_grouped.reset_index()

    # --------------------------------------------------------------------
    # Merge possible shifts to summary
    # --------------------------------------------------------------------

    # Merge possible shifts to summary
    df_summary = pd.merge(df_surveydata_merge_sorted_grouped, df_possshifts, on=colset2, how='outer')
    df_summary = df_summary.rename(columns={'NUMERATOR': POSSIBLE_COUNT_COLUMN})

    # Merge totsampcrossings to summary
    df_summary = pd.merge(df_summary, df_totsampcrossings, on=colset2, how='outer')
    df_summary = df_summary.rename(columns={'DENOMINATOR': SAMPLED_COUNT_COLUMN})

    # Merge totsampshifts to summary
    df_summary = pd.merge(df_summary, df_totsampshifts, on=colset2, how='outer')
    df_summary = df_summary.rename(columns={'DENOMINATOR': 'TEMP'})

    # Merge total sample crossings and total sample shifts to single column via addition
    df_summary[SAMPLED_COUNT_COLUMN] = df_summary[SAMPLED_COUNT_COLUMN].fillna(0) + df_summary.TEMP.fillna(0)

    df_summary = df_summary.drop(['TEMP'], 1)

    # Sort summaries
    df_summary_2 = df_summary.sort_values(colset2)

    # Re-index the data frames
    df_summary_2.index = range(df_summary_2.shape[0])

    # --------------------------------------------------------------------
    # Produce summary high
    # --------------------------------------------------------------------

    # Sort survey data
    df_surveydata_merge_3 = df_surveydata_merge.sort_values(colset3)

    # Group by the necessary columns and aggregate df_surveydata_merge shift weight
    df_summary_high = df_surveydata_merge_3.groupby(colset3)[var_shiftWeight].agg({
        COUNT_COLUMN: 'count',
        WEIGHT_SUM_COLUMN: 'sum',
        MIN_WEIGHT_COLUMN: 'min',
        AVERAGE_WEIGHT_COLUMN: 'mean',
        MAX_WEIGHT_COLUMN: 'max'
    })

    # Flatten summary high columns to single row after aggregation
    df_summary_high = df_summary_high.reset_index()

    # # test code start
    # df_test = pd.read_pickle(path_to_data+ r"\highsummary.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_summary_high, df_test, check_dtype=False)
    # # test code end

    # Append total sample crossings and total sample shifts
    df_totsampshifts_appended = df_totsampshifts.append(df_totsampcrossings)

    # Re-index the data frame
    df_totsampshifts_appended.index = range(df_totsampshifts_appended.shape[0])

    # Sort total sample shifts
    df_totsampshifts_1 = df_totsampshifts_appended.sort_values(colset3)

    # Group by the necessary columns and aggregate df_totsampshifts shift weight
    df_summary_high_sampled = df_totsampshifts_1.groupby(colset3)['DENOMINATOR'].agg([(SAMPLED_COUNT_COLUMN, 'sum')])

    # Flatten summary high sampled columns to single row after aggregation
    df_summary_high_sampled = df_summary_high_sampled.reset_index()

    # Left merge summary high with summary high sampled
    df_summary_high_1 = pd.merge(df_summary_high, df_summary_high_sampled, on=SHIFTS_SUB_STRATA, how='left')

    # Append summary and summary high
    df_summary_3 = pd.concat([df_summary_high_1, df_summary_2])

    # Set summary columns
    df_summary_4 = df_summary_3[colset4]
    df_summary_5 = df_summary_4.sort_values(by=[SUMMARY_KEY_COLUMN], ascending=True, kind='mergesort')
    df_summary_5.index = range(df_summary_5.shape[0])

    # replace 0 with nan to match SAS
    df_summary_5[SAMPLED_COUNT_COLUMN].replace(0, np.nan, inplace=True)

    # Set surveydata columns
    df_surveydata_merge_output = df_surveydata_merge_3[colset5]
    df_surveydata_merge_output_2 = df_surveydata_merge_output.sort_values(['SERIAL'])

    # re-index the dataframe
    df_surveydata_merge_output_2.index = range(df_surveydata_merge_output_2.shape[0])

    final_output_data = df_surveydata_merge_output_2
    final_summary_data = df_summary_5

    # --------------------------------------------------------------------
    # Report any weights that are not within bounds
    # --------------------------------------------------------------------

    # Create shift weight threshold data sets
    df_min_sw_check = df_summary_2[df_summary_2[SAMPLED_COUNT_COLUMN].notnull() \
                                   & (df_summary_2[MIN_WEIGHT_COLUMN] < int(MINIMUM_WEIGHT_THRESHOLD))]
    df_max_sw_check = df_summary_2[df_summary_2[SAMPLED_COUNT_COLUMN].notnull() \
                                   & (df_summary_2[MAX_WEIGHT_COLUMN] > int(MAXIMUM_WEIGHT_THRESHOLD))]

    # Merge shift weight threshold data sets
    df_sw_thresholds_check = pd.merge(df_min_sw_check, df_max_sw_check, on=colset1, how='outer')

    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_sw_thresholds_check.iterrows():
        threshold_string += "___||___" \
                            + df_sw_thresholds_check.columns[0] + " : " + str(record[0]) + " | " \
                            + df_sw_thresholds_check.columns[1] + " : " + str(record[1]) + " | " \
                            + df_sw_thresholds_check.columns[2] + " : " + str(record[2]) + " | " \
                            + df_sw_thresholds_check.columns[3] + " : " + str(record[3])

    if len(df_sw_thresholds_check) > 0:
        cf.database_logger().warning('WARNING: Shift weight outside thresholds for: ' + threshold_string)

    return final_output_data, final_summary_data


def calculate(SurveyData, ShiftsData, var_serialNum, var_shiftWeight):
    """
    Author       : Richmond Rice / Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Generates shift weights (design weights/initial weights) for each type of IPS traffic.
    Parameters   : SurveyData = the IPS survey records for the period.
                   ShiftsData = Oracle table holding # of possible shifts and sampled shifts by stratum
                   var_serialNum = Variable holding the record serial number
				   var_shiftWeight = Variable that will hold the shift weight values
    Returns      : dataframe tuple (df_surveydata_out, df_summary_out)
    Requirements : do_ips_shift_weight_calculation()
    Dependencies : N/A
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Load SAS files into dataframes (this data will come from Oracle/SQL server in final version)
    df_surveydata = pd.read_pickle(PATH_TO_TEST_DATA + r"\surveydata.pkl")
    df_shiftsdata = pd.read_pickle(PATH_TO_TEST_DATA + r"\shiftsdata.pkl")

    # Import data via SAS
    # df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    # df_shiftsdata = SAS7BDAT(path_to_shifts_data).to_data_frame()

    # df_surveydata = pd.read_sas(path_to_survey_data)
    # df_shiftsdata = pd.read_sas(path_to_shifts_data)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)
    # df_shiftsdata = cf.get_table_values(ShiftsData)

    # uppercase all columns
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_shiftsdata.columns = df_shiftsdata.columns.str.upper()

    df_surveydata_out, df_summary_out = do_ips_shift_weight_calculation(df_surveydata, df_shiftsdata, var_serialNum,
                                                                        var_shiftWeight)
    
    return df_surveydata_out, df_summary_out

    # Code to be re-factored when doing main() - Start
    # ----------------------------------

    # Append the generated data to output tables
    #cf.insert_dataframe_into_table(OutputData, surveydata_dataframe)
    #cf.insert_dataframe_into_table(SummaryData, summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    #function_name = str(inspect.stack()[0][3])
    #audit_message = "Load Shift Weight calculation: %s()" %function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    #cf.database_logger().info("SUCCESS - Completed Shift weight calculation.")
    #cf.commit_to_audit_log("Create", "ShiftWeigh", audit_message)
    #print("Completed - Calculate Shift Weight")

    # Code to be re-factored when doing main() - End
