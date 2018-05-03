import sys
import os
import logging
import inspect
import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal
import survey_support
from main.io import CommonFunctions as cf

path_to_data = r"../../tests/data/shift_weight"

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

def calculate_ips_shift_factor(df_shiftsdata, df_surveydata, ShiftsStratumDef, var_shiftFlag,
                               var_shiftNumber, var_shiftFactor, var_totals):
    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Generates the shift factor by taking number of possible shifts over
      			   sampled shifts by stratum.  Uses the imported surveydata and shiftsdata to calculate the
                   data sets records' shift factors. This calculated value is then appended to the original
                   survey data set and used further in the process.
    Parameters   : df_shiftsdata = file holding number of total shifts (and crossings)
				   df_surveydata = survey file
				   ShiftsStratumDef = variable holding the stratum definition
				   var_shiftFlag = variable that indicates that this record is shift based
				   var_shiftNumber = variable holding the name of the shift number field
				   var_shiftFactor = variable holding the name of the shift factor field
				   var_totals = variable that holds the total possible shifts information
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

    df_sampledshifts = df_surveydata[df_surveydata[var_shiftFlag] == 1]
    df_sampledshifts.dropna()

    # Re-index the data frame
    df_sampledshifts.index = range(df_sampledshifts.shape[0])

    # -----------------------------------------
    # Calculate the number of sampled shifts by
    # strata
    # -----------------------------------------

    # Keep, sort and drop duplicates
    selected_columns = ShiftsStratumDef + [var_shiftNumber]
    temp_d1 = df_sampledshifts[selected_columns]
    df_sample_sorted_no_dup = temp_d1.sort_values(selected_columns).drop_duplicates()

    # Re-index the data frame
    df_sample_sorted_no_dup.index = range(df_sample_sorted_no_dup.shape[0])

    # Calculate the number of sampled shifts by strata
    df_totalsampledshifts = df_sample_sorted_no_dup.groupby(ShiftsStratumDef)[var_shiftNumber] \
                                                    .agg([('DENOMINATOR', 'count')]) \
                                                    .reset_index()

    # -----------------------------------------
    # Calculate the number of possible shifts
    # by strata
    # -----------------------------------------

    # Sort the shifts data by the 'ShiftsStratumDef' column list
    df_possibleshifts_temp = df_shiftsdata.sort_values(ShiftsStratumDef)

    # Calculate the number of possible shifts by strata
    df_possibleshifts = df_possibleshifts_temp.groupby(ShiftsStratumDef)\
            [var_totals].agg([('NUMERATOR', 'sum')])

    # Flattens the column structure after adding the new numerator column
    df_possibleshifts = df_possibleshifts.reset_index()

    # -----------------------------------------
    # Now compute the shift factor
    # -----------------------------------------

    # Sort the sampled shift data by the 'ShiftsStratumDef' column list
    df_surveydata_sorted = df_surveydata.sort_values(ShiftsStratumDef)

    left_join_1 = df_surveydata_sorted.merge(df_possibleshifts,
                                             on=ShiftsStratumDef, how='left') \
        .merge(df_totalsampledshifts,
               on=ShiftsStratumDef, how='left')

    left_join_1[var_shiftFactor] = left_join_1.apply(calculate_factor,
                                                     axis=1, args=(var_shiftFlag,))

    df_surveydata_merge = left_join_1.drop(['NUMERATOR', 'DENOMINATOR'], 1)

    # Return the three dataframes produced
    return (df_totalsampledshifts, df_possibleshifts, df_surveydata_merge)

def calculate_ips_crossing_factor(df_shiftsdata, df_surveydata, ShiftsStratumDef, var_crossingFlag,
                                  var_shiftNumber, var_crossingNumber, var_crossingsFactor, var_totals):
    """
    Author       : Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Generates the crossings factor by taking total crossings over sampled
    			   crossings by stratum Uses the imported surveydata and shiftsdata to calculate the
                   data sets records' crossings factors. This calculated value is
                   then appended to the original survey data set and used further in the process.
    Parameters   : df_shiftsdata = file holding number of total crossings (and poss shifts)
				   df_surveydata = survey file
				   ShiftsStratumDef = variable holding the stratum definition
				   var_crossingFlag = variable that indicates that this record is crossing based
				   var_shiftNumber = variable holding the name of the shift number field
				   var_crossingNumber = variable holding the name of the crossing number field
			       var_crossingsFactor = variable that will hold the crossings factor
				   var_totals = variable that holds the total number of crossings
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
    df_sampled_crossings = df_surveydata.loc[df_surveydata[var_crossingFlag] == 1]

    # Keep, sort and drop duplicate
    selected_columns = ShiftsStratumDef + [var_shiftNumber, var_crossingNumber]
    temp_d1 = df_sampled_crossings[selected_columns]
    df_sorted_sampled_crossings = temp_d1.sort_values(selected_columns).drop_duplicates()

    # Re-index the data frame
    df_sorted_sampled_crossings.index = range(df_sorted_sampled_crossings.shape[0])

    # --------------------------------------------------
    # Calculate the number of sampled crossings by strata
    # --------------------------------------------------

    # Require reset_index() here to compose the correctly laid out data frame
    df_totalSampledCrossings = df_sorted_sampled_crossings.groupby(ShiftsStratumDef)[var_crossingNumber] \
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
    df_sorted_crossingsData = df_crossingsData.sort_values(ShiftsStratumDef)

    # Require reset_index() here to compose the correctly laid out data frame
    df_totalCrossings = df_sorted_crossingsData.groupby(ShiftsStratumDef)[var_totals]\
                                                     .agg([('_FREQ_', 'count'), ('NUMERATOR', 'sum')]) \
                                                     .reset_index()

    df_totalCrossings.index = range(df_totalCrossings.shape[0])

    # Insert the constant class type in this case as no class specified in SAS proc
    df_totalCrossings.insert(4, "_TYPE_", 0)

    # --------------------------------------------------
    # now compute the crossings factor
    # --------------------------------------------------

    df_sorted_outputData = df_outputData.sort_values(ShiftsStratumDef)

    df_totalCrossings  = df_totalCrossings[ShiftsStratumDef + ['NUMERATOR']]
    df_totalSampledCrossings = df_totalSampledCrossings[ShiftsStratumDef + ['DENOMINATOR']]

    left_join_1 = df_sorted_outputData.merge(df_totalCrossings, on=ShiftsStratumDef, how='left') \
                                      .merge(df_totalSampledCrossings, on = ShiftsStratumDef, how='left')

    # Calculate crossings factor
    left_join_1[var_crossingsFactor] = left_join_1.apply(calculate_factor,axis=1, args=(var_crossingFlag,))

    # Drop numerator and denominator columns
    df_surveydata_merge = left_join_1.drop(['NUMERATOR', 'DENOMINATOR'], 1)

    return (df_totalSampledCrossings, df_surveydata_merge)

def do_ips_shift_weight_calculation(df_surveydata,df_shiftsdata,OutputData,SummaryData,
              ShiftsStratumDef,var_serialNum,var_shiftFlag,var_shiftFactor,
              var_totals,var_shiftNumber,var_crossingFlag,var_crossingsFactor,
              var_crossingNumber,var_SI,var_shiftWeight,var_count,
              var_weightSum,var_minWeight,var_avgWeight,var_maxWeight,
              var_summaryKey,subStrata,var_possibleCount,var_sampledCount,
              minWeightThresh,maxWeightThresh):
    
    """
    Author       : Richmond Rice / Nassir Mohammad
    Date         : May 2018
    Purpose      : Generates shift weights (design weights/initial weights) for each type
        		   of IPS traffic.  Runs the shift factor and crossings factor functions.
                   Uses the data frames they return to calculate the surveydata and summary data sets.
    Parameters   : Parameters:	df_surveydata = the IPS survey records for the period.
            					df_shiftsdata = SAS data set holding # of possible shifts / total crossings by stratum													|;
            					out = Output data
            					summary = Summary data
					            ShiftsStratumDef = Variable holding the shift weight stratum definition
					            var_serialNum = Variable holding the record serial number
					            var_shiftFlag = Flag that identifies shift based records
					            var_shiftFactor = Variable holding the name of the shift factor field
					            var_totals = Variable holding the number of possible shifts / total	crossings
					            var_shiftNumber = Variable holding the shift number
					            var_crossingFlag = Flag that identifies crossing based records
					            var_crossingsFactor = Variable holding the name of the  crossings factor field
					            var_crossingNumber = Variable holding the crossing number
					            var_SI = Variable holding the name of the sampling interval field
					            var_shiftWeight = Variable holding the name of the shift weight field
					            var_count = Variable holding the name of the case count field
					            var_weightSum = Variable holding the name of the weight sum field
					            var_minWeight = Variable holding the name of the minimum weight field
					            var_avgWeight = Variable holding the name of the average weight field
					            var_maxWeight = Variable holding the name of the maximum weight field
					            var_summaryKey = Variable holding the name of the field used to sort the summary output
					            subStrata = List of variables used to produce a high level summary of the output
					            var_possibleCount = Variable holding the name of the possible shifts /
										crossings count (used in the summary output)
					            var_sampledCount = Variable holding the name of the sampled shifts /
									   crossings count (used in the summary output)
					            minWeightThresh = minimum weight threshold
					            maxWeightThresh = maximum weight threshold
    Returns      : Data frames: (final_output_data, final_summary_data)
    Requirements : logging
    Dependencies : Function - calculate_ips_shift_factor()
                   Function - calculate_ips_crossing_factor()
    """

    # Calculate the Shift Factor for the given data sets
    (df_totsampshifts, df_possshifts, df_surveydata_sf) = calculate_ips_shift_factor(df_shiftsdata,
                                                                                                 df_surveydata,
                                                                                                 ShiftsStratumDef,
                                                                                                 var_shiftFlag,
                                                                                                 var_shiftNumber,
                                                                                                 var_shiftFactor,
                                                                                                 var_totals)
    # Calculate the Crossings Factor for the given data sets
    df_totsampcrossings,  df_surveydata_merge = calculate_ips_crossing_factor(df_shiftsdata, df_surveydata_sf, ShiftsStratumDef,
                                                         var_crossingFlag, var_shiftNumber, var_crossingNumber,
                                                         var_crossingsFactor, var_totals)

    # The various column sets used for setting columns, sorting columns,
    # aggregating by, merging data frames.
    colset1 = ShiftsStratumDef + [var_SI]

    colset2 = ShiftsStratumDef

    colset3 = subStrata

    colset4 = ShiftsStratumDef \
            + [var_SI] \
            + [var_possibleCount] \
            + [var_sampledCount] \
            + [var_minWeight] \
            + [var_avgWeight] \
            + [var_maxWeight] \
            + [var_count] \
            + [var_weightSum]

    colset5 = [var_serialNum] + [var_shiftWeight]

    # Make all column headers upper case
    df_surveydata_merge.columns = df_surveydata_merge.columns.str.upper()
    df_possshifts.columns = df_possshifts.columns.str.upper()
    df_totsampcrossings.columns = df_totsampcrossings.columns.str.upper()
    df_totsampshifts.columns = df_totsampshifts.columns.str.upper()

    # --------------------------------------------------------------------
    # Check for any missing shift factors by extracting incorrect values
    # --------------------------------------------------------------------
    df_shift_flag = df_surveydata_merge[df_surveydata_merge[var_shiftFlag] == 1]

    if(len(df_shift_flag[df_shift_flag[var_shiftFactor].isnull()]) > 0):
        cf.database_logger().error('Error: Case(s) contain no shift factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge[var_shiftFactor].isnull() & \
            (df_surveydata_merge[var_shiftFlag] != 1), var_shiftFactor] = 1
        cf.database_logger().info('Success: Contains shift factor(s)')

    # --------------------------------------------------------------------
    # Check for missing crossings factor by extracting incorrect values
    # --------------------------------------------------------------------
    df_crossings_flag = df_surveydata_merge[df_surveydata_merge[var_crossingFlag] == 1]

    if(len(df_crossings_flag[df_crossings_flag[var_crossingsFactor].isnull()]) > 0):
        cf.database_logger().error('Error: Case(s) contain no crossings factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge[var_crossingsFactor].isnull() & \
            (df_surveydata_merge.CROSSINGS_FLAG_PV != 1), var_crossingsFactor] = 1
        cf.database_logger().info('Success: Contains crossings factor(s)')

    # --------------------------------------------------------------------
    # Check for invalid shift data by extracting incorrect values
    # --------------------------------------------------------------------
    df_invalid_shifts = df_surveydata_merge[df_surveydata_merge[var_shiftFactor] < 0]
    if len(df_shift_flag) > 0 & len(df_invalid_shifts) > 0:
        cf.database_logger().error('Error: Case(s) has an invalid number of possible shifts')

    # Check for invalid crossings data by extracting incorrect values.
    df_invalid_crossings = df_surveydata_merge[df_surveydata_merge[var_crossingsFactor] < 0]
    if len(df_crossings_flag) > 0 & len(df_invalid_crossings) > 0:
        cf.database_logger().error('Error: Case(s) has an invalid number of total crossings')

    # Check for missing migration sampling intervals by extracting incorrect values.
    df_missing_migsi = df_surveydata_merge[var_SI].isnull().sum()
    if df_missing_migsi > 0:
        cf.database_logger().error('Error: Case(s) missing migration sampling interval')

    # --------------------------------------------------------------------
    # Calculate shift weight
    # --------------------------------------------------------------------
    df_surveydata_merge[var_shiftWeight] = df_surveydata_merge[var_shiftFactor] \
                                            * df_surveydata_merge[var_crossingsFactor] \
                                            * df_surveydata_merge[var_SI]

    # --------------------------------------------------------------------
    # produce shift weight summary output
    # --------------------------------------------------------------------

    # Sort surveydata
    df_surveydata_merge_sorted = df_surveydata_merge.sort_values(colset1)

    # Group by the necessary columns and aggregate df_surveydata_merge shift weight
    df_surveydata_merge_sorted_grouped = df_surveydata_merge_sorted.groupby(ShiftsStratumDef \
                                                                            + [var_SI])[var_shiftWeight].agg({
        var_count: 'count',
        var_weightSum: 'sum',
        var_minWeight: 'min',
        var_avgWeight: 'mean',
        var_maxWeight: 'max'
    })

    # Flatten summary columns to single row after aggregation
    df_surveydata_merge_sorted_grouped = df_surveydata_merge_sorted_grouped.reset_index()

    # --------------------------------------------------------------------
    # Merge possible shifts to summary
    # --------------------------------------------------------------------

    # Merge possible shifts to summary
    df_summary = pd.merge(df_surveydata_merge_sorted_grouped, df_possshifts, on=colset2, how='outer')
    df_summary = df_summary.rename(columns={'NUMERATOR': var_possibleCount})

    # Merge totsampcrossings to summary
    df_summary = pd.merge(df_summary, df_totsampcrossings, on=colset2, how='outer')
    df_summary = df_summary.rename(columns={'DENOMINATOR': var_sampledCount})

    # Merge totsampshifts to summary
    df_summary = pd.merge(df_summary, df_totsampshifts, on=colset2, how='outer')
    df_summary = df_summary.rename(columns={'DENOMINATOR': 'TEMP'})

    # Merge total sample crossings and total sample shifts to single column via addition
    df_summary[var_sampledCount] = df_summary[var_sampledCount].fillna(0) + df_summary.TEMP.fillna(0)

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
        var_count: 'count',
        var_weightSum: 'sum',
        var_minWeight: 'min',
        var_avgWeight: 'mean',
        var_maxWeight: 'max'
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
    df_summary_high_sampled = df_totsampshifts_1.groupby(colset3)['DENOMINATOR'].agg([(var_sampledCount, 'sum')])

    # Flatten summary high sampled columns to single row after aggregation
    df_summary_high_sampled = df_summary_high_sampled.reset_index()

    # Left merge summary high with summary high sampled
    df_summary_high_1 = pd.merge(df_summary_high, df_summary_high_sampled, on=subStrata, how='left')

    # Append summary and summary high
    df_summary_3 = pd.concat([df_summary_high_1, df_summary_2])

    # Set summary columns
    df_summary_4 = df_summary_3[colset4]
    df_summary_5 = df_summary_4.sort_values(by=[var_summaryKey], ascending=True, kind='mergesort')
    df_summary_5.index = range(df_summary_5.shape[0])

    # replace 0 with nan to match SAS
    df_summary_5[var_sampledCount].replace(0, np.nan, inplace=True)

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
    df_min_sw_check = df_summary_2[df_summary_2[var_sampledCount].notnull() \
                                   & (df_summary_2[var_minWeight] < int(minWeightThresh))]
    df_max_sw_check = df_summary_2[df_summary_2[var_sampledCount].notnull() \
                                   & (df_summary_2[var_maxWeight] > int(maxWeightThresh))]

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

    return (final_output_data, final_summary_data)


def calculate(SurveyData,ShiftsData,OutputData,SummaryData,
              ShiftsStratumDef,var_serialNum,var_shiftFlag,var_shiftFactor,
              var_totals,var_shiftNumber,var_crossingFlag,var_crossingsFactor,
              var_crossingNumber,var_SI,var_shiftWeight,var_count,
              var_weightSum,var_minWeight,var_avgWeight,var_maxWeight,
              var_summaryKey,subStrata,var_possibleCount,var_sampledCount,
              minWeightThresh,maxWeightThresh):
    """
    Author       : Richmond Rice / Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Generates shift weights (design weights/initial weights) for each type of IPS traffic.
    Parameters   : SurveyData = the IPS survey records for the period.
                   ShiftsData = Oracle table holding # of possible shifts and sampled shifts by stratum
                   OutputData = Oracle table to hold output data
				   SummaryData = Oracle table to hold the summary output
				   ResponseTable = Oracle table to hold response information (status etc.)
				   ShiftsStratumDef = Variable holding the shifts stratum definition
				   var_serialNum = Variable holding the record serial number
				   var_shiftFlag = Variable holding the a flag indicating a shift record
				   var_shiftFactor = Variable holding the name of the shift factor field
				   var_totals = Variable holding the number of possible shifts/crossings
				   var_shiftNumber = Variable holding the shift number/identifier field
				   var_crossingFlag = Variable holding the a flag indicating a crossing	record.
				   var_crossingsFactor = Variable holding the name of the crossings factor field.
				   var_crossingNumber = Variable holding the crossing number field
				   var_SI = Variable holding the name of the sampling interval field
				   var_shiftWeight = Variable that will hold the shift weight values
				   var_count = Variable holding the name of the case count field
				   var_weightSum = Variable holding the name of the weight sum field
				   var_minWeight = Variable holding the name of the minimum weight field
				   var_avgWeight = Variable holding the name of the average weight field
				   var_maxWeight = Variable holding the name of the maximum weight field
				   var_summaryKey = Variable holding the name of the field used to sort the summary output
				   subStrata = List of variables used to produce a high level summary of the output
				   var_possibleCount = Variable holding the name of the possible shifts /
										crossings count (used in the summary output)
				   var_sampledCount = Variable holding the name of the sampled shifts /
									   crossings count (used in the summary output)
				   minWeightThresh = minimum weight threshold
				   maxWeightThresh = maximum weight threshold
    Returns      : dataframe tuple (df_surveydata_out, df_summary_out)
    Requirements : do_ips_shift_weight_calculation()
    Dependencies : N/A
    """

    print("Started IPS shift weight calculate()")

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Load SAS files into dataframes (this data will come from Oracle/SQL server in final version)
    df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
    df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")

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

    # These variables are passed into SAS but not required, we also pass them in for now
    outputData = None
    summaryData = None

    print("Start - Calculate Shift Weight")
    df_surveydata_out, df_summary_out = do_ips_shift_weight_calculation(df_surveydata,df_shiftsdata,outputData,summaryData,
                                                                  ShiftsStratumDef,var_serialNum,var_shiftFlag,var_shiftFactor,
                                                                  var_totals,var_shiftNumber,var_crossingFlag,var_crossingsFactor,
                                                                  var_crossingNumber,var_SI,var_shiftWeight,var_count,
                                                                  var_weightSum,var_minWeight,var_avgWeight,var_maxWeight,
                                                                  var_summaryKey,subStrata,var_possibleCount,var_sampledCount,
                                                                  minWeightThresh,maxWeightThresh)

    print("Calculations completed successfully, returning data sets...")

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

if __name__ == '__main__':
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE'
             , ShiftsData = 'SAS_SHIFT_DATA'
             , OutputData = 'SAS_SHIFT_WT'
             , SummaryData = 'SAS_PS_SHIFT_DATA'
             , ShiftsStratumDef = ['SHIFT_PORT_GRP_PV',
                                 'ARRIVEDEPART',
                                 'WEEKDAY_END_PV',
                                 'AM_PM_NIGHT_PV'], 
             var_serialNum = 'SERIAL', 
             var_shiftFlag = 'SHIFT_FLAG_PV', 
             var_shiftFactor = 'SHIFT_FACTOR', 
             var_totals = 'TOTAL', 
             var_shiftNumber = 'SHIFTNO', 
             var_crossingFlag = 'CROSSINGS_FLAG_PV', 
             var_crossingsFactor = 'CROSSINGS_FACTOR', 
             var_crossingNumber = 'SHUTTLE', 
             var_SI = 'MIGSI', 
             var_shiftWeight = 'SHIFT_WT', 
             var_count = 'COUNT_RESPS', 
             var_weightSum = 'SUM_SH_WT', 
             var_minWeight = 'MIN_SH_WT', 
             var_avgWeight = 'MEAN_SH_WT', 
             var_maxWeight = 'MAX_SH_WT', 
             var_summaryKey = 'SHIFT_PORT_GRP_PV', 
             subStrata = ['SHIFT_PORT_GRP_PV', 
                          'ARRIVEDEPART'], 
             var_possibleCount = 'POSS_SHIFT_CROSS', 
             var_sampledCount = 'SAMP_SHIFT_CROSS', 
             minWeightThresh = '50', 
             maxWeightThresh = '5000')
