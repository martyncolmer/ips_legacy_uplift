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


def calculate_factor(row, flag):
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Calculates the factor of the given row's values.
    Parameters   : row  - This parameter represents the row being manipulated
                          from the dataframe calling the function.
                   flag - Used to filter the rows being manipulated. If the
                          flag is true for the given row, the calculation would
                          be made to determine the factor.
    Returns      : The calculated factor value (float), or a None value.
    Requirements : NA
    Dependencies : NA
    """

    if(row[flag] != 0):
        try:
            result = row['NUMERATOR'] / row['DENOMINATOR']
        except ZeroDivisionError:
            return None
        else:
            return result
    else:
        return None


def calculate_ips_shift_factor(var_shiftFlag,ShiftsStratumDef,var_shiftNumber,
                               var_totals,var_shiftFactor,var_serialNum):
    """
    Author       : Thomas Mahoney
    Date         : 28 Dec 2017
    Purpose      : Uses the imported surveydata and shiftsdata to calculate the
                   data sets records' shift factors. This calculated value is
                   then appended to the original survey data set and used further
                   in the process.
    Parameters   : NA
    Returns      : Three data frames that are used to calculate the overall shift
                   weight and build the final output data set.
                       - df_totalsampledshifts
                       - df_possibleshifts
                       - df_surveydata_sf
    Requirements : NA
    Dependencies : NA
    """


    print("Calculate IPS Shift Factor")
    # Get survey records that are shift based
    df_sampledshifts = df_surveydata[df_surveydata[var_shiftFlag] == 1]
    df_sampledshifts.dropna()

    # Create a new data frame using the columns specified for the sampled shifts without duplicates
    df_sample_noduplicates = df_sampledshifts[ShiftsStratumDef
                                               + [var_shiftNumber]]\
                                               .drop_duplicates()

    # Sort the sample data by the 'ShiftsStratumDef' column list
    df_sample_sorted = df_sample_noduplicates.sort_values(ShiftsStratumDef)

    # Calculate the number of sampled shifts by strata
    df_totalsampledshifts = df_sample_sorted.groupby(ShiftsStratumDef)\
            [var_shiftNumber].agg({'DENOMINATOR' : 'count'})

    # Flattens the column structure after adding the new denominator column
    df_totalsampledshifts = df_totalsampledshifts.reset_index()

    # Sort the shifts data by the 'ShiftsStratumDef' column list
    df_possibleshifts_temp = df_shiftsdata.sort_values(ShiftsStratumDef)

    # Calculate the number of possible shifts by strata
    df_possibleshifts = df_possibleshifts_temp.groupby(ShiftsStratumDef)\
            [var_totals].agg({'NUMERATOR' : 'sum'})

    # Flattens the column structure after adding the new numerator column
    df_possibleshifts = df_possibleshifts.reset_index()

    # Sort the sampled shift data by the 'ShiftsStratumDef' column list
    df_sampledshifts_sorted = df_sampledshifts.sort_values(ShiftsStratumDef)

    # Merge the dataframes generated above into the sample, by the 'ShiftsStratumDef' column list
    mergedDF = pd.merge(df_sampledshifts_sorted,df_possibleshifts,on = ShiftsStratumDef, how = 'left')
    mergedDF = pd.merge(mergedDF,df_totalsampledshifts,on = ShiftsStratumDef, how = 'left')

    # Calculate the shift factor for each row of the merged dataframe    
    mergedDF[var_shiftFactor] = \
            mergedDF.apply(calculate_factor, axis=1,args = (var_shiftFlag,))

    # Merge the sampled dataframe into the full survey dataframe
    df_surveydata_sf = pd.merge(df_surveydata,mergedDF,'outer')

    # Drop the numerator and denominator columns as they're no longer needed
    df_surveydata_sf = df_surveydata_sf.drop(['NUMERATOR', 'DENOMINATOR'], axis = 1)

    # Sort the dataframe by the record's serial number
    df_surveydata_sf = df_surveydata_sf.sort_values(by=[var_serialNum])

    # Return the three dataframes produced
    return (df_totalsampledshifts, df_possibleshifts, df_surveydata_sf)


def calculate_ips_crossing_factor(df_surveydata_sf, ShiftsStratumDef, var_crossingFlag, 
                                  var_shiftNumber, var_crossingNumber, var_crossingsFactor, var_totals):
    """
    Author       : Nassir Mohammad
    Date         : Dec 2017
    Purpose      : Uses the imported surveydata and shiftsdata to calculate the
                   data sets records' crossings factors. This calculated value is
                   then appended to the original survey data set and used further
                   in the process.
    Parameters   : NA
    Returns      : Data frames:
                       - df_totalSampledCrossings
                       - df_surveydata_merge
    Requirements : NA
    Dependencies : NA
    """

    print("Calculate IPS Crossing Factor")

    # var_totals = Variable holding the number of possible shifts / total
    totals = "total".upper()

    # Set the new data frames from the SAS data sets
    df_crossingsData = df_shiftsdata
    df_outputData = df_surveydata_sf

    # Get survey records that are crossings based
    df_sampled_crossings = df_surveydata.loc[df_surveydata[var_crossingFlag] == 1]

    # Keep, sort and drop duplicates
    selected_columns = ShiftsStratumDef + [var_shiftNumber, var_crossingNumber]
    temp_d1 = df_sampled_crossings[selected_columns]
    df_sorted_sampled_crossings = temp_d1.sort_values(selected_columns).drop_duplicates()

    # Re-index the data frame
    df_sorted_sampled_crossings.index = range(df_sorted_sampled_crossings.shape[0])

    # Calculate the number of sampled crossings by strata
    # Require reset_index() here to compose the correctly laid out data frame
    df_totalSampledCrossings = df_sorted_sampled_crossings.groupby(ShiftsStratumDef)[var_crossingNumber] \
                                                     .agg(OrderedDict([('_FREQ_', 'count'),
                                                                       ('DENOMINATOR', 'count')])) \
                                                     .reset_index()

    # Not required but incase required in future for similar
    df_totalSampledCrossings.index = range(df_totalSampledCrossings.shape[0])

    # Insert the constant class type in this case as no class specified in SAS proc
    df_totalSampledCrossings.insert(4, "_TYPE_", 0)

    # Sort the data
    df_sorted_crossingsData = df_crossingsData.sort_values(ShiftsStratumDef)

    # Calculate the total number of crossings by strata
    # Require reset_index() here to compose the correctly laid out data frame
    df_totalCrossings = df_sorted_crossingsData.groupby(ShiftsStratumDef)[var_totals]\
                                                     .agg(OrderedDict([('_FREQ_', 'count'),
                                                                       ('NUMERATOR', 'sum')])) \
                                                     .reset_index()

    df_totalCrossings.index = range(df_totalCrossings.shape[0])

    # Insert the constant class type in this case as no class specified in SAS proc
    df_totalCrossings.insert(4, "_TYPE_", 0)

    df_sorted_outputData = df_outputData.sort_values(ShiftsStratumDef)

    df_sorted_outputData
    df_totalCrossings  = df_totalCrossings[ShiftsStratumDef + ['NUMERATOR']]
    df_totalSampledCrossings = df_totalSampledCrossings[ShiftsStratumDef + ['DENOMINATOR']]

    left_join_1 = df_sorted_outputData.merge(df_totalCrossings,
                                             on = ShiftsStratumDef, how = 'left') \
                                      .merge(df_totalSampledCrossings,
                                             on = ShiftsStratumDef, how = 'left')

    # Calculate crossings factor
    left_join_1[var_crossingsFactor] = left_join_1.apply(calculate_factor,
                                                     axis=1,args = (var_crossingFlag,))

    # Drop numerator and denominator columns
    df_surveydata_merge = left_join_1.drop(['NUMERATOR', 'DENOMINATOR'], 1)

    return (df_totalSampledCrossings, df_surveydata_merge)


def do_ips_shift_weight_calculation(SurveyData,ShiftsData,OutputData,SummaryData,ResponseTable,
              ShiftsStratumDef,var_serialNum,var_shiftFlag,var_shiftFactor,
              var_totals,var_shiftNumber,var_crossingFlag,var_crossingsFactor,
              var_crossingNumber,var_SI,var_shiftWeight,var_count,
              var_weightSum,var_minWeight,var_avgWeight,var_maxWeight,
              var_summaryKey,subStrata,var_possibleCount,var_sampledCount,
              minWeightThresh,maxWeightThresh):
    
    """
    Author       : Richmond Rice
    Date         : Dec 2017
    Purpose      : Runs the shift factor and crossings factor functions.
                   Uses the data frames they return to calculate the surveydata and summary data sets.
    Parameters   : NA
    Returns      : Data frame - df_surveydataData frame - df_summary
    Requirements : logging
    Dependencies : Function - calculate_ips_shift_factor
                   Function - calculate_ips_crossing_factor
    """

    # Calculate the Shift Factor for the given data sets
    shift_factor_dfs = calculate_ips_shift_factor(var_shiftFlag,ShiftsStratumDef,var_shiftNumber,
                               var_totals,var_shiftFactor,var_serialNum)

    # Extract the data frames returned by calculate_ips_shift_factor()
    df_totsampshifts = shift_factor_dfs[0]
    df_possshifts = shift_factor_dfs[1]
    df_surveydata_sf = shift_factor_dfs[2]

    # Calculate the Crossings Factor for the given data sets
    crossings_factor_dfs = calculate_ips_crossing_factor(df_surveydata_sf, ShiftsStratumDef, var_crossingFlag, 
    var_shiftNumber, var_crossingNumber, var_crossingsFactor, var_totals)

    # Extract the data frames returned by calculate_ips_crossing_factor()
    df_totsampcrossings = crossings_factor_dfs[0]
    df_surveydata_merge = crossings_factor_dfs[1]

    # The various column sets used for setting columns, sorting columns,
    # aggregating by, merging data frames.
    colset1 = ShiftsStratumDef \
            + [var_SI]

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

    colset5 = [var_serialNum] \
            + [var_shiftWeight]

    # Make all column headers upper case
    df_surveydata_merge.columns = df_surveydata_merge.columns.str.upper()
    df_possshifts.columns = df_possshifts.columns.str.upper()
    df_totsampcrossings.columns = df_totsampcrossings.columns.str.upper()
    df_totsampshifts.columns = df_totsampshifts.columns.str.upper()

    # Check for any missing shift factors by extracting incorrect values.
    df_shift_flag = df_surveydata_merge[df_surveydata_merge[var_shiftFlag] == 1]

    if(len(df_shift_flag[df_shift_flag[var_shiftFactor].isnull()]) > 0):
        cf.database_logger().error('Error: Case(s) contain no shift factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge[var_shiftFactor].isnull() & \
            (df_surveydata_merge[var_shiftFlag] != 1), var_shiftFactor] = 1
        cf.database_logger().info('Success: Contains shift factor(s)')

    # Check for missing crossings factor by extracting incorrect values.
    df_crossings_flag = df_surveydata_merge[df_surveydata_merge[var_crossingFlag] == 1]

    if(len(df_crossings_flag[df_crossings_flag[var_crossingsFactor].isnull()]) > 0):
        cf.database_logger().error('Error: Case(s) contain no crossings factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge[var_crossingsFactor].isnull() & \
            (df_surveydata_merge.CROSSINGS_FLAG_PV != 1), var_crossingsFactor] = 1
        cf.database_logger().info('Success: Contains crossings factor(s)')

    # Check for invalid shift data by extracting incorrect values.
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

    # Calculate shift weight
    df_surveydata_merge[var_shiftWeight] = \
        df_surveydata_merge[var_shiftFactor] \
            * df_surveydata_merge[var_crossingsFactor] \
            * df_surveydata_merge[var_SI]

    # Sort surveydata
    df_surveydata_merge = df_surveydata_merge.sort_values(colset1)

    # Group by the necessary columns and aggregate df_surveydata_merge shift weight
    df_summary = df_surveydata_merge.groupby(colset1)[var_shiftWeight].agg({
        var_count : 'count',
        var_weightSum : 'sum',
        var_minWeight : 'min',
        var_avgWeight : 'mean',
        var_maxWeight  : 'max'
    })

    # Flatten summary columns to single row after aggregation
    df_summary = df_summary.reset_index()

    # Merge possible shifts to summary
    df_summary = pd.merge(df_summary, df_possshifts, on = colset2, how = 'outer')
    df_summary = df_summary.rename(columns = {'NUMERATOR' : var_possibleCount})

    # Merge totsampcrossings to summary
    df_summary = pd.merge(df_summary, df_totsampcrossings, on = colset2, how = 'outer')
    df_summary = df_summary.rename(columns = {'DENOMINATOR' : var_sampledCount})

    # Merge totsampshifts to summary
    df_summary = pd.merge(df_summary, df_totsampshifts, on = colset2, how = 'outer')
    df_summary = df_summary.rename(columns = {'DENOMINATOR' : 'TEMP'})

    # Merge total sample crossings and total sample shifts to single column via addition
    df_summary[var_sampledCount] = df_summary[var_sampledCount].fillna(0) \
                                                 + df_summary.TEMP.fillna(0)
    df_summary = df_summary.drop(['TEMP'], 1)

    # Sort summary
    df_summary = df_summary.sort_values(colset2)

    # Sort survey data
    df_surveydata_merge = df_surveydata_merge.sort_values(colset3)

    # Group by the necessary columns and aggregate df_surveydata_merge shift weight
    df_summary_high = df_surveydata_merge.groupby(colset3)[var_shiftWeight].agg({
        var_count : 'count',
        var_weightSum : 'sum',
        var_minWeight : 'min',
        var_avgWeight : 'mean',
        var_avgWeight : 'max'
    })

    # Flatten summary high columns to single row after aggregation
    df_summary_high = df_summary_high.reset_index()

    # Append total sample crossings and total sample shifts
    df_totsampshifts.append(df_totsampcrossings)

    # Sort total sample shifts
    df_totsampshifts = df_totsampshifts.sort_values(colset3)

    # Group by the necessary columns and aggregate df_totsampshifts shift weight
    df_summary_high_sampled = df_totsampshifts.groupby(colset3)['DENOMINATOR'].agg({
        var_sampledCount : 'sum'
    })

    # Flatten summary high sampled columns to single row after aggregation
    df_summary_high_sampled = df_summary_high_sampled.reset_index()

    # Left merge summary high with summary high sampled
    df_summary_high = pd.merge(df_summary_high, df_summary_high_sampled,
        on = colset3, how = 'left')

    # Append summary and summary high
    df_summary = df_summary.append(df_summary_high)

    # Set summary columns
    df_summary = df_summary[colset4]

    # Sort summary
    df_summary = df_summary.sort_values(colset4)

    # Create shift weight threshold data sets
    df_min_sw_check = df_summary[df_summary[var_sampledCount].notnull() \
        & (df_summary[var_minWeight] < int(minWeightThresh))]
    df_max_sw_check = df_summary[df_summary[var_sampledCount].notnull() \
        & (df_summary[var_maxWeight] > int(maxWeightThresh))]

    # Merge shift weight threshold data sets
    df_sw_thresholds_check = pd.merge(df_min_sw_check, df_max_sw_check, on = colset1, how = 'outer')

    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_sw_thresholds_check.iterrows():
        threshold_string += "___||___" \
                         + df_sw_thresholds_check.columns[0] + " : " + str(record[0]) + " | "\
                         + df_sw_thresholds_check.columns[1] + " : " + str(record[1]) + " | "\
                         + df_sw_thresholds_check.columns[2] + " : " + str(record[2]) + " | "\
                         + df_sw_thresholds_check.columns[3] + " : " + str(record[3])
    if len(df_sw_thresholds_check) > 0:
        cf.database_logger().warning('WARNING: Shift weight outside thresholds for: ' + threshold_string)

    # Set surveydata columns
    df_surveydata_merge = df_surveydata_merge[colset5]

    # Sort surveydata columns
    df_surveydata_merge = df_surveydata_merge.sort_values(colset5)

    return (df_surveydata_merge, df_summary)


def calculate(SurveyData,ShiftsData,OutputData,SummaryData,ResponseTable,
              ShiftsStratumDef,var_serialNum,var_shiftFlag,var_shiftFactor,
              var_totals,var_shiftNumber,var_crossingFlag,var_crossingsFactor,
              var_crossingNumber,var_SI,var_shiftWeight,var_count,
              var_weightSum,var_minWeight,var_avgWeight,var_maxWeight,
              var_summaryKey,subStrata,var_possibleCount,var_sampledCount,
              minWeightThresh,maxWeightThresh):
    """
    Author       : Richmond Rice / Thomas Mahoney
    Date         : Jan 2018
    Purpose      :
    Parameters   : 
    Returns      : 
    Requirements : 
    Dependencies :
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Connect to Oracle and unload parameter list
    conn = cf.get_oracle_connection()
    global parameters
    parameters = cf.unload_parameters(205)

    # Load SAS files into dataframes (this data will come from Oracle eventually)

    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_IPS_Shift_Weight"
    path_to_survey_data = root_data_path + r"\surveydatasmall.sas7bdat"
    path_to_shifts_data = root_data_path + r"\shiftsdatasmall.sas7bdat"

    global df_surveydata
    global df_shiftsdata

    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    #df_shiftsdata = SAS7BDAT(path_to_shifts_data).to_data_frame()

    # This method is untested with a range of data sets but is faster
    #df_surveydata = pd.read_sas(path_to_survey_data)
    #df_shiftsdata = pd.read_sas(path_to_shifts_data)

    df_surveydata = cf.get_table_values(SurveyData)
    df_shiftsdata = cf.get_table_values(ShiftsData)


    print("Start - Calculate Shift Weight")
    weight_calculated_dataframes = do_ips_shift_weight_calculation(SurveyData,ShiftsData,OutputData,SummaryData,ResponseTable,
                                                                  ShiftsStratumDef,var_serialNum,var_shiftFlag,var_shiftFactor,
                                                                  var_totals,var_shiftNumber,var_crossingFlag,var_crossingsFactor,
                                                                  var_crossingNumber,var_SI,var_shiftWeight,var_count,
                                                                  var_weightSum,var_minWeight,var_avgWeight,var_maxWeight,
                                                                  var_summaryKey,subStrata,var_possibleCount,var_sampledCount,
                                                                  minWeightThresh,maxWeightThresh)

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # Output to Excel for show and tell SAS comparison
    surveydata_dataframe.to_csv('surveydata_dataframe.csv')
    summary_dataframe.to_csv('summary_dataframe.csv')
    
    print(surveydata_dataframe)
    surveydata_dataframe.to_csv('out.csv')
    cf.insert_into_table_many(OutputData, surveydata_dataframe)
    cf.insert_into_table_many(SummaryData, summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Shift Weight calculation: %s()" %function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Shift weight calculation.")
    cf.commit_to_audit_log("Create", "ShiftWeigh", audit_message)
    print("Completed - Calculate Shift Weight")


if __name__ == '__main__':
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE',
             ShiftsData = 'SAS_SHIFT_DATA', 
             OutputData = 'SAS_SHIFT_WT', 
             SummaryData = 'SAS_PS_SHIFT_DATA', 
             ResponseTable = 'SAS_RESPONSE', 
             ShiftsStratumDef = ['SHIFT_PORT_GRP_PV', 
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






