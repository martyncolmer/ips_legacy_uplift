import numpy as np
import pandas as pd
import survey_support
from main.io import CommonFunctions as cf

PATH_TO_DATA = '../../tests/data/unsampled_weight'

OUTPUT_TABLE_NAME = 'SAS_UNSAMPLED_OOH_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_UNSAMPLED_OOH'
OOH_STRATA = ['UNSAMP_PORT_GRP_PV',
              'UNSAMP_REGION_GRP_PV',
              'ARRIVEDEPART']
POPULATION_TOTALS_TABLE_NAME = 'SAS_UNSAMPLED_OOH_DATA'
TOTALS_COLUMN = 'UNSAMP_TOTAL'
MAX_RULE_LENGTH = '512'
MODEL_GROUP = 'C_GROUP'
MODEL_GROUP_COLUMN = 'C_GROUP'
OOH_WEIGHT_COLUMN = 'UNSAMP_TRAFFIC_WT'
GES_BOUND_TYPE = 'G'  # 'G' => cal. weights bound, 'F' => final weights bound
GES_UPPER_BOUND = ''
GES_LOWER_BOUND = '1.0'
GES_MAX_DIFFERENCE = '1E-8'
GES_MAX_ITERATIONS = '50'
GES_MAX_DISTANCE = '1E-8'
CASE_COUNT_COLUMN = 'CASES'
OOH_WEIGHT_SUM_COLUMN = 'SUM_UNSAMP_TRAFFIC_WT'
PRIOR_WEIGHT_SUM_COLUMN = 'SUM_PRIOR_WT'
UPLIFT_COLUMN = 'UPLIFT'
PREVIOUS_TOTAL_COLUMN = 'PREVTOTAL'
POST_WEIGHT_COLUMN = 'POSTWEIGHT'


# TODO - replace ips_ges_weighting() with correct function once available
# place holder function. This is being used until the actual GES weighting function is complete.
def do_ips_ges_weighting(input, SerialNumVarName, DesignWeightVarName,
                         StrataDef, PopTotals, TotalVar, MaxRuleLength,
                         ModelGroup, GWeightVar, CalWeightVar,
                         GESBoundType, GESUpperBound, GESLowerBound,
                         GESMaxDiff, GESMaxIter, GESMaxDist):
    df_survey_post_ges = pd.read_pickle(PATH_TO_DATA + r"/survey_serialNum_sort.pkl")
    df_output_post_ges = pd.read_pickle(PATH_TO_DATA + r"/output_merge_final.pkl")

    df_survey_post_ges.columns = df_survey_post_ges.columns.str.upper()
    df_output_post_ges.columns = df_output_post_ges.columns.str.upper()

    return df_survey_post_ges, df_output_post_ges


def do_ips_unsampled_weight_calculation(df_surveydata, var_serialNum, var_shiftWeight, var_NRWeight,
                                        var_minWeight, var_trafficWeight, df_ustotals, minCountThresh):
    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Performs calculations to determine the unsampled weight values
                   of the imported dataset.
    Parameters   : df_surveydata - the IPS df_surveydata records for the period                                
                   var_serialNum - variable holding the record serial number (UID)
                   var_shiftWeight - variable holding the shift weight field name                
                   var_NRWeight - variable holding the non-response weight field name        
                   var_minWeight - variable holding the minimum weight field name            
                   var_trafficWeight - variable holding the traffic weight field name        
                   df_ustotals - Population totals file
                   minCountThresh - The minimum cell count threshold
    Returns      : df_summary(dataframe containing random sample of rows)
                   df_output(dataframe containing serial number and calculated unsampled weight)
    Requirements : do_ips_ges_weighting()
    Dependencies : NA

    NOTES        : Currently GES weighing has not been written. Therefore the current solution
                   does not generate the output data frame. Once the function is written and we
                   are aware of what is being returned from the GES weighting function as well
                   as what is actually needed to be *sent* passed to the function we will rewrite the 
                   function call and implement its return functionality 
                   be rewriting the 
    """
    ooh_design_weight_column = 'OOHDESIGNWEIGHT'
    # Create new column for design weights (Generate the design weights)
    df_surveydata[ooh_design_weight_column] = \
        df_surveydata[var_shiftWeight] * df_surveydata[var_NRWeight] * df_surveydata[var_minWeight] * df_surveydata[
            var_trafficWeight]

    # Sort the unsampled data frame ready to be summarised
    df_ustotals = df_ustotals.sort_values(OOH_STRATA)

    # Re-index the data frame
    df_ustotals.index = range(df_ustotals.shape[0])

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
    popTotals = df_ustotals.fillna('NOTHING')

    # Summarise the uplift totals over the strata
    popTotals = popTotals.groupby(OOH_STRATA)[TOTALS_COLUMN].agg([(UPLIFT_COLUMN, 'sum')])
    popTotals.reset_index(inplace=True)

    # Replace the previously added 'NOTHING' values with their original blank values  
    popTotals = popTotals.replace('NOTHING', np.NaN)

    # Summarise the previous totals over the strata
    # Only use values where the OODesignWeight is greater than zero
    df_surveydata = df_surveydata.sort_values(by=OOH_STRATA)

    prevTotals = df_surveydata.loc[df_surveydata[ooh_design_weight_column] > 0]

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
    prevTotals = prevTotals.fillna('NOTHING')
    prevTotals = prevTotals.groupby(OOH_STRATA)[ooh_design_weight_column].agg([(PREVIOUS_TOTAL_COLUMN, 'sum')])
    prevTotals.reset_index(inplace=True)

    # Replace the previously added 'NOTHING' values with their original blank values  
    prevTotals = prevTotals.replace('NOTHING', np.NaN)

    popTotals = popTotals.sort_values(by=OOH_STRATA)

    # Generate the lifted totals data set from the two sets created
    liftedTotals = pd.merge(prevTotals, popTotals, on=OOH_STRATA, how='left')

    # Fill blank uplift and prevtotal values with zero
    liftedTotals[UPLIFT_COLUMN].fillna(0, inplace=True)
    liftedTotals[PREVIOUS_TOTAL_COLUMN].fillna(0, inplace=True)

    # Calculate the totals column from the prevtotal and uplift values
    liftedTotals[TOTALS_COLUMN] = liftedTotals[PREVIOUS_TOTAL_COLUMN] + liftedTotals[UPLIFT_COLUMN]

    # Remove any records where var_totals value is not greater than zero
    liftedTotals = liftedTotals[liftedTotals[TOTALS_COLUMN] > 0]

    # Call the GES weighting macro
    ges_dataframes = do_ips_ges_weighting(df_surveydata, var_serialNum, ooh_design_weight_column, OOH_STRATA,
                                          liftedTotals, TOTALS_COLUMN, MAX_RULE_LENGTH,
                                          MODEL_GROUP_COLUMN, OOH_WEIGHT_COLUMN, 'CalWeight', GES_BOUND_TYPE,
                                          GES_UPPER_BOUND, GES_LOWER_BOUND, GES_MAX_DIFFERENCE, GES_MAX_ITERATIONS,
                                          GES_MAX_DISTANCE)

    df_survey = ges_dataframes[0]
    df_output = ges_dataframes[1]
    # Sort df_surveydata dataframe before merge
    df_survey = df_survey.sort_values(by=var_serialNum)
    df_output = df_output.sort_values(by=var_serialNum)

    # Merge the df_surveydata and output data frame to generate the summary table
    df_survey[OOH_WEIGHT_COLUMN] = df_output[OOH_WEIGHT_COLUMN]

    # Fill blank UNSAMP_TRAFFIC_WT values with 1.0
    df_survey[OOH_WEIGHT_COLUMN].fillna(1.0, inplace=True)

    # Generate POSTWEIGHT values from the UNSAMP_TRAFFIC_WT and ooh_design_weight_column values
    df_survey[POST_WEIGHT_COLUMN] = df_survey[OOH_WEIGHT_COLUMN] * df_survey[ooh_design_weight_column]

    # Sort the data ready for summarising    
    df_survey = df_survey.sort_values(by=OOH_STRATA)

    # Create the summary data frame from the sample with ooh_design_weight_column not equal to zero
    df_summary = df_survey[df_survey[ooh_design_weight_column] != 0]

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
    df_summary = df_summary.fillna('NOTHING')

    # Generate a dataframe containing the count of each evaluated group
    df_cases = df_summary.groupby(OOH_STRATA)[OOH_WEIGHT_COLUMN].agg([(CASE_COUNT_COLUMN, 'count')])

    # Flattens the column structure after adding the new CASE_COUNT_COLUMN column
    df_cases = df_cases.reset_index()

    # Summarise the data across the OOHStrataDef
    df_summary = df_summary.groupby(OOH_STRATA).agg({
        ooh_design_weight_column: 'sum',
        POST_WEIGHT_COLUMN: 'sum',
        OOH_WEIGHT_COLUMN: 'mean'
    })

    # Flattens the column structure after adding the new ooh_design_weight_column and POSTWEIGHT columns
    df_summary = df_summary.reset_index()
    df_summary = df_summary.rename(columns={ooh_design_weight_column: PRIOR_WEIGHT_SUM_COLUMN,
                                            POST_WEIGHT_COLUMN: OOH_WEIGHT_SUM_COLUMN})

    # Merge the cases dataframe into our summary dataframe
    df_summary = pd.merge(df_summary, df_cases, on=OOH_STRATA, how='right')

    # Replace the previously added 'NOTHING' values with their original blank values  
    df_summary = df_summary.replace('NOTHING', np.NaN)

    # Identify groups where the total has been uplifted but the
    # respondent count is below the threshold.

    # Create unsampled data set for rows outside of the threshold
    df_unsampled_thresholds_check = \
        df_summary[(df_summary[OOH_WEIGHT_SUM_COLUMN] > df_summary[PRIOR_WEIGHT_SUM_COLUMN])
                   & (df_summary[CASE_COUNT_COLUMN] < minCountThresh)]

    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_unsampled_thresholds_check.iterrows():
        threshold_string += "___||___" \
                            + str(df_unsampled_thresholds_check.columns[0]) + " : " + str(record[0]) + " | " \
                            + str(df_unsampled_thresholds_check.columns[1]) + " : " + str(record[1]) + " | " \
                            + str(df_unsampled_thresholds_check.columns[2]) + " : " + str(record[2]) + " | " \
                            + str(df_unsampled_thresholds_check.columns[3]) + " : " + str(record[3])

    # Output the values outside of the threshold to the logger - COMMENTED OUT DUE TO SIZE ISSUE?
    if len(df_unsampled_thresholds_check) > 0:
        cf.database_logger().warning(
            'WARNING: Respondent count below minimum threshold for: ')  # + str(threshold_string))

    # Return the generated data frames to be appended to oracle
    return df_output, df_summary


def calculate(SurveyData, var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight, var_trafficWeight,
              minCountThresh):
    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : Apr 2018
    Purpose      : Imports the required data sets for performing the unsampled
                   weight calculation. This function also triggers the unsmapled
                   weight calculation function using the imported data. Once 
                   complete it will append the newly generated data frames to the 
                   specified oracle database tables. 
    Parameters   : SurveyData - the IPS df_surveydata records for the period                             
                   var_serialNum - variable holding the record serial number (UID)
                   var_shiftWeight - variable holding the shift weight field name                
                   var_NRWeight - variable holding the non-response weight field name        
                   var_minWeight - variable holding the minimum weight field name            
                   var_trafficWeight - variable holding the traffic weight field name        
                   var_designWeight - Variable holding the design weight field name
                   minCountThresh - The minimum cell count threshold
    Returns      : NA
    Requirements : do_ips_unsampled_weight_calculation()
    Dependencies : NA
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Import data
    df_surveydata = pd.read_pickle(PATH_TO_DATA + r"/survey_input.pkl")
    # NB: instead of reading from POPULATION_TOTALS_TABLE_NAME we read from file here
    df_ustotals = pd.read_pickle(PATH_TO_DATA + r"/ustotals.pkl")

    # Set all of the columns imported to uppercase
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_ustotals.columns = df_ustotals.columns.str.upper()

    # Calculate the unsampled weights of the imported dataset.

    print("Start - Calculate UnSampled Weight.")
    output_dataframe, summary_dataframe = do_ips_unsampled_weight_calculation(df_surveydata, var_serialNum,
                                                                              var_shiftWeight, var_NRWeight,
                                                                              var_minWeight, var_trafficWeight,
                                                                              df_ustotals, minCountThresh)

    # TODO - following code to be removed/refactored once IPS_main() done
    # Append the generated data to output tables
    # cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, output_dataframe)
    # cf.insert_dataframe_into_table(SUMMARY_TABLE_NAME, summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    # function_name = str(inspect.stack()[0][3])
    # audit_message = "Load UnSampled Weight calculation: %s()" % function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    # cf.database_logger().info("SUCCESS - Completed UnSampled weight calculation.")
    # cf.commit_to_audit_log("Create", "UnSampled", audit_message)
    return output_dataframe, summary_dataframe
