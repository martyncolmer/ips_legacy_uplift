import pandas as pd
import sys
# import survey_support
import numpy as np
from sas7bdat import SAS7BDAT
import math
from main.io import CommonFunctions as cf

PATH_TO_DATA = 'tests/data/unsampled_weight'

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


# Prepare survey data
def r_survey_input(survey_input):
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates input data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Load survey Data
    df_survey_input = survey_input
    df_survey_input_copy = survey_input

    # Sort input values
    sort1 = ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART']
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['UNSAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_copy

    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['UNSAMP_PORT_GRP_PV',
                                                 'UNSAMP_REGION_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['UNSAMP_PORT_GRP_PV',
                                                                              'UNSAMP_REGION_GRP_PV',
                                                                              'ARRIVEDEPART'], how='left')

    # Create traffic design weight used within GES weighting
    values = df_aux_variables.SHIFT_WT * df_aux_variables.NON_RESPONSE_WT * df_aux_variables.MINS_WT
    df_aux_variables['OOHDesignWeight'] = values
    df_aux_variables = df_aux_variables.sort_values(['SERIAL'])

    # Create input to pass into GES weighting
    df_r_ges_input = df_aux_variables[~df_aux_variables['T1'].isnull()]
    df_r_ges_input = df_r_ges_input[['SERIAL', 'ARRIVEDEPART', 'PORTROUTE', 'SHIFT_WT',
                                     'NON_RESPONSE_WT', 'MINS_WT', 'UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                     'OOHDesignWeight', 'T1']]

    # Export dataframes to CSV
    df_r_ges_input.to_csv(
        r"tests/data/r_setup/October_2017/unsampled_weight/df_r_ges_input.csv", index=False)

    return df_r_ges_input


# Prepare population totals to create AUX lookup variables
def r_population_input(survey_input, ustotals):
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Creates population data that feeds into the R GES weighting
    Parameters   : survey_input - A data frame containing the survey data for
                   processing month
                   ustotals - A data frame containing population information for
                   processing year
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    df_survey_input = survey_input
    df_us_totals = ustotals

    # Sort input values
    sort1 = ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART']
    df_us_totals = df_us_totals.sort_values(sort1)

    # Create population totals
    df_us_totals = df_us_totals.fillna('NOTHING')
    df_pop_totals = df_us_totals.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                          'ARRIVEDEPART']).agg({"UNSAMP_TOTAL": 'sum'}).reset_index()

    df_pop_totals.rename(columns={'UNSAMP_TOTAL': 'uplift'}, inplace=True)
    df_pop_totals = df_pop_totals.replace('NOTHING', np.NaN)
    df_pop_totals = df_pop_totals.sort_values(sort1)

    # Create unsampled design weight used within GES weighting
    df_survey_input['SHIFT_WT'] = df_survey_input.SHIFT_WT.astype(np.float)
    df_survey_input = df_survey_input.round({'SHIFT_WT': 3})
    values = df_survey_input.SHIFT_WT * df_survey_input.NON_RESPONSE_WT * df_survey_input.MINS_WT * df_survey_input.TRAFFIC_WT
    df_survey_input['OOHDesignWeight'] = values
    df_survey_input = df_survey_input.sort_values(sort1)

    df_survey_input = df_survey_input[df_survey_input.OOHDesignWeight > 0]
    df_survey_input = df_survey_input.fillna('NOTHING')

    df_prev_totals = df_survey_input.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                              'ARRIVEDEPART']).agg({"OOHDesignWeight": 'sum'}).reset_index()

    df_prev_totals.rename(columns={'OOHDesignWeight': 'prevtotals'}, inplace=True)

    df_prev_totals = df_prev_totals.replace('NOTHING', np.NaN)
    df_prev_totals = df_prev_totals.sort_values(sort1)

    df_pop_totals = df_pop_totals[df_pop_totals.uplift > 0]

    df_pop_totals = df_pop_totals.fillna('NOTHING')
    df_prev_totals = df_prev_totals.fillna('NOTHING')

    # Merge populations totals to create one dataframe lookup
    df_lifted_totals = pd.merge(df_prev_totals, df_pop_totals, on=['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                                                   'ARRIVEDEPART'], how='left')

    df_lifted_totals = df_lifted_totals.replace('NOTHING', np.NaN)
    df_lifted_totals['uplift'] = df_lifted_totals['uplift'].fillna(0)

    values = df_lifted_totals.prevtotals + df_lifted_totals.uplift
    df_lifted_totals['UNSAMP_TOTAL'] = values

    # Create lookup. Group by and aggregate. Allocates T_1 - T_n.
    lookup_dataframe = df_survey_input
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe = lookup_dataframe.replace('NOTHING', np.NaN)
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Create population totals for current survey data - Cleanse data and merge
    lookup_dataframe_aux = lookup_dataframe[['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART', 'T1']]
    lookup_dataframe_aux['T1'] = lookup_dataframe_aux.T1.astype(np.int64)

    df_mod_totals = pd.merge(df_lifted_totals, lookup_dataframe_aux, on=['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV',
                                                                         'ARRIVEDEPART'], how='left')

    df_mod_totals['C_group'] = 1
    df_mod_totals = df_mod_totals.drop(columns=['ARRIVEDEPART', 'UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV'])
    df_mod_totals = df_mod_totals.pivot_table(index='C_group',
                                              columns='T1',
                                              values='UNSAMP_TOTAL')

    df_mod_totals = df_mod_totals.add_prefix('T_')
    df_mod_totals['C_group'] = 1
    cols = ['C_group'] + [col for col in df_mod_totals if col != 'C_group']
    df_mod_pop_totals = df_mod_totals[cols]

    # File passed into R GES weighting process
    df_mod_pop_totals.to_csv(
        r"tests/data/r_setup/October_2017/unsampled_weight/df_mod_pop_totals.csv",
        index=False)

    return df_mod_pop_totals


def do_ips_unsampled_weight_calculation(df_surveydata, var_serialNum, var_shiftWeight, var_NRWeight,
                                        var_minWeight, var_trafficWeight, var_OOHWeight, 
                                        df_ustotals, minCountThresh):
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
                                          MODEL_GROUP_COLUMN, var_OOHWeight, 'CalWeight', GES_BOUND_TYPE,
                                          GES_UPPER_BOUND, GES_LOWER_BOUND, GES_MAX_DIFFERENCE, GES_MAX_ITERATIONS,
                                          GES_MAX_DISTANCE)

    df_survey = ges_dataframes[0]
    df_output = ges_dataframes[1]
    # Sort df_surveydata dataframe before merge
    df_survey = df_survey.sort_values(by=var_serialNum)
    df_output = df_output.sort_values(by=var_serialNum)

    # Merge the df_surveydata and output data frame to generate the summary table
    df_survey[var_OOHWeight] = df_output[var_OOHWeight]

    # Fill blank UNSAMP_TRAFFIC_WT values with 1.0
    df_survey[var_OOHWeight].fillna(1.0, inplace=True)

    # Generate POSTWEIGHT values from the UNSAMP_TRAFFIC_WT and ooh_design_weight_column values
    df_survey[POST_WEIGHT_COLUMN] = df_survey[var_OOHWeight] * df_survey[ooh_design_weight_column]

    # Sort the data ready for summarising    
    df_survey = df_survey.sort_values(by=OOH_STRATA)

    # Create the summary data frame from the sample with ooh_design_weight_column not equal to zero
    df_summary = df_survey[df_survey[ooh_design_weight_column] != 0]

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
    df_summary = df_summary.fillna('NOTHING')

    # Generate a dataframe containing the count of each evaluated group
    df_cases = df_summary.groupby(OOH_STRATA)[var_OOHWeight].agg([(CASE_COUNT_COLUMN, 'count')])

    # Flattens the column structure after adding the new CASE_COUNT_COLUMN column
    df_cases = df_cases.reset_index()

    # Summarise the data across the OOHStrataDef
    df_summary = df_summary.groupby(OOH_STRATA).agg({
        ooh_design_weight_column: 'sum',
        POST_WEIGHT_COLUMN: 'sum',
        var_OOHWeight: 'mean'
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
              var_OOHWeight, minCountThresh):
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
                   var_OOHWeight = Variable holding the unsampled weight field name
                   minCountThresh - The minimum cell count threshold
    Returns      : NA
    Requirements : do_ips_unsampled_weight_calculation()
    Dependencies : NA
    """

    # Call JSON configuration file for error logger setup
    # survey_support.setup_logging('IPS_logging_config_debug.json')

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
                                                                              var_OOHWeight,
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
