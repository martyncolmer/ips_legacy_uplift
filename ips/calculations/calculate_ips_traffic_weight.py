import numpy as np
import os
import pandas as pd
import subprocess
from sqlalchemy import create_engine

from ips.utils import common_functions as cf

PATH_TO_DATA = r"tests/data/calculations/october_2017/traffic_weight"
SERIAL = 'SERIAL'
TRAFFIC_WT = 'TRAFFIC_WT'
ARRIVEDEPART = 'ARRIVEDEPART'
T1 = "T1"

STRATA = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
MAX_RULE_LENGTH = '512'
MODEL_GROUP = 'C_group'
GES_BOUND_TYPE = 'G'
GES_UPPER_BOUND = ''
GES_LOWER_BOUND = '1.0'
GES_MAX_DIFFERENCE = '1E-8'
GES_MAX_ITERATIONS = '50'
GES_MAX_DISTANCE = '1E-8'
COUNT_COLUMN = 'CASES'
TRAFFIC_TOTAL_COLUMN = 'TRAFFICTOTAL'
POST_SUM_COLUMN = 'SUM_TRAFFIC_WT'.upper()

TRAFFIC_DESIGN_WEIGHT_COLUMN = 'TRAFDESIGNWEIGHT'
TRAFDESIGNWEIGHT = 'trafDesignWeight'

POST_WEIGHT_COLUMN = 'POSTWEIGHT'

POP_TOTALS = "SAS_TRAFFIC_DATA"
OUTPUT_TABLE_NAME = 'SAS_TRAFFIC_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_TRAFFIC'
SURVEY_TRAFFIC_AUX_TABLE = "survey_traffic_aux"
POP_PROWVEC_TABLE = 'poprowvec_traffic'
R_TRAFFIC_TABLE = "r_traffic"

var_serialNum = 'serial'.upper()
var_shiftWeight = 'shift_wt'.upper()
var_NRWeight = 'non_response_wt'.upper()
var_minWeight = 'mins_wt'.upper()
GWeightVar = 'traffic_wt'.upper()
minCountThresh = 30

SAMP_PORT_GRP_PV = 'SAMP_PORT_GRP_PV'
PORTROUTE = 'PORTROUTE'


# insert dataframe into sql and read back to resolve formatting issues
def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)


# Prepare survey data
def r_survey_input(df_survey_input):
    """
    Author       : David Powell / edits by Nassir Mohammad
    Date         : 07/06/2018
    Purpose      : Creates input data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Sort input values
    sort1 = [SAMP_PORT_GRP_PV, ARRIVEDEPART]
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted[SAMP_PORT_GRP_PV].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted[ARRIVEDEPART].isnull()]

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby([SAMP_PORT_GRP_PV,
                                                 ARRIVEDEPART]).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe[T1] = range(len(lookup_dataframe))
    lookup_dataframe[T1] = lookup_dataframe[T1] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=[SAMP_PORT_GRP_PV,
                                                                              ARRIVEDEPART], how='left')

    # Create traffic design weight used within GES weighting
    values = df_aux_variables.SHIFT_WT * df_aux_variables.NON_RESPONSE_WT * df_aux_variables.MINS_WT
    df_aux_variables[TRAFDESIGNWEIGHT] = values
    df_aux_variables = df_aux_variables.sort_values([SERIAL])

    # Create input to pass into GES weighting
    df_r_ges_input = df_aux_variables[~df_aux_variables[T1].isnull()]
    df_r_ges_input[SERIAL] = df_r_ges_input.SERIAL.astype(np.float64)
    df_r_ges_input = df_r_ges_input[[SERIAL, ARRIVEDEPART, PORTROUTE, SAMP_PORT_GRP_PV, var_shiftWeight,
                                     var_NRWeight, var_minWeight, TRAFDESIGNWEIGHT, T1]]

    df_r_ges_input_imported = convert_dataframe_to_sql_format(SURVEY_TRAFFIC_AUX_TABLE, df_r_ges_input)

    return df_r_ges_input_imported


# Prepare population totals to create AUX lookup variables
def r_population_input(df_survey_input, df_tr_totals):
    """
    Author       : David Powell / edits by Nassir Mohammad
    Date         : 07/06/2018
    Purpose      : Creates population data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
                   trtotals - A data frame containing population information for
                   processing year
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Sort input values
    sort1 = [SAMP_PORT_GRP_PV, ARRIVEDEPART]
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted[SAMP_PORT_GRP_PV].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted[ARRIVEDEPART].isnull()]

    # Sort input values
    df_pop_totals = df_tr_totals.sort_values(sort1)

    # Cleanse data
    df_pop_totals = df_pop_totals[~df_pop_totals[SAMP_PORT_GRP_PV].isnull()]
    df_pop_totals = df_pop_totals[~df_pop_totals[ARRIVEDEPART].isnull()]

    # Create unique list of items from survey input
    items = df_survey_input_sorted[SAMP_PORT_GRP_PV].tolist()
    unique = []
    [unique.append(x) for x in items if x not in unique]

    df_pop_totals_match = df_pop_totals[df_pop_totals[SAMP_PORT_GRP_PV].isin(unique)]

    # Create traffic totals
    df_pop_totals_match = df_pop_totals_match.sort_values([ARRIVEDEPART, SAMP_PORT_GRP_PV])
    df_traffic_totals = df_pop_totals_match.groupby([SAMP_PORT_GRP_PV,
                                                     ARRIVEDEPART]).agg({TRAFFIC_TOTAL_COLUMN: 'sum'}).reset_index()

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby([SAMP_PORT_GRP_PV,
                                                 ARRIVEDEPART]).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe[T1] = range(len(lookup_dataframe))
    lookup_dataframe[T1] = lookup_dataframe[T1] + 1

    # Create population totals for current survey data - Cleanse data and merge
    lookup_dataframe_aux = lookup_dataframe[[SAMP_PORT_GRP_PV, ARRIVEDEPART, T1]]
    lookup_dataframe_aux[T1] = lookup_dataframe_aux.T1.astype(np.int64)

    df_mod_totals = pd.merge(df_traffic_totals, lookup_dataframe_aux, on=[SAMP_PORT_GRP_PV,
                                                                          ARRIVEDEPART], how='left')

    df_mod_totals[MODEL_GROUP] = 1
    df_mod_totals = df_mod_totals.drop(columns=[ARRIVEDEPART, SAMP_PORT_GRP_PV])
    df_mod_pop_totals = df_mod_totals.pivot_table(index=MODEL_GROUP,
                                                  columns=T1,
                                                  values=TRAFFIC_TOTAL_COLUMN)
    df_mod_pop_totals = df_mod_pop_totals.add_prefix('T_')

    df_mod_pop_totals[MODEL_GROUP] = 1
    cols = [MODEL_GROUP] + [col for col in df_mod_pop_totals if col != MODEL_GROUP]
    df_mod_pop_totals = df_mod_pop_totals[cols]

    df_mod_pop_totals = df_mod_pop_totals.reset_index(drop=True)

    # Get credentials for connection
    username = os.getenv("DB_USER_NAME")
    database = os.getenv("DB_NAME")
    server = os.getenv("DB_SERVER")

    # recreate proc_vec table
    # TODO: ???
    con = create_engine(
        'mssql+pyodbc://' + username + ':' + username + '@' + server + '/' + database +
        '?driver=SQL+Server+Native+Client+11.0')

    # note the index gets added so needs to be removed when re-read from SQL
    df_mod_pop_totals.to_sql(POP_PROWVEC_TABLE, con, if_exists='replace')

    df_mod_pop_totals_import = cf.get_table_values(POP_PROWVEC_TABLE)
    df_mod_pop_totals_import = df_mod_pop_totals_import.drop('index', axis=1)

    return df_mod_pop_totals_import


# call R as a subprocess
def run_r_ges_script():
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Calls R Script to run GES Weighting
    Parameters   : 
    Returns      : Writes GES output to SQL Database
    Requirements : NA
    Dependencies : NA
    """

    print("Starting R script.....")

    # TODO: change hardcoded locations
    retcode = subprocess.call(["C:/Applications/RStudio/R-3.4.4/bin/Rscript",
                               "--vanilla",
                               "r_scripts/ges_r_step4.r"])

    print("R processed finished.")


# generates the summary data
def generate_ips_tw_summary(df_survey, df_output_merge_final,
                            serial_um, traffic_weight,
                            pop_totals, min_count_thresh):
    """
    Author       : Nassir Mohammad
    Date         : 08 Mar 2018
    Purpose      : Calculates IPS Traffic Weight summary
    Parameters   : Survey = survey data set
                   var_serialNum = Variable holding the name of the serial number field
                   var_trafficWeight = Variable holding the name of the traffic wght field
                   var_priorWeight = Variable holding the name of the prior (design) weight
                   TrafficTotals = Traffic (population) totals dataset
                   minCountThresh = The minimum cell count threshold
    Returns      : dataframe - df_summary_merge_sum_traftot
    Requirements : TODO
    Dependencies : TODO
    """

    # #####################################################
    #
    # calculate the post weight
    # add the traffic weight to the survey data
    #
    # #####################################################

    cols_to_keep = ['serial'.upper(), 'SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'TRAFFIC_WT', 'SHIFT_WT', 'NON_RESPONSE_WT',
                    'MINS_WT', 'TRAFDESIGNWEIGHT']
    df_survey = df_survey[cols_to_keep]

    df_survey_sorted = df_survey.sort_values(serial_um)

    # drop duplicate column (with 'None' values) across both tables before merging
    df_survey_sorted_dropped = df_survey_sorted.drop(traffic_weight, 1)

    # merge tables
    df_summary_tmp = df_survey_sorted_dropped.merge(df_output_merge_final, on=serial_um, how='outer')

    # only keep rows where var_priorWeight > 0
    df_summary_tmp = df_summary_tmp[df_summary_tmp[TRAFFIC_DESIGN_WEIGHT_COLUMN] > 0]

    # calculate and add the post weight column
    df_summary_tmp[POST_WEIGHT_COLUMN] = df_summary_tmp[TRAFFIC_DESIGN_WEIGHT_COLUMN] * df_summary_tmp[
        traffic_weight]

    keep_list = [serial_um, STRATA[1],
                 STRATA[0], TRAFFIC_DESIGN_WEIGHT_COLUMN,
                 traffic_weight, POST_WEIGHT_COLUMN]

    # only keep the selected columns
    df_summary = df_summary_tmp[keep_list]

    # Summarise the results by strata
    df_summary_sorted = df_summary.sort_values(STRATA)

    # Re-index the data frame
    df_summary_sorted.index = range(df_summary_sorted.shape[0])

    # method will possibly be deprecated - may not be an issue
    df_tmp5 = df_summary_sorted.groupby(STRATA) \
        .agg({POST_WEIGHT_COLUMN: {COUNT_COLUMN: 'count', POST_SUM_COLUMN: 'sum'},
              traffic_weight: {traffic_weight: 'mean'},
              })

    # drop the additional column indexes
    df_tmp5.columns = df_tmp5.columns.droplevel()

    # reset indexes to keep them aligned
    df_tmp5 = df_tmp5.reset_index()

    # reorder columns for SAS comparison
    col_order = [STRATA[0], STRATA[1], COUNT_COLUMN, POST_SUM_COLUMN, traffic_weight]
    df_summary_varpostweight = df_tmp5[col_order]

    # add in the traffic totals
    df_popTotals_stratadef_sort = pop_totals.sort_values(STRATA)

    # Re-index the data frame
    df_popTotals_stratadef_sort.index = range(df_popTotals_stratadef_sort.shape[0])

    df_merged = pd.merge(df_popTotals_stratadef_sort, df_summary_varpostweight, on=STRATA, how='outer')

    df_merged[traffic_weight] = df_merged[traffic_weight].apply(lambda x: round(x, 3))
    df_merged[POST_SUM_COLUMN] = df_merged[POST_SUM_COLUMN].apply(lambda x: round(x, 3))

    # # reorder columns for SAS comparison
    col_order = [STRATA[0], STRATA[1], COUNT_COLUMN, TRAFFIC_TOTAL_COLUMN, POST_SUM_COLUMN, traffic_weight]
    df_summary_merge_sum_traftot = df_merged[col_order]

    # perform checks and log
    df_sum = df_summary_merge_sum_traftot
    df_sum_check = df_sum[(df_sum[COUNT_COLUMN].isnull()) | (df_sum[COUNT_COLUMN] < min_count_thresh)]
    df_sum_check = df_sum_check[STRATA]

    if len(df_sum_check) > 0:
        threshold_string_cap = 4000
        warning_str = "Respondent count below minimum threshold for"

        # Loop over classificatory variables
        threshold_string = ""
        for index, record in df_sum_check.iterrows():
            threshold_string += \
                warning_str + " " + STRATA[0] + " = " + str(record[0]) \
                + " " + STRATA[1] + "=" + str(record[1]) + "\n"

        threshold_string_capped = threshold_string[:threshold_string_cap]

        cf.database_logger().warning("WARNING: " + threshold_string_capped)

    return df_summary_merge_sum_traftot


# carry out the traffic weight calculation using R call
def do_ips_trafweight_calculation_with_R(survey_data, trtotals):
    # clear the auxillary tables
    cf.delete_from_table(SURVEY_TRAFFIC_AUX_TABLE)

    # drop aux tables and r created tables
    cf.drop_table(POP_PROWVEC_TABLE)
    cf.drop_table(R_TRAFFIC_TABLE)

    r_survey_input(survey_data)
    r_population_input(survey_data, trtotals)

    run_r_ges_script()

    # grab the data from the SQL table and return
    output_final_import = cf.get_table_values(R_TRAFFIC_TABLE)

    ret_out = output_final_import[[SERIAL, TRAFFIC_WT]]

    # sort
    ret_out_sorted = ret_out.sort_values(SERIAL)
    ret_out_final = ret_out_sorted.reset_index(drop=True)

    # copy out the df without random for generate_ips_tw_summary
    df_ret_out_final_not_rounded = ret_out_final.copy()

    # Round the weights to 3dp
    ret_out_final[TRAFFIC_WT] = ret_out_final[TRAFFIC_WT].apply(lambda x: round(x, 3))

    # #################################
    # Generate the summary table
    # #################################

    # perform calculation
    survey_data[TRAFFIC_DESIGN_WEIGHT_COLUMN] = survey_data[var_shiftWeight] * survey_data[var_NRWeight] * survey_data[
        var_minWeight]

    # Summarise the population totals over the strata
    df_PopTotals = trtotals.sort_values(STRATA)

    # Re-index the data frame
    df_PopTotals.index = range(df_PopTotals.shape[0])

    df_popTotals = df_PopTotals.groupby(STRATA)[TRAFFIC_TOTAL_COLUMN] \
        .agg([(TRAFFIC_TOTAL_COLUMN, 'sum')]) \
        .reset_index()

    # ensure unrounded df_ret_out_final_not_rounded is supplied
    df_summary_merge_sum_traftot = generate_ips_tw_summary(survey_data, df_ret_out_final_not_rounded,
                                                           var_serialNum, GWeightVar,
                                                           df_popTotals, minCountThresh)

    # update the output SQL tables
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, ret_out_final)
    cf.insert_dataframe_into_table(SUMMARY_TABLE_NAME, df_summary_merge_sum_traftot)

    return ret_out_final, df_summary_merge_sum_traftot
