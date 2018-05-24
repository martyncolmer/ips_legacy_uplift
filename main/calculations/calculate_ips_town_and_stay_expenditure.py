'''
Created on March 2018

Author: Elinor Thorne
'''

import decimal
import inspect
import math

import numpy as np
import pandas as pd

from main.io import CommonFunctions as cf

OUTPUT_TABLE_NAME = "SAS_TOWN_STAY_IMP"
FLOW_COLUMN = "FLOW"
PURPOSE_GROUP_COLUMN = "PURPOSE_PV"
COUNTRY_GROUP_COLUMN = "STAYIMPCTRYLEVEL4_PV"
RESIDENCE_COLUMN = "RESIDENCE"
STAY_COLUMN = "STAY"
SPEND_COLUMN = "SPEND"
ELIGIBLE_FLAG_COLUMN = "TOWN_IMP_ELIGIBLE_PV"


def __calculate_ade(var_final_wt, df_output_data, source_dataframe, aggregation_columns,
                    col_name, count_column=None):
    target_dataframe = source_dataframe.copy()

    target_dataframe[col_name + "_TEMP1"] = (df_output_data[var_final_wt]
                                             * (df_output_data[SPEND_COLUMN]
                                                / df_output_data[STAY_COLUMN]))
    target_dataframe[col_name + "_TEMP2"] = df_output_data[var_final_wt]

    # Specify aggregations
    aggregations = {col_name + "_TEMP1": 'sum', col_name + "_TEMP2": 'sum'}
    if count_column is not None:
        aggregations[count_column] = 'count'

    # Group by and aggregate
    target_dataframe = target_dataframe.groupby(aggregation_columns).agg(aggregations)
    target_dataframe[col_name] = target_dataframe[col_name + "_TEMP1"] / target_dataframe[col_name + "_TEMP2"]

    # Cleanse dataframe
    target_dataframe = target_dataframe.reset_index()
    target_dataframe.drop([col_name + "_TEMP1", col_name + "_TEMP2"], axis=1, inplace=True)

    return target_dataframe


def __calculate_spends_part1(row):
    """
    Author        : thorne1
    Date          : Mar 2018
    Purpose       : Begins first part of calculating the spends
    Parameters    : row - each row of dataframe
    Returns       : row
    """

    nights = "NIGHTS"
    towncode = "TOWNCODE"

    if row["NIGHTS_IN_LONDON"] == 0:
        for count in range(1, 9):
            if math.isnan(row[towncode + str(count)]):
                if (((row["NIGHTS_NOT_LONDON"] != 0) & (not math.isnan(row["NIGHTS_NOT_LONDON"])))
                        & (not math.isnan(row[nights + str(count)]))):
                    row[SPEND_COLUMN + str(count)] = ((row[SPEND_COLUMN]
                                                    * row[nights + str(count)])
                                                   / row["NIGHTS_NOT_LONDON"])
                else:
                    row[SPEND_COLUMN + str(count)] = 0
    elif (row["NIGHTS_NOT_LONDON"] == 0) | (math.isnan(row["NIGHTS_NOT_LONDON"])):
        for count in range(1, 9):
            if not math.isnan(row[towncode + str(count)]):

                if (((row["NIGHTS_IN_LONDON"] != 0)
                     & (not math.isnan(row["NIGHTS_IN_LONDON"])))
                        & (not math.isnan(row[nights + str(count)]))):
                    row[SPEND_COLUMN + str(count)] = ((row[SPEND_COLUMN]
                                                    * row[nights + str(count)])
                                                   / row["NIGHTS_IN_LONDON"])
                else:
                    row[SPEND_COLUMN + str(count)] = 0
    else:
        if (((row["KNOWN_LONDON_VISIT"] != 0) & (not math.isnan(row["KNOWN_LONDON_VISIT"])))
                &
                ((row["KNOWN_LONDON_NOT_VISIT"] != 0) & (not math.isnan(row["KNOWN_LONDON_NOT_VISIT"])))
                &
                ((row["RATION_L_NL_ADES"] != 0) & (not math.isnan(row["RATION_L_NL_ADES"])))):
            row["H_K"] = ((row["NIGHTS_IN_LONDON"]
                           / row["NIGHTS_NOT_LONDON"])
                          * row["RATION_L_NL_ADES"])
        else:
            row["H_K"] = 0
        row["LONDON_SPEND"] = 0
        for count in range(1, 9):
            if (row[towncode + str(count)] >= 70000) & (row[towncode + str(count)] <= 79999):
                if (((row["NIGHTS_IN_LONDON"] != 0) & (not math.isnan(row["NIGHTS_IN_LONDON"])))
                        & (not math.isnan(row[nights + str(count)]))
                        & ((row["H_K"] != 0) & (not math.isnan(row["H_K"])))
                        & (row[SPEND_COLUMN] != 0)):
                    row[SPEND_COLUMN + str(count)] = (((row[SPEND_COLUMN]
                                                     * row["H_K"])
                                                    / (1 + row["H_K"]))
                                                   * (row[nights + str(count)]
                                                      / row["NIGHTS_IN_LONDON"]))
                else:
                    row[SPEND_COLUMN + str(count)] = 0
                row["LONDON_SPEND"] = (row["LONDON_SPEND"]
                                       + row[SPEND_COLUMN + str(count)])

    return row


def __calculate_spends_part2(row):
    """
    Author        : thorne1
    Date          : Mar 2018
    Purpose       : Finishes calculating the spends
    Parameters    : row - each row of dataframe
    Returns       : row
    """
    nights = "NIGHTS"
    towncode = "TOWNCODE"
    SPEND_COLUMN = "SPEND"

    if ((row["NIGHTS_IN_LONDON"] != 0)
        & ((row["NIGHTS_NOT_LONDON"] != 0)
           & (not math.isnan(row["NIGHTS_NOT_LONDON"])))):
        for count in range(1, 9):
            if math.isnan(row[towncode+str(count)]):
                row[SPEND_COLUMN+str(count)] = 0
            elif (row[towncode+str(count)] < 70000) | (row[towncode+str(count)] > 79999):
                if (((row["NIGHTS_NOT_LONDON"] != 0) & (not math.isnan(row["NIGHTS_NOT_LONDON"])))
                        & (not math.isnan(row[nights+str(count)]))):
                    row[SPEND_COLUMN+str(count)] = (row[nights+str(count)]
                                                 * ((row[SPEND_COLUMN]
                                                     - row["LONDON_SPEND"])
                                                    / row["NIGHTS_NOT_LONDON"]))
                else:
                    row[SPEND_COLUMN+str(count)] = 0

    return row


def do_ips_spend_imputation(df_survey_data, var_serial, var_final_wt):
    """
    Author        : thorne1
    Date          : 13 Mar 2018
    Purpose       : Calculate the town and stay expenditure
    Parameters    : df_survey_data = "SAS_SURVEY_SUBSAMPLE"
                    var_serial = "SERIAL"
                    var_final_wt = "FINAL_WT"
    Returns       : Dataframe
    """

    # Do some initial setup and selection
    df_output_data = df_survey_data.copy()
    df_output_data.drop(df_output_data[df_output_data[ELIGIBLE_FLAG_COLUMN] != 1.0].index, inplace=True)
    df_output_data["KNOWN_LONDON_NOT_VISIT"] = 0
    df_output_data["KNOWN_LONDON_VISIT"] = 0
    df_output_data["ADE1"] = 0
    df_output_data["ADE2"] = 0
    df_output_data["RATION_L_NL_ADES"] = 0

    # Create df where condition to calculate the vale ade1
    towncode_condition = ((df_output_data["TOWNCODE1"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE2"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE3"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE4"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE5"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE6"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE7"].between(70000, 79999)) |
                          (df_output_data["TOWNCODE8"].between(70000, 79999)))

    source_dataframe = df_output_data[[FLOW_COLUMN, PURPOSE_GROUP_COLUMN,
                                       COUNTRY_GROUP_COLUMN, "KNOWN_LONDON_VISIT",
                                       "KNOWN_LONDON_NOT_VISIT"]].ix[towncode_condition]
    aggregation_columns = [FLOW_COLUMN, PURPOSE_GROUP_COLUMN, COUNTRY_GROUP_COLUMN]
    df_segment1 = __calculate_ade(var_final_wt, df_output_data, source_dataframe,
                                  aggregation_columns, "ADE1", "KNOWN_LONDON_VISIT")
    df_segment2 = __calculate_ade(var_final_wt, df_output_data, source_dataframe,
                                  aggregation_columns, "ADE2", "KNOWN_LONDON_NOT_VISIT")

    # Merge the files containing ade1 and ade2
    df_segment_merge = pd.merge(df_segment1, df_segment2, on=aggregation_columns, how='left')

    # Update the extract with ade1, ade2 and counts
    df_extract_update = pd.merge(df_output_data, df_segment_merge, on=aggregation_columns, how='left')

    # Cleanse dataframe
    df_extract_update.rename(columns={"KNOWN_LONDON_VISIT_y": "KNOWN_LONDON_VISIT",
                                      "KNOWN_LONDON_NOT_VISIT_y": "KNOWN_LONDON_NOT_VISIT",
                                      "ADE1_y": "ADE1",
                                      "ADE2_y": "ADE2"}, inplace=True)
    df_extract_update.drop(["KNOWN_LONDON_VISIT_x",
                            "KNOWN_LONDON_NOT_VISIT_x",
                            "ADE1_x",
                            "ADE2_x"], axis=1, inplace=True)

    # Calculate ade1 without flow
    aggregation_columns = [PURPOSE_GROUP_COLUMN, COUNTRY_GROUP_COLUMN]
    source_dataframe = df_output_data[aggregation_columns].ix[towncode_condition]
    df_temp_london = __calculate_ade(var_final_wt, df_output_data, source_dataframe, aggregation_columns, "ADE1")

    # Calculate ade2 without flow
    df_temp_london2 = __calculate_ade(var_final_wt, df_output_data, source_dataframe, aggregation_columns, "ADE2")

    # Merge files containing ade1 ade2
    df_london = pd.merge(df_temp_london, df_temp_london2, on=aggregation_columns, how='left')

    # Update extract with ade1 ade2 where not already set
    df_stay_towns2 = pd.merge(df_extract_update, df_london, on=aggregation_columns, how='left')

    # Cleanse dataframe
    df_stay_towns2.rename(columns={"ADE1_x": "ADE1", "ADE2_x": "ADE2"}, inplace=True)
    df_stay_towns2.drop(["ADE1_y", "ADE2_y"], axis=1, inplace=True)

    # Calculate ratio london to not london
    df_stay_towns4 = df_stay_towns2.copy()
    df_stay_towns4["RATION_L_NL_ADES"] = np.where(((df_stay_towns4["ADE1"] != 0) & (df_stay_towns4["ADE2"] != 0)),
                                                  (df_stay_towns4["ADE1"] / df_stay_towns4["ADE2"]), 0)

    # Calculate number of nights in london and number of nights outside london
    df_stay_towns5 = df_stay_towns4.copy()
    df_stay_towns5["NIGHTS_IN_LONDON"] = 0
    df_stay_towns5["NIGHTS_NOT_LONDON"] = 0
    nights = "NIGHTS"
    towncode = "TOWNCODE"

    for count in range(1, 9):
        # Assign conditions
        in_london_condition = (df_stay_towns5[nights + str(count)].notnull()) & (
            df_stay_towns5[towncode + str(count)].between(70000, 79999))
        not_london_condition = (df_stay_towns5[nights + str(count)].notnull()) & ~(
            df_stay_towns5[towncode + str(count)].between(70000, 79999))

        # Apply conditions
        df_stay_towns5.loc[in_london_condition, "NIGHTS_IN_LONDON"] += df_stay_towns5.loc[
            in_london_condition, nights + str(count)]
        df_stay_towns5.loc[not_london_condition, "NIGHTS_NOT_LONDON"] += df_stay_towns5.loc[
            not_london_condition, nights + str(count)]

    # Calculate spends
    df_stay_towns6 = df_stay_towns5.copy()
    df_stay_towns6["H_K"] = np.NaN
    df_stay_towns6["LONDON_SPEND"] = 0
    df_stay_towns6 = df_stay_towns6.apply(__calculate_spends_part1, axis=1)

    # Finish calculating spends
    df_stay_towns7 = df_stay_towns6.copy()
    df_stay_towns7 = df_stay_towns7.apply(__calculate_spends_part2, axis=1)

    # Create output file ready for appending to Oracle file
    df_output = df_stay_towns7[[var_serial, "SPEND1", "SPEND2", "SPEND3", "SPEND4", "SPEND5", "SPEND6", "SPEND7",
                                "SPEND8"]]
    df_output.fillna(0.0)

    def round_number(row):
        """
        Author        : thorne1
        Date          : May 2018
        Purpose       : Applies proper rounding to each value within dataframe
        Parameters    : row - each row of dataframe
        Returns       : row
        """
        for col in range(1, len(row)):
            new_value = decimal.Decimal(str(row[col])).quantize(decimal.Decimal('0'), rounding=decimal.ROUND_HALF_UP)
            row[col] = new_value
        return row

    df_output.apply(round_number, axis=1)

    return df_output


def calculate(df_survey_data, var_serial, var_final_wt):
    """
    Author        : thorne1
    Date          : 13 Mar 2018
    Purpose       : Sets up calculation for the town and stay expenditure and
                  : commits results to SQL database
    Parameters    : var_serial = var_serialNum
                    var_final_wt = "FINAL_WT"
    Returns       : N/A
    """

    # Call JSON configuration file for error logger setup
    # survey_support.setup_logging('IPS_logging_config_debug.json')
    # logger = cf.database_logger()

    # Import data via SAS
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Oct Data\Town and Stay Imputation\input_townspend.sas7bdat"
    df_survey_data = pd.read_sas(path_to_survey_data)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(input_table_name)

    # Set all of the columns to uppercase
    df_survey_data.columns = df_survey_data.columns.str.upper()

    # Calculate the values of the imported data set
    output_dataframe = do_ips_spend_imputation(df_survey_data, var_serial, var_final_wt)

    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, output_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Town and Stay Imputation: %s()" % function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    # logger.info("SUCCESS - Completed Town and Stay Imputation.")
    cf.commit_to_audit_log("Create", "Town and Stay Imputation", audit_message)
