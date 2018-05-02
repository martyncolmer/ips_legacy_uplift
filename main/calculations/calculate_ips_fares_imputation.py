import inspect
import math

import numpy as np
import pandas as pd
# import survey_support

from main.calculations import ips_impute
from main.io import CommonFunctions as cf

OUTPUT_TABLE_NAME = 'SAS_FARES_IMP'
DATE_VARIABLE = 'INTDATE'
DONOR_VARIABLE = 'DVFARE'
OUTPUT_VARIABLE = 'FARE'
ELIGIBLE_FLAG_VARIABLE = 'FARES_IMP_ELIGIBLE_PV'
IMPUTATION_FLAG_VARIABLE = 'FARES_IMP_FLAG_PV'
IMPUTATION_LEVEL_VARIABLE = 'FAREK'
AGE_FARE_VARIABLE = 'FAGE_PV'
BABY_FARE_VARIABLE = 'BABYFARE'
CHILD_FARE_VARIABLE = 'CHILDFARE'
APD_VARIABLE = 'APD_PV'
PACKAGE_VARIABLE = 'DVPACKAGE'
FARE_DISCOUNT_VARIABLE = 'DISCNT_F2_PV'
QM_FARE_VARIABLE = 'QMFARE_PV'
PACKAGE_COST_VARIABLE = 'DVPACKCOST'
DISCOUNTED_PACKAGE_COST_VARIABLE = 'DISCNT_PACKAGE_COST_PV'
PERSONS_VARIABLE = 'DVPERSONS'
EXPENDITURE_VARIABLE = 'DVEXPEND'
BEFAF_VARIABLE = 'BEFAF'
SPEND_VARIABLE = 'SPEND'
SPEND_REASON_KEY_VARIABLE = 'SPENDIMPREASON'
DUTY_FREE_VARIABLE = 'DUTY_FREE_PV'
OLD_PACKAGE_VARIABLE = 'PACKAGE'
# Setup thresh and strata base nested lists. These are used to group the data
# differently at each iteration.
THRESH_BASE_LIST = [3, 3, 3, 3, 3, 3, 3, 0, 0]
STRATA_BASE_LIST = [['INTMONTH', 'TYPE_PV', 'UKPORT1_PV', 'OSPORT1_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT2_PV', 'OSPORT1_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT1_PV', 'OSPORT2_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT2_PV', 'OSPORT2_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT3_PV', 'OSPORT2_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT3_PV', 'OSPORT3_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT4_PV', 'OSPORT3_PV', 'OPERA_PV'],
                    ['INTMONTH', 'TYPE_PV', 'UKPORT4_PV', 'OSPORT4_PV'],
                    ['INTMONTH', 'TYPE_PV', 'OSPORT4_PV']]


def do_ips_fares_imputation(df_input, var_serial, num_levels, measure):
    """
    Author       : James Burr
    Date         : 19 Feb 2018
    Purpose      : Imputes fares for the IPS system.
    Parameters   : df_input - the IPS survey dataset.
                   var_serial - the serial number field name
                   num_levels - number of imputation levels
                   measure - measure function, such as mean
    Returns      : Dataframe - df_output_final
    Requirements : NA
    Dependencies : NA
    """

    # Ensure imputation only occurs on eligible rows
    df_eligible = df_input.loc[df_input[ELIGIBLE_FLAG_VARIABLE] == 1.0]

    # Perform the imputation on eligible dataset
    df_output = ips_impute.ips_impute(df_eligible, var_serial,
                                      STRATA_BASE_LIST, THRESH_BASE_LIST,
                                      num_levels, DONOR_VARIABLE, OUTPUT_VARIABLE,
                                      measure, IMPUTATION_FLAG_VARIABLE,
                                      IMPUTATION_LEVEL_VARIABLE)

    # Merge df_output_final and df_input by var_serial_num
    df_output.sort_values(var_serial, inplace=True)

    df_input.sort_values(var_serial, inplace=True)

    df_output = df_input.merge(df_output, on=var_serial, how='left')

    # Above merge creates fares_x and fares_y column; this line removes the empty
    # fares_x column and keeps then renames the imputed fares_y column 
    df_output = df_output.drop([OUTPUT_VARIABLE + '_x', IMPUTATION_LEVEL_VARIABLE + '_x'], axis=1)

    df_output.rename(index=str, columns={OUTPUT_VARIABLE + '_y': OUTPUT_VARIABLE,
                                         IMPUTATION_LEVEL_VARIABLE + '_y': IMPUTATION_LEVEL_VARIABLE},
                     inplace=True)

    # Re-sort columns by column name in alphabetical order (may not be required)
    df_output.sort_index(axis=1, inplace=True)

    df_output = df_output.apply(compute_additional_fares, axis=1)
    df_output = df_output.apply(compute_additional_spend, axis=1)

    final_output_column_list = [var_serial, SPEND_VARIABLE, SPEND_REASON_KEY_VARIABLE, OUTPUT_VARIABLE,
                                IMPUTATION_LEVEL_VARIABLE]

    df_output = df_output[final_output_column_list]

    return df_output


def compute_additional_fares(row):
    """
    Author       : James Burr
    Date         : 13 March 2018
    Purpose      : Computes spend based on fares data and updates output dataframe.
    Parameters   : Each individal row of the df_output data frame.
    Returns      : The same row with extra calculations/edits applied.
    Requirements : NA
    Dependencies : NA
    """

    non_pack_fare = np.NaN

    # Sort out child/baby fares
    if row[IMPUTATION_FLAG_VARIABLE] == 0 or row[ELIGIBLE_FLAG_VARIABLE] == 0:
        row[OUTPUT_VARIABLE] = row[DONOR_VARIABLE]

    else:
        # Separate intdate column into usable integer values.
        day = int(row[DATE_VARIABLE][:2])
        month = int(row[DATE_VARIABLE][2:4])
        year = int(row[DATE_VARIABLE][4:8])

        # Ensure date is on or later than the 1st of May 2016
        # This is because APD for under 16's was removed from this date.
        if year >= 2016 and month >= 5 and day >= 1:
            if row[AGE_FARE_VARIABLE] == 1:
                non_pack_fare = row[BABY_FARE_VARIABLE] * (row[OUTPUT_VARIABLE] - row[APD_VARIABLE])

            elif row[AGE_FARE_VARIABLE] == 2:
                non_pack_fare = row[CHILD_FARE_VARIABLE] * (row[OUTPUT_VARIABLE] - row[APD_VARIABLE])

            elif row[AGE_FARE_VARIABLE] == 6:
                non_pack_fare = row[OUTPUT_VARIABLE]

        else:
            if row[AGE_FARE_VARIABLE] == 1:
                non_pack_fare = row[BABY_FARE_VARIABLE] * (row[OUTPUT_VARIABLE] - row[APD_VARIABLE])

            elif row[AGE_FARE_VARIABLE] == 2:
                non_pack_fare = (row[CHILD_FARE_VARIABLE] * (row[OUTPUT_VARIABLE] - row[APD_VARIABLE])) + row[APD_VARIABLE]

            elif row[AGE_FARE_VARIABLE] == 6:
                non_pack_fare = row[OUTPUT_VARIABLE]

        # Compute package versions of fare
        if row[PACKAGE_VARIABLE] in (1, 2):
            if math.isnan(non_pack_fare) or math.isnan(row[FARE_DISCOUNT_VARIABLE]):
                row[OUTPUT_VARIABLE] = np.NaN
            else:
                row[OUTPUT_VARIABLE] = round(non_pack_fare * row[FARE_DISCOUNT_VARIABLE])

        else:
            row[OUTPUT_VARIABLE] = round(non_pack_fare, 0)

    # Test for Queen Mary fare
    if row[OUTPUT_VARIABLE] == np.nan and row[QM_FARE_VARIABLE] != np.nan:
        row[OUTPUT_VARIABLE] = row[QM_FARE_VARIABLE]

    # Ensure the fare is rounded to nearest integer
    row[OUTPUT_VARIABLE] = round(row[OUTPUT_VARIABLE], 0)

    return row


def compute_additional_spend(row):
    # Compute spend per person per visit
    # For package holidays, spend is imputed if the package cost is less
    # than the cost of the fares. If all relevant fields are 0, participant
    # is assumed to have spent no money.
    if row[PACKAGE_VARIABLE] == 1:
        if row[PACKAGE_COST_VARIABLE] == 0 and row[EXPENDITURE_VARIABLE] == 0 and row[BEFAF_VARIABLE] == 0:
            row[SPEND_VARIABLE] = 0

        elif (row[PACKAGE_COST_VARIABLE] == 999999 or row[PACKAGE_COST_VARIABLE] == np.nan
              or row[DISCOUNTED_PACKAGE_COST_VARIABLE] == np.nan
              or row[PERSONS_VARIABLE] == np.nan
              or row[OUTPUT_VARIABLE] == np.nan
              or row[EXPENDITURE_VARIABLE] == 999999
              or row[EXPENDITURE_VARIABLE] == np.nan
              or row[BEFAF_VARIABLE] == np.nan
              or row[BEFAF_VARIABLE] == 999999):
            row[SPEND_VARIABLE] = np.nan

        elif (((row[DISCOUNTED_PACKAGE_COST_VARIABLE] + row[EXPENDITURE_VARIABLE] +
                row[BEFAF_VARIABLE]) / row[PERSONS_VARIABLE]) < (row[OUTPUT_VARIABLE] * 2)):
            row[SPEND_VARIABLE] = np.nan
            row[SPEND_REASON_KEY_VARIABLE] = 1

        else:
            row[SPEND_VARIABLE] = ((row[DISCOUNTED_PACKAGE_COST_VARIABLE] + row[EXPENDITURE_VARIABLE]
                             + row[BEFAF_VARIABLE]) / row[PERSONS_VARIABLE]) - (row[OUTPUT_VARIABLE] - 2)

    # DVPackage is 0
    else:
        if row[OLD_PACKAGE_VARIABLE] == 9:
            row[SPEND_VARIABLE] = np.nan

        elif row[EXPENDITURE_VARIABLE] == 0 and row[BEFAF_VARIABLE] == 0:
            row[SPEND_VARIABLE] = 0

        elif row[EXPENDITURE_VARIABLE] == 999999 or row[EXPENDITURE_VARIABLE] == np.nan \
              or row[BEFAF_VARIABLE] == 999999 or row[BEFAF_VARIABLE] == np.nan \
              or row[PERSONS_VARIABLE] == np.nan:
            row[SPEND_VARIABLE] = np.nan

        else:
            row[SPEND_VARIABLE] = (row[EXPENDITURE_VARIABLE] + row[BEFAF_VARIABLE]) / row[PERSONS_VARIABLE]

    if row[SPEND_VARIABLE] != np.nan:
        row[SPEND_VARIABLE] = row[SPEND_VARIABLE] + row[DUTY_FREE_VARIABLE]

    # Ensure the spend values are integers
    row[SPEND_VARIABLE] = round(row[SPEND_VARIABLE], 0)

    return row


def ips_fares_imp(survey_data, var_serial, num_levels, measure):
    """
    Author       : James Burr
    Date         : 19 Feb 2018
    Purpose      : Imputes fares for IPS system
    Parameters   : survey_data - the IPS survey dataset
                   var_serial - the serial number field name
                   num_levels - number of imputation levels
                   measure - Measure function (e.g. mean)
    Returns      : Dataframe containing the imputed records
    Requirements : NA
    Dependencies : NA
    """

    # Call JSON configuration file for error logger setup
    # survey_support.setup_logging('IPS_logging_config_debug.json')

    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Fares Imputation"

    # This commented out to be changed when data is availabe for this step
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    # Import data
    # This method is untested with a range of data sets but is faster
    df_surveydata = pd.read_sas(path_to_survey_data)

    df_surveydata[DATE_VARIABLE] = df_surveydata[DATE_VARIABLE].astype(str)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)

    df_surveydata.columns = df_surveydata.columns.str.upper()

    df_output = do_ips_fares_imputation(df_surveydata, var_serial, num_levels, measure)

    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, df_output)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Fares Imputation calculation: %s()" % function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Fares Imputation.")
    cf.commit_to_audit_log("Create", "FaresImputation", audit_message)
