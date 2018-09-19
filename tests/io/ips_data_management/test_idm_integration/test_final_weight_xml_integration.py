import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from main.io import ips_data_management as idm
from main.calculations import calculate_ips_final_weight

import sys

with open(r'data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-idm-integration-final-wt'
TEST_DATA_DIR = r'tests\data\ips_data_management\final_weight_integration'
STEP_NAME = 'FINAL_WEIGHT'
EXPECTED_LEN = 19980

START_TIME = time.time()
print("Module level start time: {}".format(START_TIME))

@pytest.fixture(scope='module')
def database_connection():
    """ This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again. """

    return cf.get_sql_connection()


def setup_module(module):
    """ Setup any state specific to the execution of the given module. """

    #sys.setrecursionlimit(5000)
    # Assign variables
    december_survey_data_path = (TEST_DATA_DIR + r'\surveydata.csv')

    # Import survey data
    import_survey_data(december_survey_data_path)

    # Deletes data from tables as necessary
    reset_tables()


def teardown_module(module):
    """ Teardown any state that was previously setup with a setup_module method. """
    # Deletes data from temporary tables as necessary
    reset_tables()

    # Cleanses Survey Subsample table
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    # Play audio notification to indicate test is complete and print duration for performance
    cf.beep()
    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


def import_survey_data(survey_data_path):
    """
    Author       : (pinched from) Thomas Mahoney (modified by) Elinor Thorne
    Date         : (26/04/ 2018) 23/08/2018
    Purpose      : Loads the import data into 'SURVEY_SUBSAMPLE' table on the connected database.
    Parameters   : survey_data_path - the dataframe containing all of the import data.
    Returns      : NA
    Requirements : Datafile is of type '.csv', '.pkl' or '.sas7bdat'
    """

    starttime = time.time()

    # Check the survey_data_path's suffix to see what it matches then extract using the appropriate method.
    df_survey_data = pd.read_csv(survey_data_path)

    # Add the generated run id to the dataset.
    df_survey_data['RUN_ID'] = pd.Series(RUN_ID, index=df_survey_data.index)

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table('SURVEY_SUBSAMPLE', df_survey_data)

    # Print Import runtime to record performance
    print("Import runtime: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - starttime))))


def reset_tables():
    """ Cleanses tables within database. """
    # List of tables to cleanse entirely
    tables_to_unconditionally_cleanse = [idm.SAS_SURVEY_SUBSAMPLE_TABLE,
                                         idm.SAS_PROCESS_VARIABLES_TABLE]

    # Try to delete from each table in list.  If exception occurs, assume table is
    # already empty, and continue deleting from tables in list.
    for table in tables_to_unconditionally_cleanse:
        try:
            cf.delete_from_table(table)
        except Exception:
            continue

    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = ['[dbo].[PROCESS_VARIABLE_PY]',
                         '[dbo].[PROCESS_VARIABLE_TESTING]']

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list
    for table in tables_to_cleanse:
        try:
            cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID)
        except Exception:
            continue

    # Try to delete from each table in list.  If exception occurs, assume table is
    # already empty, and continue deleting from tables in list
    for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
        try:
            cf.delete_from_table(table)
        except Exception:
            continue


def test_final_weight_step():
    """ Test function """

    # Assign variables
    conn = database_connection()
    cur = conn.cursor()

    # Run, and test, first step of run.shift_weight_step
    idm.populate_survey_data_for_step(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all deleted tables are empty
    for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check all nullified columns are NULL
    for column in STEP_CONFIGURATION[STEP_NAME]['nullify_pvs']:
        column_name = column.replace('[', '').replace(']', '')
        result = cf.select_data(column_name, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
        assert result[column_name].isnull().sum() == len(result)

    # Check table has been populated
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    table_len = len(sas_survey_data.index)
    assert table_len == EXPECTED_LEN

    # Save the Survey Data before importing to calculation function
    sas_survey_data.to_csv(TEST_DATA_DIR + '\sas_survey_data_actual.csv', index=False)

    actual_results = pd.read_csv(TEST_DATA_DIR + '\sas_survey_data_actual.csv')
    expected_results = pd.read_csv(TEST_DATA_DIR + '\sas_survey_data_expected.csv')

    # Formatting because pd testing is annoying
    actual_results.sort_values(by=["SERIAL"], inplace=True)
    actual_results.index = range(0, len(actual_results))
    actual_results['SHIFT_PORT_GRP_PV'] = actual_results['SHIFT_PORT_GRP_PV'].astype(str)

    # Formatting because pd testing is annoying
    expected_results.sort_values(by=["SERIAL"], inplace=True)
    expected_results.index = range(0, len(expected_results))
    expected_results['SHIFT_PORT_GRP_PV'] = actual_results['SHIFT_PORT_GRP_PV'].astype(str)

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Run the next step and test
    surveydata_out, summary_out = calculate_ips_final_weight.do_ips_final_wt_calculation(sas_survey_data,
                                                                                         var_serialNum='SERIAL',
                                                                                         var_shiftWeight='SHIFT_WT',
                                                                                         var_NRWeight='NON_RESPONSE_WT',
                                                                                         var_minWeight='MINS_WT',
                                                                                         var_trafficWeight='TRAFFIC_WT',
                                                                                         var_unsampWeight='UNSAMP_TRAFFIC_WT',
                                                                                         var_imbWeight='IMBAL_WT',
                                                                                         var_finalWeight='FINAL_WT')

    # Test survey data from calculation function before inserting to db
    surveydata_out.to_csv(TEST_DATA_DIR + '\surveydata_out_actual.csv', index=False)
    actual_results = pd.read_csv(TEST_DATA_DIR + '\surveydata_out_actual.csv')

    expected_results = pd.read_csv(TEST_DATA_DIR + '\surveydata_out_expected.csv')

    actual_results.sort_values(by=["SERIAL"], inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results.sort_values(by=["SERIAL"], inplace=True)
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Test length of summary data from calculation as only a random sample is produced each time
    summary_out.to_csv(TEST_DATA_DIR + '\summary_out_actual.csv', index=False)
    actual_results = pd.read_csv(TEST_DATA_DIR + '\summary_out_actual.csv')

    assert(len(actual_results) == calculate_ips_final_weight.NUMBER_RECORDS_DISPLAYED)

    # Replicate intermediate steps within final_weight_step() and test length
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], surveydata_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"], summary_out)

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"]))
    assert table_len == calculate_ips_final_weight.NUMBER_RECORDS_DISPLAYED

    # Run the next step and test
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 0

    # Run the next step and test
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert SURVEY_SUBSAMPLE_TABLE was populated
    sql = """
        SELECT * FROM {}
        WHERE RUN_ID = '{}'
        """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)
    result = cur.execute(sql).fetchall()
    table_len = len(result)
    assert table_len == EXPECTED_LEN

    # Assert all records for corresponding run_id were deleted from ps_table
    sql = """
    SELECT * FROM {}
    WHERE RUN_ID = '{}'
    """.format(STEP_CONFIGURATION[STEP_NAME]["ps_table"], RUN_ID)
    result = cur.execute(sql).fetchall()
    table_len = len(result)
    assert table_len == 0

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0

    # Run the final step and test
    idm.store_step_summary(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]['ps_table']))
    assert table_len == calculate_ips_final_weight.NUMBER_RECORDS_DISPLAYED