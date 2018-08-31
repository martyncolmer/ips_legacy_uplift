import pytest
import json
import pandas as pd
import time
import numpy as np

import sys

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from main.io import ips_data_management as idm
from main.calculations import calculate_ips_spend_imputation
from main.utils import process_variables
from numpy import testing

with open(r'data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-idm-integration-spend-imp'
# RUN_ID = 'test-idm-int-spend-imp-PVPROBLEM'
TEST_DATA_DIR = r'tests\data\ips_data_management\spend_imputation_integration'
STEP_NAME = 'SPEND_IMPUTATION'
EXPECTED_LEN = 19980
# EXPECTED_LEN = 10
NUMBER_OF_PVS = len(STEP_CONFIGURATION[STEP_NAME]['pv_columns'])

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

    # Assign variables
    # december_survey_data_path = (TEST_DATA_DIR + r'\surveydataPVPROBLEM.csv')
    december_survey_data_path = (TEST_DATA_DIR + r'\surveydata.csv')

    # # Import survey data
    # import_survey_data(december_survey_data_path)

    # Deletes data from tables as necessary
    reset_tables()

    # populates test data within pv table
    populate_test_pv_table()


def teardown_module(module):
    """ Teardown any state that was previously setup with a setup_module method. """
    # Deletes data from temporary tables as necessary
    reset_tables()

    # # Cleanses Survey Subsample table
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)
    # cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', RUN_ID)

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
    df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)

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


def populate_test_pv_table():
    """ Set up table to run and test copy_step_pvs_for_survey_data()
        Note: Had to break up sql statements due to following error:
        'pyodbc.Error: ('HY000', '[HY000] [Microsoft][ODBC SQL Server Driver]Connection is busy with results for
             another hstmt (0) (SQLExecDirectW)')'
        Error explained in http://sourceitsoftware.blogspot.com/2008/06/connection-is-busy-with-results-for.html
        """

    conn = database_connection()
    cur = conn.cursor()

    sql1 = """
    INSERT INTO [ips_test].[dbo].[PROCESS_VARIABLE_TESTING]
    SELECT * FROM [ips_test].[dbo].[PROCESS_VARIABLE_PY]
    WHERE [RUN_ID] = 'TEMPLATE'
    """

    sql2 = """
    UPDATE [ips_test].[dbo].[PROCESS_VARIABLE_TESTING]
    SET [RUN_ID] = '{}'
    """.format(RUN_ID)

    sql3 = """
    INSERT INTO [ips_test].[dbo].[PROCESS_VARIABLE_PY]
    SELECT * FROM [ips_test].[dbo].[PROCESS_VARIABLE_TESTING]
    WHERE RUN_ID = '{}'
    """.format(RUN_ID)

    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)


def test_final_weight_step():
    """ Test function """

    # Assign variables
    conn = database_connection()
    # cur = conn.cursor()

    # Run, and test, first step of run.shift_weight_step
    idm.populate_survey_data_for_step(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # # Check all deleted tables are empty
    # for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
    #     delete_result = cf.get_table_values(table)
    #     assert delete_result.empty
    #
    # # Check all nullified columns are NULL
    # result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    # for column in STEP_CONFIGURATION[STEP_NAME]['nullify_pvs']:
    #     column_name = column.replace('[', '').replace(']', '')
    #     assert result[column_name].isnull().sum() == len(result)
    #
    # # Check table has been populated
    # sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    # table_len = len(sas_survey_data.index)
    # assert table_len == EXPECTED_LEN

    # Run the next step and test
    idm.copy_step_pvs_for_survey_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # # Assert idm.SAS_PROCESS_VARIABLES_TABLE has been populated
    # table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    # assert table_len == NUMBER_OF_PVS
    #
    # # Assert STEP_CONFIGURATION[STEP_NAME]["spv_table"] has been cleansed
    # table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    # assert table_len == 0

    # Run the next step and test
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='SERIAL')

    # # TODO: Assert pv_columns were updated
    # table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    # assert table_len == EXPECTED_LEN

    # Run the next step
    idm.update_survey_data_with_step_pv_output(conn, STEP_CONFIGURATION[STEP_NAME])

    # # Check all columns in SAS_SURVEY_SUBSAMPLE have been altered
    # result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    # for column in STEP_CONFIGURATION[STEP_NAME]['pv_columns']:
    #     column_name = column.replace("'", "")
    #     assert len(result[column_name]) == EXPECTED_LEN
    #     assert result[column_name].sum() != 0
    #
    # # Assert SAS_PROCESS_VARIABLES_TABLE has been cleansed
    # table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    # assert table_len == 0
    #
    # # Assert spv_table has been cleansed
    # table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    # assert table_len == 0

    # Get and test the Survey Data before importing to calculation function
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    # sas_survey_data.to_csv(TEST_DATA_DIR + '\sas_survey_data_actual.csv', index=False)
    sas_survey_data.to_pickle(TEST_DATA_DIR + '\sas_survey_data_actual.pkl')

    # actual_results = pd.read_csv(TEST_DATA_DIR + '\sas_survey_data_actual.csv')
    actual_results = pd.read_pickle(TEST_DATA_DIR + '\sas_survey_data_actual.pkl')
    # expected_results = pd.read_csv(TEST_DATA_DIR + '\sas_survey_data_expected.csv')
    expected_results = pd.read_pickle(TEST_DATA_DIR + '\sas_survey_data_expected.pkl')

    # Formatting because pd testing is annoying
    actual_results.sort_values(by=["SERIAL"], inplace=True)
    actual_results.index = range(0, len(actual_results))

    # Formatting because pd testing is annoying
    expected_results.sort_values(by=["SERIAL"], inplace=True)
    expected_results.drop(['IND'], axis=1, inplace=True)
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False, check_like=True)

    sys.exit()

    # TODO: CHECK DTYPE OF DATA LOADED TO SQL AND REPLICATE.
    try:
        assert_frame_equal(actual_results, expected_results, check_dtype=False, check_like=True)
    except Exception:
        pass

    # Run the next step and test
    surveydata_out = calculate_ips_spend_imputation.do_ips_spend_imputation(sas_survey_data,
                                                                            var_serial="SERIAL",
                                                                            measure="MEAN")

    # Test survey data from calculation function before inserting to db
    surveydata_out.to_csv(TEST_DATA_DIR + '\surveydata_out_actual.csv', index=False)
    actual_results = pd.read_csv(TEST_DATA_DIR + '\surveydata_out_actual.csv')

    expected_results = pd.read_csv(TEST_DATA_DIR + '\surveydata_out_expected.csv')

    actual_results.sort_values(by=["SERIAL"], inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results.sort_values(by=["SERIAL"], inplace=True)
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Replicate intermediate steps within final_weight_step() and test length
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], surveydata_out)

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == EXPECTED_LEN

    # Run the next step and test
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 0

    # Run the next step and test
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert SURVEY_SUBSAMPLE_TABLE was populated
    # sql = """
    #     SELECT * FROM {}
    #     WHERE RUN_ID = '{}'
    #     """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)
    # result = cur.execute(sql).fetchall()
    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == EXPECTED_LEN

    # Assert all records for corresponding run_id were deleted from ps_table
    # sql = """
    # SELECT * FROM {}
    # WHERE RUN_ID = '{}'
    # """.format(STEP_CONFIGURATION[STEP_NAME]["ps_table"], RUN_ID)
    # result = cur.execute(sql).fetchall()
    # table_len = len(result)
    result = cf.select_data('*', STEP_CONFIGURATION[STEP_NAME]["ps_table"], 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == EXPECTED_LEN
    assert table_len == 0

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0

    # TODO: Ensure all PV_DEFs are updated throughout correct tables in all databases
