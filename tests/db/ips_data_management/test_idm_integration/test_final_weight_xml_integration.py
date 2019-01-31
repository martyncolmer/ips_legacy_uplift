import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from tests import common_testing_functions as ctf
from utils import common_functions as cf
from main.db import data_management as idm
from main.calculations import calculate_ips_final_weight

with open(r'data/steps_configuration.json') as config_file:
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

    # Assign variables
    december_survey_data_path = (TEST_DATA_DIR + r'\surveydata.csv')

    # Deletes data from tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data
    ctf.import_survey_data_into_database(december_survey_data_path, RUN_ID)


def teardown_module(module):
    """ Teardown any state that was previously setup with a setup_module method. """
    # Deletes data from temporary tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


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
                                                                                         serial_num='SERIAL',
                                                                                         shift_weight='SHIFT_WT',
                                                                                         non_response_weight='NON_RESPONSE_WT',
                                                                                         min_weight='MINS_WT',
                                                                                         traffic_weight='TRAFFIC_WT',
                                                                                         unsampled_weight='UNSAMP_TRAFFIC_WT',
                                                                                         imbalance_weight='IMBAL_WT',
                                                                                         final_weight='FINAL_WT')

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

    # table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]['ps_table']))
    table_len = len(cf.select_data('*', STEP_CONFIGURATION[STEP_NAME]['ps_table'], 'RUN_ID', RUN_ID))
    assert table_len == calculate_ips_final_weight.NUMBER_RECORDS_DISPLAYED