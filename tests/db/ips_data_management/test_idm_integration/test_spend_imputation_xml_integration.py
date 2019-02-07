import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from ips import common_functions as cf, process_variables
from tests import common_testing_functions as ctf
from ips import data_management as idm
from ips import calculate_ips_spend_imputation

with open(r'data/steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-idm-integration-spend-imp'
PV_RUN_ID = 'TEMPLATE'
TEST_DATA_DIR = r'tests\data\ips_data_management\spend_imputation_integration'
STEP_NAME = 'SPEND_IMPUTATION'
EXPECTED_LEN = 19980
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
    december_survey_data_path = (TEST_DATA_DIR + r'\surveydata.csv')

    # Deletes data from tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data
    ctf.import_survey_data_into_database(december_survey_data_path, RUN_ID)

    # populates test data within pv table
    conn = database_connection()
    ctf.populate_test_pv_table(conn, RUN_ID, PV_RUN_ID)


def teardown_module(module):
    """ Teardown any state that was previously setup with a setup_module method. """
    # Deletes data from temporary tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # # Cleanses Survey Subsample table
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


def test_spend_weight_step():
    """ Test function """

    # Assign variables
    conn = database_connection()

    # Run, and test, first step of run.shift_weight_step
    idm.populate_survey_data_for_step(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all deleted tables are empty
    for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check all nullified columns are NULL
    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    for column in STEP_CONFIGURATION[STEP_NAME]['nullify_pvs']:
        column_name = column.replace('[', '').replace(']', '')
        assert result[column_name].isnull().sum() == len(result)

    # Check table has been populated
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    table_len = len(sas_survey_data.index)
    assert table_len == EXPECTED_LEN

    # Run the next step and test
    idm.copy_step_pvs_for_survey_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert idm.SAS_PROCESS_VARIABLES_TABLE has been populated
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == NUMBER_OF_PVS

    # Assert STEP_CONFIGURATION[STEP_NAME]["spv_table"] has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == 0

    # Run the next step and test
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='SERIAL')

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == EXPECTED_LEN

    # Run the next step
    idm.update_survey_data_with_step_pv_output(conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all columns in SAS_SURVEY_SUBSAMPLE have been altered
    result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    for column in STEP_CONFIGURATION[STEP_NAME]['pv_columns']:
        column_name = column.replace("'", "")
        assert len(result[column_name]) == EXPECTED_LEN
        assert result[column_name].sum() != 0

    # Assert SAS_PROCESS_VARIABLES_TABLE has been cleansed
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 0

    # Assert spv_table has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == 0

    # Get Survey Data before importing to calculation function
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Run the next step and test
    surveydata_out = calculate_ips_spend_imputation.do_ips_spend_imputation(sas_survey_data,
                                                                            var_serial="SERIAL",
                                                                            measure="mean")

    # Replicate intermediate steps within final_weight_step() and test length
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], surveydata_out)

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 5134

    # Extract our test results from the survey and summary tables then write the results to csv.
    df_survey_actual = cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"])

    # Read in both the target datasets and the results we previously wrote out then sort them on specified columns.
    df_survey_actual.to_csv(TEST_DATA_DIR + '\sas_survey_subsample_actual.csv', index=False)

    df_survey_actual = pd.read_csv(TEST_DATA_DIR + '\sas_survey_subsample_actual.csv').sort_values('SERIAL')
    df_survey_target = pd.read_csv(TEST_DATA_DIR + '\sas_survey_subsample_target.csv', encoding='ANSI').sort_values(
        'SERIAL')

    # Reset the dataframe's index before comparing the outputs.
    df_survey_actual.index = range(0, len(df_survey_actual))
    df_survey_target.index = range(0, len(df_survey_target))

    assert_frame_equal(df_survey_actual, df_survey_target, check_dtype=False)

    # Run the next step and test
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 0

    # Run the next step and test
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    result.to_csv(r'S:\CASPA\IPS\Testing\scratch\spend_integration_testing_survey_subsample.csv')
    table_len = result.shape[0]
    assert table_len == EXPECTED_LEN

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0
