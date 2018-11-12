import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from tests import common_testing_functions as ctf
from main.io import CommonFunctions as cf
from main.io import ips_data_management as idm
from main.calculations.calculate_ips_rail_imputation import do_ips_railex_imp
from main.utils import process_variables

with open(r'data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-idm-integration-rail-imp'
TEST_DATA_DIR = r'tests\data\ips_data_management\rail_imputation_integration'
STEP_NAME = 'RAIL_IMPUTATION'
EXPECTED_LEN = 19980
NUMBER_OF_PVS = 3
PV_RUN_ID = 'TEMPLATE'

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

    # Deletes data from tables as necessary.
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data.
    ctf.import_survey_data_into_database(december_survey_data_path, RUN_ID)

    # Populates test data within pv table.
    conn = database_connection()
    ctf.populate_test_pv_table(conn, RUN_ID, PV_RUN_ID)


def teardown_module(module):
    """ Teardown any state that was previously setup with a setup_module method. """
    # Deletes data from temporary tables as necessary.
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table.
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    # Play audio notification to indicate test is complete and print duration for performance.
    cf.beep()
    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


# @pytest.mark.skip(reason="Test failing on rounding error")
def test_rail_imputation_step():
    """ Test function. """

    # Assign variables.
    conn = database_connection()

    # Run, and test, first step.
    idm.populate_survey_data_for_step(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all deleted tables are empty.
    for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check all nullified columns are NULL.
    survey_subsample = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    for column in STEP_CONFIGURATION[STEP_NAME]['nullify_pvs']:
        column_name = column.replace('[', '').replace(']', '')
        assert survey_subsample[column_name].isnull().sum() == len(survey_subsample)

    # Check table has been populated.
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    # Run the next step and test.
    idm.copy_step_pvs_for_survey_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert idm.SAS_PROCESS_VARIABLES_TABLE has been populated.
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == NUMBER_OF_PVS

    # Assert STEP_CONFIGURATION[STEP_NAME]["spv_table"] has been cleansed.
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == 0

    # Run the next step and test.
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name=STEP_CONFIGURATION[STEP_NAME]["spv_table"],
                              in_id='serial')

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == EXPECTED_LEN

    # Run the next step.
    idm.update_survey_data_with_step_pv_output(conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all columns in SAS_SURVEY_SUBSAMPLE have been altered.
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    for column in STEP_CONFIGURATION[STEP_NAME]['pv_columns']:
        column_name = column.replace("'", "")
        assert len(sas_survey_data[column_name]) == EXPECTED_LEN
        assert sas_survey_data[column_name].sum() != 0

    # Assert SAS_PROCESS_VARIABLES_TABLE has been cleansed.
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 0

    # Assert spv_table has been cleansed.
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == 0

    # Run the next step and test.
    surveydata_out = do_ips_railex_imp(sas_survey_data,
                                       var_serial="SERIAL",
                                       var_final_weight="FINAL_WT",
                                       minimum_count_threshold=30)

    # Insert the data generated by the calculate function into the database
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], surveydata_out)

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == EXPECTED_LEN

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

    # Select the newly updated weight column from the dataframe and ensure it matches the expected weights.
    df_survey_actual = df_survey_actual
    df_survey_target = df_survey_target

    assert_frame_equal(df_survey_actual, df_survey_target, check_dtype=False)

    # Run the next step and test.
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert SAS_SURVEY_SUBSAMPLE was populated.
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    print(table_len)
    assert table_len == EXPECTED_LEN

    # Assert table was cleansed accordingly.
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 0

    # Run the next step and test.
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert SURVEY_SUBSAMPLE_TABLE was populated.
    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == EXPECTED_LEN

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed.
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0