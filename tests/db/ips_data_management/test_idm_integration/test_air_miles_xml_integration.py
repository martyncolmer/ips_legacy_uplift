import pytest
import json
import pandas as pd
import time

from tests import common_testing_functions as ctf
from pandas.util.testing import assert_frame_equal
from ips.utils import common_functions as cf
from ips.db import data_management as idm
from ips.calculations import calculate_ips_airmiles

with open('data/steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test_air_miles_xml'
TEST_DATA_DIR = r'tests\data\ips_data_management\air_miles_integration'
STEP_NAME = 'AIR_MILES'

ist = time.time()
print("Module level start time: {}".format(ist))

@pytest.fixture(scope='module')
def database_connection():
    '''
    This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again.
    '''
    return cf.get_sql_connection()


def setup_module(module):
    """ setup any state specific to the execution of the given module."""

    # Assign variables
    december_survey_data_path = (TEST_DATA_DIR + r'\surveydata.csv')

    # Deletes data from tables as necessary.
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data.
    ctf.import_survey_data_into_database(december_survey_data_path, RUN_ID)

    print("Setup")


def teardown_module(module):
    # Delete any previous records from the Survey_Subsample tables for the given run ID
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table.
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    print("Teardown")


def test_air_miles_step():

    # Get database connection
    conn = database_connection()

    # Run step 1 / 4
    idm.populate_survey_data_for_step(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all deleted tables are empty
    for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check table has been populated
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 19980

    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Run step 2 / 4
    surveydata_out = calculate_ips_airmiles.do_ips_airmiles_calculation(sas_survey_data,
                                                                        var_serial='SERIAL')

    surveydata_out.to_csv(TEST_DATA_DIR + r'\\airmiles_actual.csv', index=False)

    df_survey_actual = pd.read_csv(TEST_DATA_DIR + r'\\airmiles_actual.csv', engine='python').sort_values('SERIAL')
    df_survey_expected = pd.read_csv(TEST_DATA_DIR + r'\\airmiles_expected.csv', engine='python').sort_values('SERIAL')

    # Reset the dataframe's index before comparing the outputs.
    df_survey_actual.index = range(0, len(df_survey_actual))
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(df_survey_actual,df_survey_expected, check_dtype=False)

    # Insert the data generated by the calculate function into the database
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], surveydata_out)

    # Run step 3 / 4
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    # Run step 4 / 4
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    print("Import runtime: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - ist))))

