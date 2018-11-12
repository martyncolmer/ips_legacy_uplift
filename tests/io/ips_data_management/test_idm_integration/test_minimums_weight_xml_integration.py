import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from tests import common_testing_functions as ctf
from main.io import import_traffic_data
from main.io import ips_data_management as idm
from main.utils import process_variables
from main.calculations import calculate_ips_minimums_weight


with open('data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test_minimums_weight_xml'
TEST_DATA_DIR = r'tests\data\ips_data_management\minimums_weight_step'
STEP_NAME = 'MINIMUMS_WEIGHT'
PV_RUN_ID = 'TEMPLATE'

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

    ist = time.time()

    # Deletes data from tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data
    survey_data_path = r'tests\data\ips_data_management\import_data\minimums_weight\surveydata.csv'
    ctf.import_survey_data_into_database(survey_data_path, RUN_ID)

    # Import external data
    import_data_dir = r'tests\data\import_data\dec'
    ctf.import_test_data_into_database(import_data_dir, RUN_ID, load_survey_data=False)

    # Populate test data within pv table
    conn = database_connection()
    ctf.populate_test_pv_table(conn, RUN_ID, PV_RUN_ID)

    print("Setup")


def teardown_module(module):
    # Delete any previous records from the Survey_Subsample tables for the given run ID
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table.
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    print("Teardown")


def test_minimums_weight_step():

    # Get database connection
    conn = database_connection()

    # Run step 1 / 8
    idm.populate_survey_data_for_step(RUN_ID, conn, STEP_CONFIGURATION["MINIMUMS_WEIGHT"])

    # Check all deleted tables are empty
    for table in STEP_CONFIGURATION['MINIMUMS_WEIGHT']['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check all nullified columns are NULL
    for column in STEP_CONFIGURATION['MINIMUMS_WEIGHT']['nullify_pvs']:
        column_name = column.replace('[', '').replace(']', '')
        result = cf.select_data(column_name, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
        assert result[column_name].isnull().sum() == len(result)

    # Check table has been populated
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 19980

    # Run step 2 / 8
    idm.copy_step_pvs_for_survey_data(RUN_ID, conn, STEP_CONFIGURATION["MINIMUMS_WEIGHT"])

    # Assert idm.SAS_PROCESS_VARIABLES_TABLE has been populated
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 3

    # Assert STEP_CONFIGURATION["SHIFT_WEIGHT"]["spv_table"] has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION["MINIMUMS_WEIGHT"]["spv_table"]))
    assert table_len == 0

    # Run step 3 / 8
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_MINIMUMS_SPV',
                              in_id='serial')

    table_len = len(cf.get_table_values(STEP_CONFIGURATION["MINIMUMS_WEIGHT"]["spv_table"]))
    assert table_len == 19980

    # Run step 4 / 8
    idm.update_survey_data_with_step_pv_output(conn, STEP_CONFIGURATION["MINIMUMS_WEIGHT"])

    # Assert SAS_PROCESS_VARIABLES_TABLE content has been deleted
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 0

    # Assert spv_table content has been deleted
    table_len = len(cf.get_table_values(STEP_CONFIGURATION["MINIMUMS_WEIGHT"]["spv_table"]))
    assert table_len == 0

    # Get and test Survey Data before importing to calculation function
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Run step 5 / 8
    surveydata_out, summary_out = calculate_ips_minimums_weight.do_ips_minweight_calculation(sas_survey_data,
                                                                                             var_serialNum='SERIAL',
                                                                                             var_shiftWeight='SHIFT_WT',
                                                                                             var_NRWeight='NON_RESPONSE_WT',
                                                                                             var_minWeight='MINS_WT')

    # Insert the data generated by the calculate function into the database
    cf.insert_dataframe_into_table(STEP_CONFIGURATION["MINIMUMS_WEIGHT"]["temp_table"], surveydata_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION["MINIMUMS_WEIGHT"]["sas_ps_table"], summary_out)

    # Run step 6 / 8
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION["MINIMUMS_WEIGHT"])

    # Run step 7 / 8
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION["MINIMUMS_WEIGHT"])

    # Run step 8 / 8
    idm.store_step_summary(RUN_ID, conn, STEP_CONFIGURATION["MINIMUMS_WEIGHT"])

    # Extract our test results from the survey and summary tables then write the results to csv.
    df_survey_actual = cf.select_data('*', '[dbo].[SURVEY_SUBSAMPLE]', 'RUN_ID', RUN_ID)
    df_summary_actual = cf.select_data('*', '[dbo].[PS_MINIMUMS]', 'RUN_ID', RUN_ID)

    df_survey_actual.to_csv(TEST_DATA_DIR + '\survey_subsample_actual.csv', index=False)
    df_summary_actual.to_csv(TEST_DATA_DIR + '\ps_minimums_actual.csv', index=False)

    # Read in both the target datasets and the results we previously wrote out then sort them on specified columns.
    df_survey_actual = pd.read_csv(TEST_DATA_DIR + '\survey_subsample_actual.csv', engine='python').sort_values('SERIAL')
    df_survey_target = pd.read_csv(TEST_DATA_DIR + '\survey_subsample_target_new_rounding.csv', engine='python').sort_values('SERIAL')
    df_summary_actual = pd.read_csv(TEST_DATA_DIR + '\ps_minimums_actual.csv', engine='python').sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    df_summary_target = pd.read_csv(TEST_DATA_DIR + '\ps_minimums_target_new_rounding.csv', engine='python').sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])

    # Reset the dataframe's index before comparing the outputs.
    df_survey_actual.index = range(0, len(df_survey_actual))
    df_survey_target.index = range(0, len(df_survey_target))
    df_summary_actual.index = range(0, len(df_summary_actual))
    df_summary_target.index = range(0, len(df_summary_target))

    # Drop column EXPENDCODE from survey data as not required for testing - ET 12/11/2018
    df_survey_actual.drop(['EXPENDCODE'], axis=1, inplace=True)
    df_survey_target.drop(['EXPENDCODE'], axis=1, inplace=True)

    # Ensure summary output is equal to expected summary output
    assert_frame_equal(df_summary_actual, df_summary_target, check_dtype=False,check_like=True, check_less_precise=True)

    # Select the newly updated weight column from the dataframe and ensure it matches the expected weights
    assert_frame_equal(df_survey_actual, df_survey_target, check_dtype=False)

    print("Import runtime: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - ist))))
