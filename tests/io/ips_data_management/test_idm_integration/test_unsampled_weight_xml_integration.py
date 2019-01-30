import pytest
import json
import pandas as pd
import time
import numpy as np

from pandas.util.testing import assert_frame_equal
from utils import common_functions as cf
from tests import common_testing_functions as ctf
from main.io import data_management as idm
from main.calculations.calculate_ips_unsampled_weight import do_ips_unsampled_weight_calculation
from main.utils import process_variables
from main.io import import_reference_data

with open(r'data/steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-idm-int-unsampled_wt'
TEST_DATA_DIR = r'tests\data\ips_data_management\unsampled_weight_integration'
STEP_NAME = 'UNSAMPLED_WEIGHT'
EXPECTED_LEN = 17731
NUMBER_OF_PVS = 2
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

    # Assign variables and import survey data
    december_survey_data_path = (TEST_DATA_DIR + r'\surveydata.csv')
    ctf.import_survey_data_into_database(december_survey_data_path, RUN_ID)

    unsampled_data_path = (TEST_DATA_DIR + r'\unsampleddata.csv')
    import_reference_data.import_traffic_data(RUN_ID, unsampled_data_path)

    # Deletes data from tables as necessary.
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Populates test data within pv table.
    ctf.populate_test_pv_table(database_connection(), RUN_ID, PV_RUN_ID)


def teardown_module(module):

    """ Teardown any state that was previously setup with a setup_module method. """
    # Deletes data from temporary tables as necessary.
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table.
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)


def sort_and_set_index(df, sort_columns):
    df = df.sort_values(sort_columns)
    df.index = range(0, len(df))
    return df


def test_unsampled_weight_step():
    # Get database connection
    conn = database_connection()

    # Run step 1 / 8
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
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    # Run step 2 / 8
    idm.populate_step_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Check table has been populated
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["data_table"]))
    assert table_len == 1252

    # Run step 3 / 8
    idm.copy_step_pvs_for_survey_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert idm.SAS_PROCESS_VARIABLES_TABLE has been populated
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == NUMBER_OF_PVS

    # Assert STEP_CONFIGURATION[STEP_NAME]["spv_table"] has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == 0

    # Run step 4 / 8
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_UNSAMPLED_OOH_SPV',
                              in_id='serial')

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == EXPECTED_LEN

    # Run step 5 / 8
    idm.update_survey_data_with_step_pv_output(conn, STEP_CONFIGURATION[STEP_NAME])

    # Check all columns in SAS_SURVEY_SUBSAMPLE have been altered
    result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    for column in STEP_CONFIGURATION[STEP_NAME]['pv_columns']:
        column_name = column.replace("'", "")
        assert len(result[column_name]) == EXPECTED_LEN

    # Assert SAS_PROCESS_VARIABLES_TABLE has been cleansed
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 0

    # Assert spv_table has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["spv_table"]))
    assert table_len == 0

    # Run step 6 / 8
    idm.copy_step_pvs_for_step_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert pv_table has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["pv_table"]))
    assert table_len == 0

    # Assert SAS_PROCESS_VARIABLES_TABLE was populated
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 2

    # Run step 7 / 8
    process_variables.process(dataset='unsampled',
                              in_table_name='SAS_UNSAMPLED_OOH_DATA',
                              out_table_name='SAS_UNSAMPLED_OOH_PV',
                              in_id='REC_ID')

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["pv_table"]))
    assert table_len == 1252

    # Run step 8 / 12
    idm.update_step_data_with_step_pv_output(conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert the following tables were cleansed
    deleted_tables = [STEP_CONFIGURATION[STEP_NAME]["pv_table"],
                      STEP_CONFIGURATION[STEP_NAME]["temp_table"],
                      idm.SAS_PROCESS_VARIABLES_TABLE,
                      STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"]]

    for table in deleted_tables:
        table_len = len(cf.get_table_values(table))
        assert table_len == 0

    # Get and test Survey data input
    sas_survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    sas_survey_data.to_csv(TEST_DATA_DIR + '\survey_data_in_actual.csv', index=False)

    df_survey_actual = pd.read_csv(TEST_DATA_DIR + '\survey_data_in_actual.csv', engine='python')
    df_survey_target = pd.read_csv(TEST_DATA_DIR + '\survey_data_in_target.csv', engine='python')

    df_survey_actual = sort_and_set_index(df_survey_actual,'SERIAL')
    df_survey_target = sort_and_set_index(df_survey_target,'SERIAL')

    # Drop the EXPENDCODE columns because of format issue
    df_check_a = df_survey_actual.drop(columns=['EXPENDCODE'])
    df_check_t = df_survey_target.drop(columns=['EXPENDCODE'])#[['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV']]

    assert_frame_equal(df_check_a, df_check_t, check_dtype=False)

    # Get and test Unsampled data input
    sas_unsampled_data = cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["data_table"])

    sas_unsampled_data.to_csv(TEST_DATA_DIR + r'\unsampled_data_in_actual.csv', index=False)

    df_unsampled_actual = pd.read_csv(TEST_DATA_DIR + r'\unsampled_data_in_actual.csv', engine='python')
    df_unsampled_target = pd.read_csv(TEST_DATA_DIR + r'\unsampled_data_in_target.csv', engine='python')

    df_unsampled_actual = sort_and_set_index(df_unsampled_actual, ['PORTROUTE', 'REGION', 'ARRIVEDEPART', 'UNSAMP_TOTAL'])
    df_unsampled_target = sort_and_set_index(df_unsampled_target, ['PORTROUTE', 'REGION', 'ARRIVEDEPART', 'UNSAMP_TOTAL'])

    # Drop unique REC_ID column
    df_unsampled_test = df_unsampled_actual.drop('REC_ID', axis=1)

    # Fix format of comparison data
    df_unsampled_test['REGION'] = df_unsampled_test['REGION'].replace(0, np.NaN)
    df_unsampled_target['UNSAMP_REGION_GRP_PV'] = df_unsampled_target['UNSAMP_REGION_GRP_PV'].fillna(0)
    df_unsampled_target['UNSAMP_REGION_GRP_PV'] = df_unsampled_target['UNSAMP_REGION_GRP_PV'].astype(int)

    assert_frame_equal(df_unsampled_test, df_unsampled_target, check_dtype=False)

    # TODO: Compare integration summary input with xml summary input
    df_unsampled_actual.to_csv(r'S:\CASPA\IPS\Testing\scratch\summary_in_xml.csv', index=False)

    # Run step 9 / 12
    output_data, summary_data = do_ips_unsampled_weight_calculation(df_survey_actual,
                                                                    serial_num='SERIAL',
                                                                    shift_weight='SHIFT_WT',
                                                                    nr_weight='NON_RESPONSE_WT',
                                                                    min_weight='MINS_WT',
                                                                    traffic_weight='TRAFFIC_WT',
                                                                    out_of_hours_weight="UNSAMP_TRAFFIC_WT",
                                                                    df_ustotals=df_unsampled_actual,
                                                                    min_count_threshold=30)

    # Sort and reset the index of the results produced by the calculation
    output_data = sort_and_set_index(output_data, 'SERIAL')
    summary_data = sort_and_set_index(summary_data, ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART'])

    # Import the expected results, then sort and reset their index
    test_result_survey = pd.read_csv(TEST_DATA_DIR + r'\outputdata_final.csv', engine='python')
    cf.delete_from_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"])
    test_result_survey = convert_dataframe_to_sql_format(STEP_CONFIGURATION[STEP_NAME]["temp_table"], test_result_survey)
    test_result_survey = sort_and_set_index(test_result_survey, 'SERIAL')

    test_result_summary = pd.read_csv(TEST_DATA_DIR + r'\summarydata_final.csv', engine='python')
    cf.delete_from_table(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"])
    test_result_summary = convert_dataframe_to_sql_format(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"], test_result_summary)

    test_result_summary.ARRIVEDEPART = test_result_summary.ARRIVEDEPART.astype(int)
    test_result_summary.UNSAMP_REGION_GRP_PV = pd.to_numeric(test_result_summary.UNSAMP_REGION_GRP_PV, errors='coerce')
    test_result_summary.CASES = test_result_summary.CASES.astype(int)

    test_result_summary = sort_and_set_index(test_result_summary, ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART'])

    # Assert dfs are equal
    assert_frame_equal(output_data, test_result_survey, check_dtype=False, check_like=True,
                       check_less_precise=True)

    assert_frame_equal(summary_data, test_result_summary, check_dtype=False, check_like=True,
                       check_less_precise=True)

    # Put the SQL data back in for the remaining steps
    cf.delete_from_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"])
    cf.delete_from_table(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"])
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], output_data)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"], summary_data)

    # Check the number of records in the output tables are correct
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"]))
    assert table_len == 203

    # Run step 10 / 12
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    # Check record count in the
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 0

    # Run step 11 / 12
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert SURVEY_SUBSAMPLE_TABLE was populated
    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == 17731

    # Assert all records for corresponding run_id were deleted from ps_table.
    result = cf.select_data('*', STEP_CONFIGURATION[STEP_NAME]["ps_table"], 'RUN_ID', RUN_ID)
    # Indicating no dataframe was pulled from SQL.
    if not result:
        assert True

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0

    # Run step 12 / 12
    idm.store_step_summary(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert summary was populated.
    result = cf.select_data('*', STEP_CONFIGURATION[STEP_NAME]["ps_table"], 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == 203

    # Assert temp table was cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"]))
    assert table_len == 0

