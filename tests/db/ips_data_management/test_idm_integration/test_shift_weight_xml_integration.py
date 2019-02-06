import time

import json
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from ips.calculations import calculate_ips_shift_weight
from ips.utils import common_functions as cf, process_variables
from ips.db import data_management as idm
from tests import common_testing_functions as ctf

with open(r'data/steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-idm-integration-shift-wt'
TEST_DATA_DIR = r'tests\data\ips_data_management\shift_weight_integration'
STEP_NAME = 'SHIFT_WEIGHT'
EXPECTED_LEN = 19980
NUMBER_OF_PVS = 5
PV_RUN_ID = 'TEMPLATE'

START_TIME = time.time()
print("Module level start time: {}".format(START_TIME))


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
    # deletes data from tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey and external data.
    import_data_dir = r'tests\data\import_data\dec'
    ctf.import_test_data_into_database(import_data_dir, RUN_ID)

    # populates test data within pv table
    conn = database_connection()
    ctf.populate_test_pv_table(conn, RUN_ID, PV_RUN_ID)


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
        method.
        """
    # deletes data from tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table.
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


def test_shift_weight_step():
    # Assign variables
    conn = database_connection()

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
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    # Run the next step and test
    idm.populate_step_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Check table has been populated
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["data_table"]))
    assert table_len == 372

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
                              out_table_name='SAS_SHIFT_SPV',
                              in_id='serial')

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

    # run and test idm.copy_step_pvs_for_step_data
    idm.copy_step_pvs_for_step_data(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert pv_table has been cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["pv_table"]))
    assert table_len == 0

    # Assert SAS_PROCESS_VARIABLES_TABLE was populated
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 3

    # Run the next step and test
    process_variables.process(dataset='shift',
                              in_table_name='SAS_SHIFT_DATA',
                              out_table_name='SAS_SHIFT_PV',
                              in_id='REC_ID')

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["pv_table"]))
    assert table_len == 372

    # Run the next step and test
    idm.update_step_data_with_step_pv_output(conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert data table was populated
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["data_table"]))
    assert table_len == 372

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

    df_survey_actual = pd.read_csv(TEST_DATA_DIR + '\survey_data_in_actual.csv', engine='python').sort_values('SERIAL')
    df_survey_target = pd.read_csv(TEST_DATA_DIR + '\survey_data_in_target.csv', engine='python').sort_values('SERIAL')

    # Formatting issues
    df_survey_actual.drop(['EXPENDCODE'], axis=1, inplace=True)
    df_survey_target.drop(['EXPENDCODE'], axis=1, inplace=True)

    df_survey_actual['SHIFT_PORT_GRP_PV'] = df_survey_actual['SHIFT_PORT_GRP_PV'].apply(pd.to_numeric, errors='coerce')
    df_survey_target['SHIFT_PORT_GRP_PV'] = df_survey_target['SHIFT_PORT_GRP_PV'].apply(pd.to_numeric, errors='coerce')

    df_survey_actual['SHIFT_PORT_GRP_PV'].fillna('LHR Transi', inplace=True)
    df_survey_target['SHIFT_PORT_GRP_PV'].fillna('LHR Transi', inplace=True)

    # Reset the dataframe's index before comparing the outputs.
    df_survey_actual.index = range(0, len(df_survey_actual))
    df_survey_target.index = range(0, len(df_survey_target))

    assert_frame_equal(df_survey_actual, df_survey_target, check_dtype=False)

    # Get and test Shift data input
    sas_shift_data = cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["data_table"])

    sas_shift_data.to_csv(TEST_DATA_DIR + '\shift_data_in_actual.csv', index=False)

    cols = ['PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'TOTAL', 'AM_PM_NIGHT',
            'SHIFT_PORT_GRP_PV', 'AM_PM_NIGHT_PV', 'WEEKDAY_END_PV']

    df_shift_actual = pd.read_csv(TEST_DATA_DIR + '\shift_data_in_actual.csv', engine='python')
    df_shift_actual.sort_values(by=cols, inplace=True)
    df_shift_actual.drop(['REC_ID'], axis=1, inplace=True)
    df_shift_actual[cols] = df_shift_actual[cols].apply(pd.to_numeric, errors='coerce', downcast='float')
    df_shift_actual.index = range(0, len(df_shift_actual))

    df_shift_target = pd.read_csv(TEST_DATA_DIR + '\shift_data_in_target.csv', engine='python')
    df_shift_target.sort_values(by=cols, inplace=True)
    df_shift_target.drop(['REC_ID'], axis=1, inplace=True)
    df_shift_target[cols] = df_shift_target[cols].apply(pd.to_numeric, errors='coerce', downcast='float')
    df_shift_target.index = range(0, len(df_shift_target))

    assert_frame_equal(df_shift_actual, df_shift_target, check_dtype=False, check_like=True)

    # Run the next step and test
    surveydata_out, summary_out = calculate_ips_shift_weight.do_ips_shift_weight_calculation(sas_survey_data,
                                                                                             sas_shift_data,
                                                                                             var_serialNum='SERIAL',
                                                                                             var_shiftWeight='SHIFT_WT')

    # Test survey data from calculation function before inserting to db
    surveydata_out.to_csv(TEST_DATA_DIR + '\surveydata_out_actual.csv', index=False)
    actual_results = pd.read_csv(TEST_DATA_DIR + '\surveydata_out_actual.csv')

    expected_results = pd.read_csv(TEST_DATA_DIR + '\surveydata_out_target.csv')

    actual_results.sort_values(by=["SERIAL"], inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results.sort_values(by=["SERIAL"], inplace=True)
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    cols = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS',
            'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT']

    # Test summary data from calculation function before inserting to db
    summary_out.to_csv(TEST_DATA_DIR + '\summary_out_actual.csv', index=False)
    actual_results = pd.read_csv(TEST_DATA_DIR + '\summary_out_actual.csv')

    expected_results = pd.read_csv(TEST_DATA_DIR + '\summary_out_expected.csv')

    actual_results.sort_values(by=cols, inplace=True)
    actual_results[cols] = actual_results[cols].apply(pd.to_numeric, errors='coerce', downcast='float')
    actual_results.index = range(0, len(actual_results))

    expected_results.sort_values(by=cols, inplace=True)
    expected_results[cols] = expected_results[cols].apply(pd.to_numeric, errors='coerce', downcast='float')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Replicate intermediate steps within run.shift_weight_step() and test length
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["temp_table"], surveydata_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"], summary_out)

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"]))
    assert table_len == 424

    # Run the next step and test
    idm.update_survey_data_with_step_results(conn, STEP_CONFIGURATION[STEP_NAME])

    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["temp_table"]))
    assert table_len == 0

    # Run the next step and test
    idm.store_survey_data_with_step_results(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert SURVEY_SUBSAMPLE_TABLE was populated
    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == 21638

    # Assert all records for corresponding run_id were deleted from ps_table.
    result = cf.select_data('*', STEP_CONFIGURATION[STEP_NAME]["ps_table"], 'RUN_ID', RUN_ID)
    # Indicating no dataframe was pulled from SQL.
    if not result:
        assert True

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0

    # Run the next step and test
    idm.store_step_summary(RUN_ID, conn, STEP_CONFIGURATION[STEP_NAME])

    # Assert summary was populated.
    result = cf.select_data('*', STEP_CONFIGURATION[STEP_NAME]["ps_table"], 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == 424

    # Assert temp table was cleansed
    table_len = len(cf.get_table_values(STEP_CONFIGURATION[STEP_NAME]["sas_ps_table"]))
    assert table_len == 0
