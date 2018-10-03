import pytest
import json
import pandas as pd
import time
import numpy as np

from pandas.util.testing import assert_frame_equal
from main import main as main_run
from main.io import CommonFunctions as cf
from main.io import import_data
from main.io import import_traffic_data
from main.io import ips_data_management as idm

with open(r'data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-main'
PV_RUN_ID = 'TEMPLATE'
TEST_DATA_DIR = r'tests\data'

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
    # TODO: Uncomment this before merge
    # Import external and survey data
    # import_data_into_database()

    # populates test data within pv table
    populate_test_pv_table()


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
        method.
        """
    # TODO: Add this to the list before merge
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = ['[dbo].[PROCESS_VARIABLE_PY]',
                         '[dbo].[PROCESS_VARIABLE_TESTING]']
                         # '[dbo].[TRAFFIC_DATA]',
                         # '[dbo].[SHIFT_DATA]',
                         # '[dbo].[NON_RESPONSE_DATA]',
                         # '[dbo].[UNSAMPLED_OOH_DATA]',
                         # idm.SURVEY_SUBSAMPLE_TABLE]

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list.
    for table in tables_to_cleanse:
        try:
            cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID)
        except Exception:
            continue

    # Play audio notification to indicate test is complete and print duration for performance.
    cf.beep()
    print("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - START_TIME))))


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
    INSERT INTO [PROCESS_VARIABLE_TESTING]
    SELECT * FROM [PROCESS_VARIABLE_PY]
    WHERE [RUN_ID] = '{}'
    """.format(PV_RUN_ID)

    sql2 = """
    UPDATE [PROCESS_VARIABLE_TESTING]
    SET [RUN_ID] = '{}'
    """.format(RUN_ID)

    sql3 = """
    INSERT INTO [PROCESS_VARIABLE_PY]
    SELECT * FROM [PROCESS_VARIABLE_TESTING]
    WHERE RUN_ID = '{}'
    """.format(RUN_ID)

    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)


def import_data_into_database():
    '''
    This function prepares all the data necessary to run all 14 steps.
    The input files have been edited to make sure data types match the database tables.
    Note that no process variables are uploaded and are expected to be present in the database.
    '''

    # Import data paths (these will be passed in through the user)
    survey_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\surveydata.csv'
    shift_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\Poss shifts Dec 2017.csv'
    nr_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\Dec17_NR.csv'
    sea_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\Sea Traffic Dec 2017.csv'
    tunnel_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\Traffic Dec 2017.csv'
    air_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\Air Sheet Dec 2017 VBA.csv'
    unsampled_data_path = TEST_DATA_DIR + r'\ips_data_management\shift_weight_integration\Unsampled Traffic Dec 2017.csv'

    # Cleanse
    cf.delete_from_table('SURVEY_SUBSAMPLE', 'RUN_ID', '=', RUN_ID)
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')

    # Import survey data function to go here
    import_data.import_survey_data(survey_data_path, RUN_ID)

    # Import Shift Data
    import_traffic_data.import_traffic_data(RUN_ID, shift_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, nr_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, unsampled_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, sea_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, tunnel_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, air_data_path)


def test_shift_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "SHIFT_WEIGHT"

    # Run Shift Weight step
    main_run.shift_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\shift_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\shift_weight\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS',
         'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS',
         'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT'])
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_non_response_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "NON_RESPONSE"

    # Run Shift Weight step
    main_run.non_response_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\non_response\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\non_response\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
         'GROSS_RESP', 'GNR', 'MEAN_NR_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
         'GROSS_RESP', 'GNR', 'MEAN_NR_WT'])
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)