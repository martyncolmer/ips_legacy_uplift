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

    # Import external and survey data for October
    test_data_dir = r'tests\data\main\oct\import_data'
    run_id = 'test-main-october'

    cleanse_tables(run_id)
    import_data_into_database(survey_data_path=test_data_dir + r'\ips1710b_1.csv',
                              shift_data_path=test_data_dir + r'\Poss shifts Oct 2017.csv',
                              nr_data_path=test_data_dir + r'\Oct_NRMFS.csv',
                              sea_data_path=test_data_dir + r'\Sea Traffic Oct 2017.csv',
                              tunnel_data_path=test_data_dir + r'\Tunnel Traffic Oct 2017.csv',
                              air_data_path=test_data_dir + r'\Air Sheet Oct 2017 VBA2nd.csv',
                              unsampled_data_path=test_data_dir + r'\Unsampled Traffic Oct 20172nd.csv',
                              run_id=run_id)

    # populates test data within pv table for December
    populate_test_pv_table(run_id)

    # Import external and survey data for November
    test_data_dir = r'tests\data\main\nov\import_data'
    run_id = 'test-main-november'

    cleanse_tables(run_id)
    import_data_into_database(survey_data_path=test_data_dir + r'\ips1711h_1.csv',
                              shift_data_path=test_data_dir + r'\Poss shifts Nov 2017.csv',
                              nr_data_path=test_data_dir + r'\Nov_NRMFS.csv',
                              sea_data_path=test_data_dir + r'\Sea Traffic Nov 2017.csv',
                              tunnel_data_path=test_data_dir + r'\Tunnel Traffic Nov 2017.csv',
                              air_data_path=test_data_dir + r'\Air Sheet Nov 2017 VBA.csv',
                              unsampled_data_path=test_data_dir + r'\Unsampled Traffic Nov 2017.csv',
                              run_id=run_id)

    # populates test data within pv table for December
    populate_test_pv_table(run_id)

    # Import external and survey data for December
    test_data_dir = r'tests\data\main\dec\import_data'
    run_id = 'test-main-december'

    cleanse_tables(run_id)
    import_data_into_database(survey_data_path=test_data_dir + r'\surveydata.csv',
                              shift_data_path=test_data_dir + r'\Poss shifts Dec 2017.csv',
                              nr_data_path=test_data_dir + r'\Dec17_NR.csv',
                              sea_data_path=test_data_dir + r'\Sea Traffic Dec 2017.csv',
                              tunnel_data_path=test_data_dir + r'\Tunnel Traffic Dec 2017.csv',
                              air_data_path=test_data_dir + r'\Air Sheet Dec 2017 VBA.csv',
                              unsampled_data_path=test_data_dir + r'\Unsampled Traffic Dec 2017.csv',
                              run_id=run_id)

    # populates test data within pv table for December
    populate_test_pv_table(run_id)


def cleanse_tables(run_id):
    """ teardown any state that was previously setup with a setup_module
        method.
        """
    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = ['[PROCESS_VARIABLE_PY]',
                         '[PROCESS_VARIABLE_TESTING]',
                         '[TRAFFIC_DATA]',
                         '[SHIFT_DATA]',
                         '[NON_RESPONSE_DATA]',
                         '[UNSAMPLED_OOH_DATA]',
                         idm.SURVEY_SUBSAMPLE_TABLE]

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list.
    for table in tables_to_cleanse:
        try:
            cf.delete_from_table(table, 'RUN_ID', '=', run_id)
        except Exception:
            continue


def populate_test_pv_table(run_id):
    """ Set up table to run and test copy_step_pvs_for_survey_data()
        Note: Had to break up sql statements due to following error:
        'pyodbc.Error: ('HY000', '[HY000] [Microsoft][ODBC SQL Server Driver]Connection is busy with results for
             another hstmt (0) (SQLExecDirectW)')'
        Error explained in http://sourceitsoftware.blogspot.com/2008/06/connection-is-busy-with-results-for.html
        """

    conn = database_connection()
    cur = conn.cursor()

    cf.delete_from_table('PROCESS_VARIABLE_TESTING', 'RUN_ID', '=', run_id)
    cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

    sql1 = """
    INSERT INTO [PROCESS_VARIABLE_TESTING]
    SELECT * FROM [PROCESS_VARIABLE_PY]
    WHERE [RUN_ID] = 'TEMPLATE'
    """

    sql2 = """
    UPDATE [PROCESS_VARIABLE_TESTING]
    SET [RUN_ID] = '{}'
    WHERE [RUN_ID] = 'TEMPLATE'
    """.format(run_id)

    sql3 = """
    INSERT INTO [PROCESS_VARIABLE_PY]
    SELECT * FROM [PROCESS_VARIABLE_TESTING]
    WHERE RUN_ID = '{}'
    """.format(run_id)

    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)


def import_data_into_database(survey_data_path, shift_data_path, nr_data_path, sea_data_path, tunnel_data_path,
                              air_data_path, unsampled_data_path, run_id):
    '''
    This function prepares all the data necessary to run all 14 steps.
    The input files have been edited to make sure data types match the database tables.
    Note that no process variables are uploaded and are expected to be present in the database.
    '''

    # Cleanse
    cf.delete_from_table('SURVEY_SUBSAMPLE', 'RUN_ID', '=', run_id)
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')

    # Import survey data function to go here
    import_data.import_survey_data(survey_data_path, run_id)

    # Import External Data
    import_traffic_data.import_traffic_data(run_id, shift_data_path)
    import_traffic_data.import_traffic_data(run_id, nr_data_path)
    import_traffic_data.import_traffic_data(run_id, sea_data_path)
    import_traffic_data.import_traffic_data(run_id, tunnel_data_path)
    import_traffic_data.import_traffic_data(run_id, air_data_path)
    import_traffic_data.import_traffic_data(run_id, unsampled_data_path)


@pytest.mark.parametrize('run_id, expected_survey_results_path, expected_summary_results_path', [
    ('test-main-october',
     r'tests\data\main\oct\shift_weight\surveydata_out_expected.csv',
     r'tests\data\main\oct\shift_weight\summary_out_expected.csv'),
    ('test-main-november',
     r'tests\data\main\nov\shift_weight\surveydata_out_expected.csv',
     r'tests\data\main\nov\shift_weight\summary_out_expected.csv'),
    ('test-main-december',
     r'tests\data\main\dec\shift_weight\surveydata_out_expected.csv',
     r'tests\data\main\dec\shift_weight\summary_out_expected.csv'),
    ])
def test_shift_weight_step(run_id, expected_survey_results_path, expected_summary_results_path):
    # Assign variables
    conn = database_connection()
    step_name = "SHIFT_WEIGHT"

    # Run Shift Weight step
    main_run.shift_weight_step(run_id, conn)

    # Get results of Survey Data and compare
    sql_cols = "[SERIAL], [SHIFT_WT]"

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', run_id)
    expected_results = pd.read_csv(expected_survey_results_path, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\actual_results-' + run_id + '.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\expected_results-' + run_id + '.csv')

    if run_id == 'test-main-december':
        expected_results = expected_results[['SERIAL', 'SHIFT_WT']].copy()

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', run_id)
    expected_results = pd.read_csv(expected_summary_results_path, engine='python')

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


@pytest.mark.skip(reason="Because!")
def test_non_response_weight_steps():
    # Assign variables
    conn = database_connection()
    step_name = "NON_RESPONSE"

    # Run Shift Weight step
    main_run.non_response_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\non_response\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results['NR_PORT_GRP_PV'] = pd.to_numeric(actual_results['NR_PORT_GRP_PV'], errors='coerce')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results['NR_PORT_GRP_PV'] = pd.to_numeric(expected_results['NR_PORT_GRP_PV'], errors='coerce')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\non_response\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
         'GROSS_RESP', 'GNR', 'MEAN_NR_WT'])
    actual_results['NR_PORT_GRP_PV'] = pd.to_numeric(actual_results['NR_PORT_GRP_PV'], errors='coerce')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
         'GROSS_RESP', 'GNR', 'MEAN_NR_WT'])
    expected_results['NR_PORT_GRP_PV'] = pd.to_numeric(expected_results['NR_PORT_GRP_PV'], errors='coerce')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_minimums_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "MINIMUMS_WEIGHT"

    # Run Shift Weight step
    main_run.minimums_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    sql_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\minimums_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = sql_results.dropna(subset=['MINS_FLAG_PV'])
    actual_results['MINS_PORT_GRP_PV'] = pd.to_numeric(actual_results['MINS_PORT_GRP_PV'], errors='coerce')
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\minimums_weight\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['MINS_PORT_GRP_PV', 'ARRIVEDEPART', 'MINS_CTRY_GRP_PV', 'MINS_NAT_GRP_PV', 'MINS_CTRY_PORT_GRP_PV',
         'MINS_CASES', 'FULLS_CASES', 'PRIOR_GROSS_MINS', 'PRIOR_GROSS_FULLS', 'PRIOR_GROSS_ALL', 'MINS_WT', 'POST_SUM',
         'CASES_CARRIED_FWD'])
    actual_results['MINS_PORT_GRP_PV'] = pd.to_numeric(actual_results['MINS_PORT_GRP_PV'], errors='coerce')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['MINS_PORT_GRP_PV', 'ARRIVEDEPART', 'MINS_CTRY_GRP_PV', 'MINS_NAT_GRP_PV', 'MINS_CTRY_PORT_GRP_PV',
         'MINS_CASES', 'FULLS_CASES', 'PRIOR_GROSS_MINS', 'PRIOR_GROSS_FULLS', 'PRIOR_GROSS_ALL', 'MINS_WT', 'POST_SUM',
         'CASES_CARRIED_FWD'])
    expected_results['MINS_PORT_GRP_PV'] = pd.to_numeric(expected_results['MINS_PORT_GRP_PV'], errors='coerce')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_traffic_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "TRAFFIC_WEIGHT"

    # Run Shift Weight step
    main_run.traffic_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\traffic_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR +r'\main\dec\traffic_weight\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'CASES', 'TRAFFICTOTAL', 'SUM_TRAFFIC_WT', 'TRAFFIC_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'CASES', 'TRAFFICTOTAL', 'SUM_TRAFFIC_WT', 'TRAFFIC_WT'])
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_unsampled_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "UNSAMPLED_WEIGHT"

    # Run Shift Weight step
    main_run.unsampled_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\unsampled_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\unsampled_weight\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART', 'CASES', 'SUM_PRIOR_WT', 'SUM_UNSAMP_TRAFFIC_WT',
         'UNSAMP_TRAFFIC_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'ARRIVEDEPART', 'CASES', 'SUM_PRIOR_WT', 'SUM_UNSAMP_TRAFFIC_WT',
         'UNSAMP_TRAFFIC_WT'])
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_imbalance_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "IMBALANCE_WEIGHT"

    # Run Shift Weight step
    main_run.imbalance_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\imbalance_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\imbalance_weight\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(['FLOW', 'SUM_PRIOR_WT', 'SUM_IMBAL_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(['FLOW', 'SUM_PRIOR_WT', 'SUM_IMBAL_WT'])
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_final_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "FINAL_WEIGHT"

    # Run Shift Weight step
    main_run.final_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql = """
                SELECT [SERIAL], [FINAL_WT]
                FROM {}
                WHERE RUN_ID = '{}'
                AND [SERIAL] not like '9999%'
                AND [RESPNSE] between '1' and '6'
            """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)
    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\final_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\final_weight\dec\summary_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['SERIAL', 'SHIFT_WT', 'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['SERIAL', 'SHIFT_WT', 'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT'])
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

@pytest.mark.skip(reason="Because!")
def test_stay_imputation_step():
    # Assign variables
    conn = database_connection()
    step_name = "STAY_IMPUTATION"

    # Run Shift Weight step
    main_run.stay_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    sql_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\stay_imputation\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = sql_results.dropna(subset=['STAY_IMP_FLAG_PV'])
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_fares_imputation_step():
    # Assign variables
    conn = database_connection()

    # Run Shift Weight step
    main_run.fares_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql = """
    SELECT [SERIAL], [FARE], [FAREK], [SPEND], [SPENDIMPREASON]
      FROM {}
      WHERE RUN_ID = '{}'
      AND SERIAL not like '9999%'
      AND RESPNSE between 1 and 6
    """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    # Using comparison data populated by Python from unit test due
    # to random values populated in OPERA_PV. NOT USING SAS BASELINE DATA
    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\fares_imputation\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_spend_imputation_step():
    # Assign variables
    conn = database_connection()

    # Run Spend Imputation step
    main_run.spend_imputation_step(RUN_ID, conn)

    sql = """
            SELECT [SERIAL], [SPENDK], [SPEND] as 'newspend'
            FROM {}
            WHERE RUN_ID = '{}'
            AND [SERIAL] not like '9999%'
            AND [RESPNSE] between '1' and '6'
            AND [SPEND_IMP_ELIGIBLE_PV] = '1'
        """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\spend_imputation\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_rail_imputation_step():
    # Assign variables
    conn = database_connection()

    # Run Spend Imputation step
    main_run.rail_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql = """
            SELECT [SERIAL], [SPEND]
            FROM {}
            WHERE RUN_ID = '{}'
            AND [SERIAL] not like '9999%'
            AND [RESPNSE] between 1 and 6
        """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\rail_imputation\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    # actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_regional_weights_step():
    # Assign variables
    conn = database_connection()

    # Run Spend Imputation step
    main_run.regional_weights_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = """[SERIAL], [NIGHTS1], [NIGHTS2], [NIGHTS3], [NIGHTS4], [NIGHTS5],	[NIGHTS6],	[NIGHTS7],	[NIGHTS8],
                    [EXPENDITURE_WT],	[EXPENDITURE_WTK],	[STAY1K],	[STAY2K],	[STAY3K],	[STAY4K],	[STAY5K],
                    [STAY6K],	[STAY7K],	[STAY8K],	[STAY_WT],	[STAY_WTK],	[VISIT_WT],	[VISIT_WTK]"""

    sql = """
            SELECT {}
            FROM {}
            WHERE RUN_ID = '{}'
            AND [SERIAL] not like '9999%'
            AND [RESPNSE] between '1' and '6'
            AND [REG_IMP_ELIGIBLE_PV] = '1'
        """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\regional_weight\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    # actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_town_stay_expenditure_imputation_step():
    # Assign variables
    conn = database_connection()

    # Run Spend Imputation step
    main_run.town_stay_expenditure_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = "[SERIAL], [SPEND1], [SPEND2], [SPEND3], [SPEND4], [SPEND5], [SPEND6], [SPEND7], [SPEND8]"
    sql = """
                SELECT {}
                FROM {}
                WHERE RUN_ID = '{}'
                AND [SERIAL] not like '9999%'
                AND [RESPNSE] between 1 and 6
                AND [TOWN_IMP_ELIGIBLE_PV] = 1
            """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\town_and_stay\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


@pytest.mark.skip(reason="Because!")
def test_airmiles_step():
    # Assign variables
    conn = database_connection()

    # Run Spend Imputation step
    main_run.airmiles_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = "[SERIAL], [UKLEG], [OVLEG], [DIRECTLEG]"
    sql = """
                   SELECT {}
                   FROM {}
                   WHERE RUN_ID = '{}'
                   AND [SERIAL] not like '9999%'
                   AND [RESPNSE] between '1' and '6'
                   AND [FLOW] in ('1', '2', '3', '4')
               """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(TEST_DATA_DIR + r'\main\dec\airmiles\surveydata_out_expected.csv', engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)