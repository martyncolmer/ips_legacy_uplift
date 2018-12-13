import json
import os
import pytest
import time
import numpy as np
import pandas as pd

from pandas.util.testing import assert_frame_equal
from main import main as main_run
from main.io import CommonFunctions as cf
from tests import common_testing_functions as ctf
from main.io import ips_data_management as idm

with open(r'data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test-main'
PV_RUN_ID = 'TEMPLATE'
TEST_DATA_DIR = r'tests\data\main\dec'
SURVEY_DATA_FILENAME = 'surveydata_out_expected.csv'
SUMMARY_DATA_FILENAME = 'summary_out_expected.csv'

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

    # Import data to database
    import_data_dir = r'tests\data\import_data\dec'
    ctf.import_test_data_into_database(import_data_dir, RUN_ID)

    # populates test data within pv table
    conn = database_connection()
    ctf.populate_test_pv_table(conn, RUN_ID, PV_RUN_ID)


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
        method.
        """
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)

    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = ['[dbo].[PROCESS_VARIABLE_PY]',
                         '[dbo].[PROCESS_VARIABLE_TESTING]',
                         '[dbo].[TRAFFIC_DATA]',
                         '[dbo].[SHIFT_DATA]',
                         '[dbo].[NON_RESPONSE_DATA]',
                         '[dbo].[UNSAMPLED_OOH_DATA]',
                         idm.SURVEY_SUBSAMPLE_TABLE]

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


def test_shift_weight_step():
    # # Assign variables
    conn = database_connection()
    step_name = "SHIFT_WEIGHT"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.shift_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.replace('None', np.nan, inplace=True)
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(summary_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS',
         'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS',
         'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT'])
    expected_results['RUN_ID'] = RUN_ID
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_non_response_weight_steps():
    # # Assign variables
    conn = database_connection()
    step_name = "NON_RESPONSE"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Non Response Weight step
    main_run.non_response_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(survey_file, engine='python')

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
    expected_results = pd.read_csv(summary_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
         'GROSS_RESP', 'GNR', 'MEAN_NR_WT'])
    actual_results['NR_PORT_GRP_PV'] = pd.to_numeric(actual_results['NR_PORT_GRP_PV'], errors='coerce')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
         'GROSS_RESP', 'GNR', 'MEAN_NR_WT'])
    expected_results['RUN_ID'] = RUN_ID
    expected_results['NR_PORT_GRP_PV'] = pd.to_numeric(expected_results['NR_PORT_GRP_PV'], errors='coerce')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_minimums_weight_step():
    # # Assign variables
    conn = database_connection()
    step_name = "MINIMUMS_WEIGHT"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.minimums_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results['MINS_PORT_GRP_PV'] = pd.to_numeric(actual_results['MINS_PORT_GRP_PV'], errors='coerce')
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(summary_file, engine='python')

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
    expected_results['RUN_ID'] = RUN_ID
    expected_results['MINS_PORT_GRP_PV'] = pd.to_numeric(expected_results['MINS_PORT_GRP_PV'], errors='coerce')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_traffic_weight_step():
    # # Assign variables
    conn = database_connection()
    step_name = "TRAFFIC_WEIGHT"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.traffic_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(summary_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'CASES', 'TRAFFICTOTAL', 'SUM_TRAFFIC_WT', 'TRAFFIC_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'CASES', 'TRAFFICTOTAL', 'SUM_TRAFFIC_WT', 'TRAFFIC_WT'])
    expected_results['RUN_ID'] = RUN_ID
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

@pytest.mark.xfail
def test_unsampled_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "UNSAMPLED_WEIGHT"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.unsampled_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    # Get results of Survey Data and compare
    sql = """
        SELECT {}
        FROM {}
        WHERE RUN_ID = '{}'
        AND [SERIAL] not like '9999%'
        AND [RESPNSE] between 1 and 2
    """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.fillna(value=np.nan, inplace=True)
    actual_results['UNSAMP_REGION_GRP_PV'] = pd.to_numeric(actual_results['UNSAMP_REGION_GRP_PV'],
                                                           errors='coerce',
                                                           downcast='float')
    actual_results.index = range(0, len(actual_results))

    expected_results.columns = expected_results.columns.str.upper()
    expected_results = expected_results.sort_values('SERIAL')
    expected_results.fillna(value=np.nan, inplace=True)
    expected_results['UNSAMP_REGION_GRP_PV'] = pd.to_numeric(expected_results['UNSAMP_REGION_GRP_PV'],
                                                             errors='coerce',
                                                             downcast='float')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\unsampled_survey_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\unsampled_survey_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]["ps_table"], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(summary_file, engine='python')

    # Formatting and fudgery
    actual_results['UNSAMP_REGION_GRP_PV'] = pd.to_numeric(actual_results['UNSAMP_REGION_GRP_PV'],
                                                             errors='coerce',
                                                             downcast='float')
    actual_results = actual_results.sort_values(
        ['UNSAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'UNSAMP_REGION_GRP_PV', 'CASES', 'SUM_PRIOR_WT', 'SUM_UNSAMP_TRAFFIC_WT',
         'UNSAMP_TRAFFIC_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results['UNSAMP_REGION_GRP_PV'] = pd.to_numeric(expected_results['UNSAMP_REGION_GRP_PV'],
                                                             errors='coerce',
                                                             downcast='float')
    expected_results['RUN_ID'] = RUN_ID
    expected_results = expected_results.sort_values(
        ['UNSAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'UNSAMP_REGION_GRP_PV', 'cases', 'sum_prior_wt', 'sum_unsamp_traffic_wt',
         'unsamp_traffic_wt'])
    expected_results[['cases', 'sum_prior_wt', 'sum_unsamp_traffic_wt',
                    'unsamp_traffic_wt']] = expected_results[['cases', 'sum_prior_wt', 'sum_unsamp_traffic_wt',
                                                            'unsamp_traffic_wt']].round(3)
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\unsampled_summary_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\unsampled_summary_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False, check_like=True)

@pytest.mark.xfail
def test_imbalance_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "IMBALANCE_WEIGHT"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.imbalance_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    # Get results of Survey Data and compare
    sql = """
            SELECT {}
            FROM {}
            WHERE RUN_ID = '{}'
            AND [SERIAL] not like '9999%'
            AND [RESPNSE] between 1 and 6
        """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\imbalance_survey_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\imbalance_survey_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(summary_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(['FLOW', 'SUM_PRIOR_WT', 'SUM_IMBAL_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(['FLOW', 'SUM_PRIOR_WT', 'SUM_IMBAL_WT'])
    expected_results['RUN_ID'] = RUN_ID
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\imbalance_summary_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\imbalance_summary_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

@pytest.mark.xfail
def test_final_weight_step():
    # Assign variables
    conn = database_connection()
    step_name = "FINAL_WEIGHT"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)
    summary_file = os.path.join(dir_path, SUMMARY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.final_weight_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    # Get results of Survey Data and compare
    sql = """
            SELECT {}
            FROM {}
            WHERE RUN_ID = '{}'
            AND SERIAL not like '9999%'
            AND RESPNSE between 1 and 6
        """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\final_survey_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\final_survey_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

    # Get results of Summary Data and compare
    actual_results = cf.select_data('*', STEP_CONFIGURATION[step_name]['ps_table'], 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(summary_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values(
        ['SERIAL', 'SHIFT_WT', 'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT'])
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values(
        ['SERIAL', 'SHIFT_WT', 'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT'])
    expected_results['RUN_ID'] = RUN_ID
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\final_summary_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\final_summary_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_stay_imputation_step():
    # Assign variables
    conn = database_connection()
    step_name = "STAY_IMPUTATION"
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.stay_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    sql_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    sql_results = sql_results.dropna(subset=['STAY_IMP_FLAG_PV'])
    actual_results = sql_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_fares_imputation_step():
    # Assign variables
    conn = database_connection()
    step_name = 'FARES_IMPUTATION'
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)

    # Run Shift Weight step
    main_run.fares_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    # Get results of Survey Data and compare
    sql = """
        SELECT {}
        FROM {}
        WHERE RUN_ID = '{}'
        AND SERIAL not like '9999%'
        AND RESPNSE between 1 and 6
    """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    # Using comparison data populated by Python from unit test due
    # to random values populated in OPERA_PV. NOT USING SAS BASELINE DATA
    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\fares_survey_actual.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


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

    # Merge results from Fares in to SAS comparison data to create expected dataset
    fares_output = pd.read_csv(TEST_DATA_DIR + r'\fares_imputation\surveydata_out_expected.csv',
                               engine='python')
    sas_spend_output = pd.read_csv(TEST_DATA_DIR + r'\spend_imputation\surveydata_out_expected.csv',
                                   engine='python')

    fares_output = fares_output[['SERIAL', 'SPEND']].copy()
    fares_output.sort_values(by='SERIAL', inplace=True)
    fares_output.index = range(0, len(fares_output))

    sas_spend_output = sas_spend_output[['SERIAL', 'SPENDK', 'newspend']].copy()
    sas_spend_output.sort_values(by='SERIAL', inplace=True)
    sas_spend_output.index = range(0, len(sas_spend_output))

    expected_results = pd.merge(sas_spend_output, fares_output, on='SERIAL', how='left')
    expected_results.loc[(np.isnan(expected_results['newspend'])), 'newspend'] = expected_results['SPEND']
    expected_results.drop(columns='SPEND', inplace=True)

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\spend_survey_actual.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

@pytest.mark.xfail
def test_rail_imputation_step():
    # Assign variables
    conn = database_connection()
    step_name = 'RAIL_IMPUTATION'
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)

    # Run Spend Imputation step
    main_run.rail_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols + ", [SPEND]"

    # Get results of Survey Data and compare
    sql = """
        SELECT SERIAL, SPEND
        FROM {}
        WHERE RUN_ID = '{}'
        AND [SERIAL] not like '9999%'
        AND [RESPNSE] between 1 and 6
    """.format(idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\rail_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\rail_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

@pytest.mark.xfail
def test_regional_weights_step():
    # Assign variables
    conn = database_connection()
    step_name = 'REGIONAL_WEIGHTS'
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)

    # Run Spend Imputation step
    main_run.regional_weights_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    # sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    # sql_cols = "[SERIAL], " + sql_cols

    sql_cols = '[SERIAL], [NIGHTS1], [NIGHTS2], [NIGHTS3], [NIGHTS4], [NIGHTS5], [NIGHTS6], [NIGHTS7], [NIGHTS8], [EXPENDITURE_WT], [EXPENDITURE_WTK], [STAY1K], [STAY2K], [STAY3K], [STAY4K], [STAY5K], [STAY6K], [STAY7K], [STAY8K], [STAY_WT], [STAY_WTK], [VISIT_WT], [VISIT_WTK]'

    sql = """
        SELECT {}
        FROM {}
        WHERE RUN_ID = '{}'
        AND [SERIAL] not like '9999%'
        AND [RESPNSE] between 1 and 6
        AND [REG_IMP_ELIGIBLE_PV] = 1
    """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\regional_actual.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)

@pytest.mark.xfail
def test_town_stay_expenditure_imputation_step():
    # Assign variables
    conn = database_connection()
    step_name = 'TOWN_AND_STAY_EXPENDITURE'
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)

    # Run Spend Imputation step
    main_run.town_stay_expenditure_imputation_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    # sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    # sql_cols = "[SERIAL], " + sql_cols

    sql_cols = '[SERIAL], [SPEND1], [SPEND2], [SPEND3], [SPEND4], [SPEND5], [SPEND6], [SPEND7], [SPEND8]'

    # Get results of Survey Data and compare
    sql = """
        SELECT {}
        FROM {}
        WHERE RUN_ID = '{}'
        AND [SERIAL] not like '9999%'
        AND [RESPNSE] between 1 and 6
        AND [TOWN_IMP_ELIGIBLE_PV] = 1
    """.format(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, RUN_ID)

    actual_results = pd.read_sql_query(sql, conn)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    actual_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\town_stay_actual.csv')
    expected_results.to_csv(r'S:\CASPA\IPS\Testing\scratch\compare these\town_stay_expected.csv')

    assert_frame_equal(actual_results, expected_results, check_dtype=False)


def test_airmiles_step():
    # Assign variables
    conn = database_connection()
    step_name = 'AIR_MILES'
    dir_path = os.path.join(TEST_DATA_DIR, step_name.lower())
    survey_file = os.path.join(dir_path, SURVEY_DATA_FILENAME)

    # Run Spend Imputation step
    main_run.airmiles_step(RUN_ID, conn)

    # Get results of Survey Data and compare
    sql_cols = " , ".join(STEP_CONFIGURATION[step_name]['nullify_pvs'])
    sql_cols = "[SERIAL], " + sql_cols

    actual_results = cf.select_data(sql_cols, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    expected_results = pd.read_csv(survey_file, engine='python')

    # Formatting and fudgery
    actual_results = actual_results.sort_values('SERIAL')
    actual_results.replace('None', np.nan, inplace=True)
    actual_results.index = range(0, len(actual_results))

    expected_results = expected_results.sort_values('SERIAL')
    expected_results.index = range(0, len(expected_results))

    assert_frame_equal(actual_results, expected_results, check_dtype=False)
