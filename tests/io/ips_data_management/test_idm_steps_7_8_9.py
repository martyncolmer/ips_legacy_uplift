import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
import main.io.CommonFunctions as cf
import main.io.ips_data_management as idm

TEST_DATA_DIR = 'tests/data/ips_data_management'

@pytest.fixture(scope='module')
def database_connection():
    '''
    This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again.
    '''
    return cf.get_sql_connection()


@pytest.mark.parametrize('step_name, temp_table, results_columns, prefix',
                         [("SHIFT_WEIGHT", "[dbo].[SAS_SHIFT_WT]", ["[SHIFT_WT]"], '/shift_wt_'),
                          ("UNSAMPLED_WEIGHT", '[dbo].[SAS_UNSAMPLED_OOH_WT]', ["[UNSAMP_TRAFFIC_WT]"], '/unsampled_wt_'),
                          ("FARES_IMPUTATION", '[dbo].[SAS_FARES_IMP]', ["[FARE]", "[FAREK]", "[SPEND]", "[SPENDIMPREASON]"], '/fares_imp_'),
                          ("IMBALANCE_WEIGHT", '[dbo].[SAS_IMBALANCE_WT]', ["[IMBAL_WT]"], '/imb_wt_'),
                          ("STAY_IMPUTATION", '[dbo].[SAS_STAY_IMP]', ["[STAY]", "[STAYK]"], '/stay_imp_'),
                          ("SPEND_IMPUTATION", '[dbo].[SAS_SPEND_IMP]', ["[SPENDK]"], '/spend_imp_')])
def test_update_survey_data_with_step_results(step_name, temp_table, results_columns, prefix, database_connection):
    '''
    # This test is parameterised. The values for the arguments of this test function
    # are taken from the parameters specified in pytest.mark.parametrize
    # see https://docs.pytest.org/en/latest/parametrize.html
    '''

    # step_config and variables
    step_config = {"name": step_name,
                   "temp_table": temp_table,
                   "results_columns": results_columns}

    folder = '/update_survey_data_with_step_results'

    # Cleanse and set up test data/tables
    cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    sas_survey_subsample_input = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'sas_survey_subsample_test_input.csv', dtype=object)
    cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection, fast=False)

    cf.delete_from_table(step_config["temp_table"])
    sas_shift_wt_input = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'temp_table_test_input.csv', dtype=object)
    cf.insert_dataframe_into_table(step_config["temp_table"], sas_shift_wt_input, database_connection, fast=False)

    # Run function
    idm.update_survey_data_with_step_results(database_connection, step_config)

    # Get and format results
    results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    results.to_csv(TEST_DATA_DIR + folder + prefix + 'actual_results.csv', index=False)
    results = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'actual_results.csv', dtype=object)
    test_results = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'expected_results.csv', dtype=object)

    results.sort_values(by=["SERIAL"], inplace=True)
    results.index = range(0, len(results))

    test_results.sort_values(by=["SERIAL"], inplace=True)
    test_results.index = range(0, len(test_results))

    assert_frame_equal(results, test_results, check_dtype=False)

    # Assert temp tables had been cleansed in function
    result = cf.get_table_values(step_config['temp_table'])
    assert len(result) == 0


@pytest.mark.parametrize('step_name, nullify_pvs, ps_table, prefix',
                         [("SHIFT_WEIGHT", ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]", "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"], "[dbo].[PS_SHIFT_DATA]", '/shift_wt_'),
                          ("UNSAMPLED_WEIGHT", ["[UNSAMP_PORT_GRP_PV]", "[UNSAMP_REGION_GRP_PV]", "[UNSAMP_TRAFFIC_WT]"], "[dbo].[PS_UNSAMPLED_OOH]", '/uns_wt_'),
                          ("SPEND_IMPUTATION", ["[SPEND_IMP_FLAG_PV]", "[SPEND_IMP_ELIGIBLE_PV]", "[UK_OS_PV]", "[PUR1_PV]", "[PUR2_PV]", "[PUR3_PV]", "[DUR1_PV]", "[DUR2_PV]", "[SPENDK]"], "[dbo].[PS_SPEND_IMPUTATION]", '/spend_imp_')])
def test_store_survey_data_with_step_results(step_name, nullify_pvs, ps_table, prefix, database_connection):
    '''
    # This test is parameterised. The values for the arguments of this test function
    # are taken from the parameters specified in pytest.mark.parametrize
    # see https://docs.pytest.org/en/latest/parametrize.html
    '''

    # step_config and variables
    step_config = {"name": step_name,
                   "nullify_pvs": nullify_pvs,
                   "ps_table": ps_table}
    run_id = 'store_survey_data_test'
    folder = '/store_survey_data_with_step_results'
    applicable_ps_tables = ["SHIFT_WEIGHT"
        , "NON_RESPONSE"
        , "MINIMUMS_WEIGHT"
        , "TRAFFIC_WEIGHT"
        , "UNSAMPLED_WEIGHT"
        , "IMBALANCE_WEIGHT"
        , "FINAL_WEIGHT"]

    # Cleanse and delete test inputs
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)
    cf.delete_from_table(step_config['ps_table'], 'RUN_ID', '=', run_id)

    # Set up records in SURVEY_SUBSAMPLE with above run_id
    survey_subsample_input = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'survey_subsample_test_input.csv', dtype=object)
    cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, survey_subsample_input, database_connection, fast=False)

    # Set up records in SAS_SURVEY_SUBSAMPLE with above run_id
    sas_survey_subsample_input = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'sss_test_input.csv', dtype=object)
    cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection, fast=False)

    # Set up records in ps_table with above run_id
    if step_name in applicable_ps_tables:
        ps_shift_data_input = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'summary_table_test_input.csv', dtype=object)
        cf.insert_dataframe_into_table(step_config['ps_table'], ps_shift_data_input, database_connection, fast=False)

    # Run function
    idm.store_survey_data_with_step_results(run_id, database_connection, step_config)

    # Assert tables were cleansed by function
    if step_name in applicable_ps_tables:
        sql = """
            SELECT * FROM {}
            WHERE RUN_ID = '{}'""".format(step_config['ps_table'], run_id)
        cur = database_connection.cursor()
        result = cur.execute(sql).fetchone()
        assert result == None

    result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    assert len(result) == 0

    # Retrieve results produced by function
    sql = """
    SELECT * FROM {}
    WHERE RUN_ID = '{}'
    """.format(idm.SURVEY_SUBSAMPLE_TABLE, run_id)
    results = pd.read_sql(sql, database_connection)
    results.to_csv(TEST_DATA_DIR + folder + prefix + 'actual_results.csv', index=False)

    # Get and format results
    results = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'actual_results.csv', dtype=object)
    test_results = pd.read_csv(TEST_DATA_DIR + folder + prefix + 'expected_result.csv',
                               dtype=object)

    results.sort_values(by=["SERIAL"], inplace=True)
    results.index = range(0, len(results))

    test_results.sort_values(by=["SERIAL"], inplace=True)
    test_results.index = range(0, len(test_results))

    assert_frame_equal(results, test_results, check_dtype=False)


def test_store_step_summary(database_connection):
    # step_config and variables
    step_config = {"ps_table": "[dbo].[PS_SHIFT_DATA]",
                   "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
                   "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
                                  "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
                                  "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"]}
    run_id = 'shift-wt-idm-test'
    folder = '/store_step_summary'

    # Set up test data/tables
    test_ps_data = pd.read_csv(TEST_DATA_DIR + folder + '/shift_wt_sas_ps_shift_data_test_input.csv')
    cf.insert_dataframe_into_table(step_config["sas_ps_table"], test_ps_data, database_connection)

    # Run function return results
    idm.store_step_summary(run_id, database_connection, step_config)
    sql = """
    SELECT * FROM {}
    WHERE RUN_ID = '{}'
    """.format(step_config["ps_table"], run_id)
    results = pd.read_sql(sql, database_connection)
    results.to_csv(TEST_DATA_DIR + folder + '/shift_wt_actual_results.csv', index=False)

    # Get and format results
    results = pd.read_csv(TEST_DATA_DIR + folder + '/shift_wt_actual_results.csv', dtype=object)
    test_results = pd.read_csv(TEST_DATA_DIR + folder + '/shift_wt_expected_results.csv',
                               dtype=object)

    results.sort_values(by=['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV'], inplace=True)
    results.index = range(0, len(results))

    test_results.sort_values(by=['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV'], inplace=True)
    test_results.index = range(0, len(test_results))

    assert_frame_equal(results, test_results, check_dtype=False)

    # Assert temp tables had been cleansed in function
    results = cf.get_table_values(step_config['sas_ps_table'])
    assert len(results) == 0

    # Cleanse test inputs
    cf.delete_from_table(step_config['ps_table'], 'RUN_ID', '=', run_id)