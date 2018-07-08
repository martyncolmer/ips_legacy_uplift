import main.io.CommonFunctions as cf
import main.io.generic_xml_steps as gxs
import pytest
import pandas as pd
import sys
from main.io import import_data
from main.io import import_traffic_data
from main.io.CommonFunctions import get_oracle_connection
from pandas.util.testing import assert_frame_equal
from main.main import shift_weight_step

TEST_DATA_DIR = 'tests/data/generic_xml_steps/'


@pytest.fixture(scope='module')
def database_connection():
    '''
    This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again.
    '''
    return get_oracle_connection()


def get_rec_id(value, table, database_connection):
    # value = 'min' or 'max'
    # table = table name
    # Retrieve rec_id

    cur = database_connection.cursor()
    sql = """
        SELECT {}([REC_ID])
          FROM {}
          """.format(value, table)

    result = cur.execute(sql).fetchone()
    return result[0]


def store_rec_id(table_name):
    '''
    This function retrieves the max REC_ID from the database and stores value within a text file.
    '''

    file = r'tests/data/generic_xml_steps/record_id.txt'

    conn = get_oracle_connection()
    cur = conn.cursor()
    sql = """
        SELECT MAX(REC_ID)
        FROM [ips_test].{}    
        """.format(table_name)
    try:
        result = cur.execute(sql).fetchone()
    except Exception as err:
        print(err)
    else:
        rec_id = result[0]

    try:
        with open(file, 'w') as f:
            f.write(str(rec_id))
        # f.flush()
    except IOError as err:
        print(err)


def amend_rec_id(dataframe, rec_id):
    '''
    This function retrieves REC_ID from text file and inputs to test result dataframe.
    '''
    # with open(r'tests/data/generic_xml_steps/record_id.txt', 'r') as file:
    #     rec_id = int(file.read())

    for row in range(0, len(dataframe['REC_ID'])):
        dataframe['REC_ID'][row] = rec_id
        rec_id = rec_id + 1

    return dataframe


def import_data_into_database():
    '''
    This function prepares all the data necessary to run all 14 steps.
    The input files have been edited to make sure data types match the database tables.
    Note that no process variables are uploaded and are expected to be present in the database.
    :return:
    '''
    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    version_id = 1891

    # Import data paths (these will be passed in through the user)
    survey_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\ips1712bv4_amtspnd.sas7bdat"
    shift_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts Q1 2017.csv'
    nr_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response Q1 2017.csv'
    sea_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv'
    tunnel_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017 - Copy.csv'
    air_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017 - Copy.csv'
    unsampled_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv'

    # Import survey data function to go here
    import_data.import_survey_data(survey_data_path, run_id, version_id)

    # Import Shift Data
    import_traffic_data.import_traffic_data(run_id, shift_data_path)
    import_traffic_data.import_traffic_data(run_id, nr_data_path)
    import_traffic_data.import_traffic_data(run_id, sea_data_path)
    import_traffic_data.import_traffic_data(run_id, tunnel_data_path)
    import_traffic_data.import_traffic_data(run_id, air_data_path)
    import_traffic_data.import_traffic_data(run_id, unsampled_data_path)


def test_nullify_survey_subsample_pv_values(database_connection):
    test_data = pd.read_pickle('tests/data/generic_xml_steps/nullify_pv_survey_data.pkl')
    # test to make sure our test data is different from the data after applying the function
    assert test_data['SHIFT_PORT_GRP_PV'].isnull().sum() == 0
    assert test_data['WEEKDAY_END_PV'].isnull().sum() == 0

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)
    gxs.nullify_survey_subsample_pv_values("nullify-test", database_connection, ["[SHIFT_PORT_GRP_PV]",
                                                                                 "[WEEKDAY_END_PV]"])

    # COMMENTED AS CAUSING TEST TO FAIL.  RECORDS BEING INSERTED AND THEN DELETED BEFORE TESTING - ET
    # cleanse tables before testing output
    # cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'nullify-test')

    result = cf.select_data('SHIFT_PORT_GRP_PV', gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', 'nullify-test')
    assert result['SHIFT_PORT_GRP_PV'].isnull().sum() == len(result)
    result = cf.select_data('WEEKDAY_END_PV', gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "nullify-test")
    assert result['WEEKDAY_END_PV'].isnull().sum() == len(result)

    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'nullify-test')


def test_move_survey_subsample_to_sas_table(database_connection):
    test_data = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample.pkl')

    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)
    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    gxs.move_survey_subsample_to_sas_table('move-survey-test', database_connection, step_name="")

    result = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)
    # cleanse tables before testing output
    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')

    # one record has a value beyond the RESPNSE range
    assert len(result) == (len(test_data)-1)
    assert result.columns.isin(gxs.COLUMNS_TO_MOVE).sum() == len(gxs.COLUMNS_TO_MOVE)

    test_result = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample_result.pkl')
    assert_frame_equal(result, test_result)


def test_move_survey_subsample_to_sas_table_traffic_weight(database_connection):
    test_data = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample.pkl')

    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)
    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    gxs.move_survey_subsample_to_sas_table('move-survey-test', database_connection, step_name="TRAFFIC_WEIGHT")

    result = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)
    # cleanse tables before testing output
    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')

    # two records have a value beyond the RESPNSE range
    assert len(result) == (len(test_data)-2)
    assert result.columns.isin(gxs.COLUMNS_TO_MOVE).sum() == len(gxs.COLUMNS_TO_MOVE)

    test_result = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample_traffic_result.pkl')
    assert_frame_equal(result, test_result)


def test_populate_survey_data_for_step(database_connection):
    # this is an integration of the above tests so we will keep things simple

    test_data = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample.pkl')
    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)

    step_config = {'nullify_pvs': ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]"],
                   'name': 'SHIFT_WEIGHT',
                   'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"]}
    gxs.populate_survey_data_for_step('move-survey-test', database_connection, step_config)

    # cleanse tables before testing output
    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')

    result = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)
    test_result = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample_result.pkl')
    assert_frame_equal(result, test_result)

    # cleanse tables before testing output
    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    result = cf.get_table_values("SAS_SHIFT_WT")
    assert len(result) == 0

    result = cf.get_table_values("SAS_PS_SHIFT_DATA")
    assert len(result) == 0


def test_populate_step_data(database_connection):
    step_config = {"table_name": "[dbo].[SHIFT_DATA]",
                   "data_table": "[dbo].[SAS_SHIFT_DATA]",
                   "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
                                          "[AM_PM_NIGHT]"],
                   }

    # setup test data/tables
    test_data = pd.read_pickle('tests/data/generic_xml_steps/populate_step_data.pkl')

    # Reorder columns to match db and insert
    test_data.columns = test_data.columns.str.upper()
    test_data = test_data[
        ['RUN_ID', 'YEAR', 'MONTH', 'DATA_SOURCE_ID', 'PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'TOTAL', 'AM_PM_NIGHT']]
    cf.insert_dataframe_into_table(step_config["table_name"], test_data)

    # Assign run_id, and input test data to function
    run_id = 'populate-step-data'
    gxs.populate_step_data(run_id, database_connection, step_config)
    result = cf.get_table_values(step_config['data_table'])

    # Retrieve and amend rec_id within test result data
    test_result = pd.read_pickle('tests/data/generic_xml_steps/populate_step_data_result.pkl')
    test_result = amend_rec_id(test_result)

    # Before deleting anything get and record the latest REC_ID number
    store_rec_id(step_config['data_table'])

    # tear-down and cleanse
    cf.delete_from_table(step_config['data_table'])
    cf.delete_from_table(step_config['table_name'], 'RUN_ID', '=', run_id)
    cf.delete_from_table(step_config['data_table'])

    # result.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\El's Temp VDI Folder\XML\Generic\result.csv")
    # test_result.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\El's Temp VDI Folder\XML\Generic\test_result.csv")

    print(result)
    print(test_result)

    assert_frame_equal(result, test_result)


@pytest.mark.parametrize('step_name, expected_results_file, pv_columns, spv_table', [
    ("SHIFT_WEIGHT", 'copy_pvs_shift_weight_result.pkl', ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"], "[dbo].[SAS_SHIFT_SPV]"),
    ("UNSAMPLED_WEIGHT", 'copy_pvs_unsampled_weight_result.pkl', ["'UNSAMP_PORT_GRP_PV'", "'UNSAMP_REGION_GRP_PV'"], "[dbo].[SAS_UNSAMPLED_OOH_SPV]"),
])
def test_copy_step_pvs_for_survey_data(step_name, expected_results_file, pv_columns,
                                       spv_table, database_connection):
    # This test is parameterised. The values for the arguments of this test function
    # are taken from the parameters specified in pytest.mark.parametrize
    # see https://docs.pytest.org/en/latest/parametrize.html
    step_config = {'name': step_name,
                   'spv_table': spv_table,
                   'pv_columns': pv_columns}
    run_id = 'copy-step-pvs'

    # set up test data/tables
    test_process_variables = pd.read_pickle(TEST_DATA_DIR + 'process_variables.pkl')
    cf.insert_dataframe_into_table('PROCESS_VARIABLE_PY', test_process_variables, database_connection)

    gxs.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)

    results = cf.get_table_values(gxs.SAS_PROCESS_VARIABLES_TABLE)

    # clean test data before actually testing results
    cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
    cf.delete_from_table(gxs.SAS_PROCESS_VARIABLES_TABLE)

    test_results = pd.read_pickle(TEST_DATA_DIR + expected_results_file)
    results = results.sort_values(by='PROCVAR_NAME')
    test_results = test_results.sort_values(by='PROCVAR_NAME')
    results.index = range(0, len(results))
    test_results.index = range(0, len(test_results))
    assert_frame_equal(results, test_results)

    results = cf.get_table_values(step_config['spv_table'])
    assert len(results) == 0


def test_copy_step_pvs_for_survey_data_stay_imp(database_connection):
    step_config = {'name': "STAY_IMPUTATION",
                   'spv_table': '[dbo].[SAS_STAY_SPV]',
                   "copy_pvs": ["[STAY_IMP_FLAG_PV]", "[STAY_IMP_ELIGIBLE_PV]", "[STAY_PURPOSE_GRP_PV]"],
                   "copy_pvs2": ["[STAYIMPCTRYLEVEL1_PV]", "[STAYIMPCTRYLEVEL2_PV]", "[STAYIMPCTRYLEVEL3_PV]",
                                 "[STAYIMPCTRYLEVEL4_PV]"]}
    run_id = 'copy-step-pvs'

    # set up test data/tables
    test_process_variables = pd.read_pickle(TEST_DATA_DIR + 'process_variables.pkl')
    cf.insert_dataframe_into_table('PROCESS_VARIABLE_PY', test_process_variables, database_connection)

    gxs.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)

    results = cf.get_table_values(gxs.SAS_PROCESS_VARIABLES_TABLE)

    # clean test data before actually testing results
    cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
    cf.delete_from_table(gxs.SAS_PROCESS_VARIABLES_TABLE)

    test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_pvs_stay_imp_result.pkl')
    # we need to massage the data frames a little to ensure outputs are the same
    results = results.sort_values(by='PROCVAR_NAME')
    test_results = test_results.sort_values(by='PROCVAR_NAME')
    results.index = range(0, len(results))
    test_results.index = range(0, len(test_results))
    assert_frame_equal(results, test_results)

    results = cf.get_table_values(step_config['spv_table'])
    assert len(results) == 0


def test_update_survey_data_with_step_pv_output(database_connection):
    step_config = {'name': "NON_RESPONSE",
                   'spv_table': '[dbo].[SAS_NON_RESPONSE_SPV]',
                   'pv_columns': ["'NR_PORT_GRP_PV'", "'MIG_FLAG_PV'", "'NR_FLAG_PV'"]
                   }
    run_id = 'update-survey-pvs'

    # set up test data/tables
    test_survey_data = pd.read_pickle(TEST_DATA_DIR + 'update_survey_data_pvs.pkl')
    cf.insert_dataframe_into_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE, test_survey_data, database_connection)

    test_nr_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_nr_pv_data.pkl')
    cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)

    gxs.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)

    results = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    # clean test data before actually testing results
    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    test_results = pd.read_pickle(TEST_DATA_DIR + 'update_survey_data_pvs_result.pkl')
    assert_frame_equal(results, test_results)

    results = cf.get_table_values(step_config['spv_table'])
    assert len(results) == 0

    results = cf.get_table_values(gxs.SAS_PROCESS_VARIABLES_TABLE)
    assert len(results) == 0


def test_copy_step_pvs_for_step_data(database_connection):
    step_config = {'name': '[dbo].[SHIFT_DATA]'
                   , 'pv_table': '[dbo].[SAS_SHIFT_PV]'
                   , 'pv_columns': ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"]
                   , 'order': 0
                   }
    run_id = 'copy-step-pvs-for-step-data'

    # Pickle some test data
    test_data = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data.pkl')
    cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)

    # Plug it in to copy_step_pvs_for_step_data(run_id, conn, step_configuration)
    gxs.copy_step_pvs_for_step_data(run_id, database_connection, step_config)
    results = cf.get_table_values('SAS_PROCESS_VARIABLE')

    # Assert step_configuration["pv_table"] has 0 records
    sql = """
    SELECT COUNT(*)
    FROM {}
    """.format(step_config['pv_table'])

    cur = database_connection.cursor()
    result = cur.execute(sql).fetchone()[0]

    assert result == 0

    # Cleanse tables before continuing
    cf.delete_from_table(gxs.SAS_PROCESS_VARIABLES_TABLE)
    cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

    # Pickle some test results
    test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.pkl')

    # Assert equal
    assert_frame_equal(results, test_results, check_dtype=False)


def test_update_step_data_with_step_pv_output(database_connection):
    # step_config and variables
    step_config = {"pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
                   "pv_table": "[dbo].[SAS_SHIFT_PV]",
                   "data_table": "[dbo].[SAS_SHIFT_DATA]",
                   "weight_table": "[dbo].[SAS_SHIFT_WT]",
                   "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]"}

    # set up test data/tables
    test_shift_data = pd.read_pickle(TEST_DATA_DIR + 'update_shift_data_pvs.pkl')
    cf.delete_from_table(step_config["data_table"])  # This is a hack and should probably be deleted!
    cf.insert_dataframe_into_table(step_config["data_table"], test_shift_data, database_connection)

    test_shift_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_shift_pv_data.pkl')

    # Get rec_id and amend test dataframe
    rec_id = get_rec_id("MIN", step_config["data_table"], database_connection)
    test_shift_pv_data = amend_rec_id(test_shift_pv_data, rec_id)

    cf.insert_dataframe_into_table(step_config['pv_table'], test_shift_pv_data, database_connection)

    gxs.update_step_data_with_step_pv_output(database_connection, step_config)
    results = cf.get_table_values(step_config["data_table"])

    # clean test data before actually testing results
    cf.delete_from_table(step_config['data_table'])
    cf.delete_from_table(step_config['pv_table'])

    # Create expected test results
    test_results = pd.read_pickle(TEST_DATA_DIR + 'update_shift_data_pvs_result.pkl')
    test_results = amend_rec_id(test_results, rec_id)

    assert_frame_equal(results, test_results, check_dtype=False)

    # Assert temp tables had been cleanse in function
    results = cf.get_table_values(step_config['pv_table'])
    assert len(results) == 0

    results = cf.get_table_values(step_config['weight_table'])
    assert len(results) == 0

    results = cf.get_table_values(gxs.SAS_PROCESS_VARIABLES_TABLE)
    assert len(results) == 0

    results = cf.get_table_values(step_config['sas_ps_table'])
    assert len(results) == 0


@pytest.mark.skip('problems inserting data to sas_survey_subsample')
def test_update_survey_data_with_step_results(database_connection):
    # step_config and variables
    step_config = {"name": "SHIFT_WEIGHT",
                   "weight_table": "[dbo].[SAS_SHIFT_WT]",
                   "results_columns": ["[SHIFT_WT]"]}

    # set up test data/tables - export fake data into SAS_SURVEY_SUBSAMPLE (213
    # columns) and SAS_SHIFT_WT (2 columns) with matching SERIAL numbers

    # clean test data before actually testing results

    # Create expected test results and test against result

    # Assert temp tables had been cleansed in function


@pytest.mark.skip('problems inserting data to sas_survey_subsample')
def test_store_survey_data_with_step_results(database_connection):
    # step_config and variables

    # set up test data/tables

    # clean test data before actually testing results

    # Create expected test results and test against result

    # Assert temp tables had been cleansed in function
    assert False


def test_store_step_summary(database_connection):
    # step_config and variables
    step_config = {"ps_table": "[dbo].[PS_SHIFT_DATA]",
                   "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
                   "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
                                  "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
                                  "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"]}
    run_id = 'store-shift-data-summary'

    # set up test data/tables
    test_ps_data = pd.read_pickle(TEST_DATA_DIR + 'store_ps_summary.pkl')
    cf.insert_dataframe_into_table(step_config["sas_ps_table"], test_ps_data, database_connection)

    # Run function return results
    gxs.store_step_summary(run_id, database_connection, step_config)
    results = cf.get_table_values(step_config["ps_table"])

    # Create expected test results and test against result
    test_results = pd.read_pickle(TEST_DATA_DIR + 'store_shift_data_summary_test_result.pkl')
    assert_frame_equal(results, test_results, check_dtype=False)

    # Assert temp tables had been cleansed in function
    results = cf.get_table_values(step_config['sas_ps_table'])
    assert len(results) == 0


@pytest.mark.skip('this takes very long')
def test_shift_weight_step(database_connection):

    # import the necessary data into the database
    # note that this has not been tested to work repeatedly
    import_data_into_database()

    step_config = {"nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]", "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
                   'name': 'SHIFT_WEIGHT',
                   'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"],
                   "table_name": "[dbo].[SHIFT_DATA]",
                   "data_table": "[dbo].[SAS_SHIFT_DATA]",
                   "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
                                          "[AM_PM_NIGHT]"],
                   "spv_table": "[dbo].[SAS_SHIFT_SPV]",
                   "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"],
                   "pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
                   "order": 0,
                   "pv_table": "[dbo].[SAS_SHIFT_PV]",
                   "weight_table": "[dbo].[SAS_SHIFT_WT]",
                   "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
                   "results_columns": ["[SHIFT_WT]"],
                   "ps_table": "[dbo].[PS_SHIFT_DATA]",
                   "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
                                  "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
                                  "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"],

                   }

    shift_weight_step('9e5c1872-3f8e-4ae5-85dc-c67a602d011e', database_connection, step_config)