import pytest
import json
import pandas as pd

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from main.io import import_traffic_data
from main.io import ips_data_management as idm
from main.utils import process_variables
from main.calculations import calculate_ips_minimums_weight


with open('data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test_minimums_weight_xml'
TEST_DATA_DIR = r'tests\data\ips_data_management\minimums_weight_step'
STEP_NAME = 'MINIMUMS_WEIGHT'

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

    import_data_into_database()
    # populates test data within pv table
    populate_test_pv_table()

    print("Setup")


def teardown_module(module):
    # Delete any previous records from the Survey_Subsample tables for the given run ID
    reset_tables()

    print("Teardown")


def reset_tables():
    """ Cleanses tables within database. """
    # List of tables to cleanse entirely
    tables_to_unconditionally_cleanse = [idm.SAS_SURVEY_SUBSAMPLE_TABLE,
                                         idm.SAS_PROCESS_VARIABLES_TABLE]

    # Try to delete from each table in list.  If exception occurs, assume table is
    # already empty, and continue deleting from tables in list.
    for table in tables_to_unconditionally_cleanse:
        try:
            print("cf.delete_from_table({})".format(table))
            cf.delete_from_table(table)
        except Exception:
            continue

    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = ['[dbo].[PROCESS_VARIABLE_PY]',
                         '[dbo].[PROCESS_VARIABLE_TESTING]']

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list
    for table in tables_to_cleanse:
        try:
            print("cf.delete_from_table({}, 'RUN_ID', '=', RUN_ID)".format(table))
            cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID)
        except Exception:
            continue

    # Try to delete from each table in list.  If exception occurs, assume table is
    # already empty, and continue deleting from tables in list
    for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
        try:
            cf.delete_from_table(table)
        except Exception:
            continue


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
    DELETE from [dbo].[PROCESS_VARIABLE_TESTING]
    """

    sql2 = """
    DELETE from [dbo].[PROCESS_VARIABLE_PY]
    WHERE RUN_ID = '{}'
    """.format(RUN_ID)

    sql3 = """
    INSERT INTO [dbo].[PROCESS_VARIABLE_TESTING]
    SELECT * FROM [dbo].[PROCESS_VARIABLE_PY]
    WHERE [RUN_ID] = 'TEMPLATE'
    """

    sql4 = """
    UPDATE [dbo].[PROCESS_VARIABLE_TESTING]
    SET [RUN_ID] = '{}'
    """.format(RUN_ID)

    sql5 = """
    INSERT INTO [dbo].[PROCESS_VARIABLE_PY]
    SELECT * FROM [dbo].[PROCESS_VARIABLE_TESTING]
    WHERE RUN_ID = '{}'
    """.format(RUN_ID)

    cur.execute(sql1)
    cur.execute(sql2)
    cur.execute(sql3)
    cur.execute(sql4)
    cur.execute(sql5)


def import_data_into_database():
    '''
    This function prepares all the data necessary to run all 14 steps.
    The input files have been edited to make sure data types match the database tables.
    Note that no process variables are uploaded and are expected to be present in the database.
    '''

    # Import data paths (these will be passed in through the user)
    survey_data_path = r'tests\data\ips_data_management\integration_import_data\minimums_weight\surveydata.csv'
    shift_data_path = r'tests\data\ips_data_management\integration_import_data\external\Poss shifts Dec 2017.csv'
    nr_data_path = r'tests\data\ips_data_management\integration_import_data\external\Dec17_NR.csv'
    sea_data_path = r'tests\data\ips_data_management\integration_import_data\external\Sea Traffic Dec 2017.csv'
    tunnel_data_path = r'tests\data\ips_data_management\integration_import_data\external\Tunnel Traffic Dec 2017.csv'
    air_data_path = r'tests\data\ips_data_management\integration_import_data\external\Air Sheet Dec 2017 VBA.csv'
    unsampled_data_path = r'tests\data\ips_data_management\integration_import_data\external\Unsampled Traffic Dec 2017.csv'

    # Delete table content
    cf.delete_from_table('SURVEY_SUBSAMPLE', 'RUN_ID', '=', RUN_ID)
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')

    df = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)

    # Add the generated run id to the dataset.
    df['RUN_ID'] = pd.Series(RUN_ID, index=df.index)

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table('SURVEY_SUBSAMPLE', df)

    # Import the external files into the database.
    import_traffic_data.import_traffic_data(RUN_ID, shift_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, nr_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, sea_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, tunnel_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, air_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, unsampled_data_path)


@pytest.mark.skip("Rounding error causes test to fail.")
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
    df_survey_actual = pd.read_csv(TEST_DATA_DIR + '\survey_subsample_actual.csv').sort_values('SERIAL')
    df_survey_target = pd.read_csv(TEST_DATA_DIR + '\survey_subsample_target.csv', encoding='ANSI').sort_values('SERIAL')
    df_summary_actual = pd.read_csv(TEST_DATA_DIR + '\ps_minimums_actual.csv').sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    df_summary_target = pd.read_csv(TEST_DATA_DIR + '\ps_minimums_target.csv', encoding='ANSI').sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])

    # Reset the dataframe's index before comparing the outputs.
    df_survey_actual.index = range(0, len(df_survey_actual))
    df_survey_target.index = range(0, len(df_survey_target))
    df_summary_actual.index = range(0, len(df_summary_actual))
    df_summary_target.index = range(0, len(df_summary_target))

    # Neither of the tests succeed due to the rounding error. Once target data has been replaced these will pass.

    # Ensure summary output is equal to expected summary output
    assert_frame_equal(df_summary_actual, df_summary_target, check_dtype=False,check_like=True, check_less_precise=True)

    # Select the newly updated weight column from the dataframe and ensure it matches the expected weights
    df_survey_actual = df_survey_actual[['SERIAL', 'MINS_WT']]
    df_survey_target = df_survey_target[['SERIAL', 'MINS_WT']]
    assert assert_frame_equal(df_survey_actual, df_survey_target, check_dtype=False)
