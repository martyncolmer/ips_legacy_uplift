import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from main.io import import_traffic_data
from main.io import ips_data_management as idm
from main.utils import process_variables
from main.calculations.calculate_ips_traffic_weight import do_ips_trafweight_calculation_with_R

with open('data/xml_steps_configuration.json') as config_file: STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test_traffic_weight_xml'
STEP_NAME = 'TRAFFIC_WEIGHT'
step_config = STEP_CONFIGURATION[STEP_NAME]
PV_RUN_ID = 'TEMPLATE'

# data lengths for testing
SURVEY_SUBSAMPLE_LENGTH = 21638
EXPECTED_LEN = 17731
TRAFFIC_DATA_LENGTH = 238
TRAFFIC_SAS_PROCESS_VARIABLE_TABLE_LENGTH = 1

TRAFFIC_DATA_TABLE = 'TRAFFIC_DATA'
SAS_TRAFFIC_TABLE = 'SAS_TRAFFIC_DATA'
OUT_TABLE_NAME = "SAS_TRAFFIC_WT"  # output data
SUMMARY_OUT_TABLE_NAME = "SAS_PS_TRAFFIC"  # output data

# columns to sort the summary results by in order to check calculated dataframes match expected results
# NR_COLUMNS = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT',
#               'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']

ist = time.time()
print("Module level start time: {}".format(ist))


def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)


@pytest.fixture(scope='module')
def database_connection():
    '''
    This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again.
    '''
    return cf.get_sql_connection()


def setup_module():
    """ setup any state specific to the execution of the given module."""

    ist = time.time()
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
    for table in step_config['delete_tables']:
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

    survey_data_path = r'tests/data/ips_data_management/traffic_weight_integration/surveydata_traffic.csv'

    # grab the december traffic data
    shift_data_path = r'tests\data\ips_data_management\import_data\external\december\Poss shifts Dec 2017.csv'
    nr_data_path = r'tests\data\ips_data_management\import_data\external\december\Dec17_NR.csv'
    sea_data_path = r'tests\data\ips_data_management\import_data\external\december\Sea Traffic Dec 2017.csv'
    tunnel_data_path = r'tests\data\ips_data_management\import_data\external\december\Tunnel Traffic Dec 2017.csv'
    air_data_path = r'tests\data\ips_data_management\import_data\external\december\Air Sheet Dec 2017 VBA.csv'
    unsampled_data_path = r'tests\data\ips_data_management\import_data\external\december\Unsampled Traffic Dec 2017.csv'

    # Delete table content
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)
    cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # import the survey data for this step manually to avoid import errors
    df_survey_data = pd.read_csv(survey_data_path, engine='python')

    # Add the generated run id to the dataset.
    df_survey_data['RUN_ID'] = pd.Series(RUN_ID, index=df_survey_data.index)

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, df_survey_data)

    cf.delete_from_table(TRAFFIC_DATA_TABLE)

    # Import the external files into the database.
    import_traffic_data.import_traffic_data(RUN_ID, shift_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, nr_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, sea_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, tunnel_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, air_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, unsampled_data_path)

@pytest.mark.parametrize('path_to_data', [
    r'tests/data/ips_data_management/traffic_weight_integration'
    #r'tests\data\calculations\december_2017\traffic_weight', other months not available right now
    #r'tests\data\calculations\november_2017\non_response_weight', # ignored as data not available
    #r'tests\data\calculations\october_2017\non_response_weight', # ignored as data not available
    ])
def test_traffic_weight_step(path_to_data):

    # Get database connection
    conn = database_connection()

    # Run step 1
    idm.populate_survey_data_for_step(RUN_ID, conn, step_config)

    # ###########################
    # run checks 1
    # ###########################

    # Check all deleted tables are empty
    for table in step_config['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check all nullified columns are NULL
    for column in step_config['nullify_pvs']:
        column_name = column.replace('[', '').replace(']', '')
        result = cf.select_data(column_name, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
        assert result[column_name].isnull().sum() == len(result)

    # Check table has been populated
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    # Run step 2
    idm.populate_step_data(RUN_ID, conn, step_config)

    # ###########################
    # run checks 2
    # ###########################

    # Check table has been populated
    table_len = len(cf.get_table_values(step_config["data_table"]))
    assert table_len == TRAFFIC_DATA_LENGTH

    # Run step 3
    idm.copy_step_pvs_for_survey_data(RUN_ID, conn, step_config)

    # ###########################
    # run checks 3
    # ###########################

    # Get all values from the sas_process_variables table
    results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)

    # Check number of PV records moved matches number passed in through step configuration.
    assert len(results) == len(step_config['pv_columns'])

    # Get the spv_table values and ensure all records have been deleted
    results = cf.get_table_values(step_config['spv_table'])
    assert len(results) == 0

    # Run step 4  : Apply Traffic Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TRAFFIC_SPV',
                              in_id='serial')

    # ###########################
    # run checks 4
    # ###########################

    table_len = len(cf.get_table_values(step_config["spv_table"]))
    assert table_len == EXPECTED_LEN

    # Run step 5 : Update Survey Data with Traffic Wt PVs Output
    idm.update_survey_data_with_step_pv_output(conn, step_config)

    # ###########################
    # run checks 5
    # ###########################

    # Check all columns in SAS_SURVEY_SUBSAMPLE have been altered
    result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    for column in step_config['pv_columns']:
        column_name = column.replace("'", "")
        assert len(result[column_name]) == EXPECTED_LEN
        assert result[column_name].count() != 0

    # Assert SAS_PROCESS_VARIABLES_TABLE has been cleansed
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == 0

    # Assert spv_table has been cleansed
    table_len = len(cf.get_table_values(step_config["spv_table"]))
    assert table_len == 0

    # Run step 6 : Copy Non Response Wt PVs for Non Response Data
    idm.copy_step_pvs_for_step_data(RUN_ID, conn, step_config)

    # ###########################
    # run checks 6
    # ###########################

    # Assert pv_table has been cleansed
    table_len = len(cf.get_table_values(step_config["pv_table"]))
    assert table_len == 0

    # Assert SAS_PROCESS_VARIABLES_TABLE was populated
    table_len = len(cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE))
    assert table_len == TRAFFIC_SAS_PROCESS_VARIABLE_TABLE_LENGTH

    # Run step 7 : Apply Non Response Wt PVs On Non Response Data
    process_variables.process(dataset='traffic',
                                  in_table_name='SAS_TRAFFIC_DATA',
                                  out_table_name='SAS_TRAFFIC_PV',
                                  in_id='REC_ID')

    # ###########################
    # run checks 7
    # ###########################

    table_len = len(cf.get_table_values(step_config["pv_table"]))
    assert table_len == TRAFFIC_DATA_LENGTH

    # Run step 8 : Update NonResponse Data With PVs Output
    idm.update_step_data_with_step_pv_output(conn, step_config)

    # ###########################
    # run checks 8
    # ###########################

    # Assert data table was populated
    table_len = len(cf.get_table_values(step_config["data_table"]))
    assert table_len == TRAFFIC_DATA_LENGTH

    # Assert the following tables were cleansed
    deleted_tables = [step_config["pv_table"],
                      step_config["temp_table"],
                      idm.SAS_PROCESS_VARIABLES_TABLE,
                      step_config["sas_ps_table"]]

    for table in deleted_tables:
        table_len = len(cf.get_table_values(table))
        assert table_len == 0

    # ##############################
    # Calculate Traffic Weight
    # ##############################

    # import the data from SQL and sort
    df_surveydata_import_actual = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    df_surveydata_import_actual_sql = df_surveydata_import_actual.sort_values(by='SERIAL')
    df_surveydata_import_actual_sql.index = range(0, len(df_surveydata_import_actual_sql))

    df_tr_data_import_actual = cf.get_table_values(SAS_TRAFFIC_TABLE)

    df_surveydata_import_actual_sql.to_csv(r'C:\Temp\traffic_data_test\tr_surveydata.csv', index=False)
    df_tr_data_import_actual.to_csv(r'C:\Temp\traffic_data_test\tr_data.csv', index=False)

    # ------------ NASSIR

    df_test_traffic_data = pd.read_csv(r"C:\Git_projects\IPS_Legacy_Uplift\tests\data\calculations\december_2017\traffic_weight\trafficdata.csv")
    assert_frame_equal(df_tr_data_import_actual, df_test_traffic_data, check_dtype=False, check_less_precise=True)

    # fix formatting in actual data
    #df_surveydata_import_actual_sql.drop(['EXPENDCODE'], axis=1, inplace=True)

    #df_surveydata_import_actual_sql['SHIFT_PORT_GRP_PV'] = \
    #    df_surveydata_import_actual_sql['SHIFT_PORT_GRP_PV'].apply(pd.to_numeric, errors='coerce')

    df_test_survey_data = pd.read_csv(r'C:\Git_projects\IPS_Legacy_Uplift\tests\data\calculations\december_2017\traffic_weight\surveydata.csv', engine='python')
    df_test_survey_data.columns = df_test_survey_data.columns.str.upper()
    df_test_survey_data = df_test_survey_data.sort_values(by='SERIAL')
    df_test_survey_data.index = range(0, len(df_test_survey_data))

    assert_frame_equal(df_surveydata_import_actual_sql, df_test_survey_data, check_dtype=False, check_less_precise=True)

    # ------------ NASSIR


    # do the calculation
    df_output_merge_final, df_output_summary = do_ips_trafweight_calculation_with_R(df_surveydata_import_actual_sql,
                                                                                    df_tr_data_import_actual)

    # ###########################
    # run checks
    # ###########################

    # test start - turn on when testing/refactoring intermediate steps
    df_test = pd.read_csv(path_to_data + '/output_final.csv', engine='python')
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_output_merge_final, df_test, check_dtype=False, check_less_precise=True)

    df_test2 = pd.read_csv(path_to_data + '/summary_final.csv', engine='python')
    df_test2.columns = df_test2.columns.str.upper()
    assert_frame_equal(df_output_summary, df_test2, check_dtype=False, check_less_precise=True)

    # grab the table data from SQL directly

    # insert the calculated csv data into SQL and pull back

    # compare the calculated data against the SQL data

    # Retrieve and sort python calculated dataframes
    # py_survey_data = result_py_data[0]
    # py_survey_data = py_survey_data.sort_values(by='SERIAL')
    # py_survey_data.index = range(0, len(py_survey_data))
    #
    # py_summary_data = result_py_data[1]
    # py_summary_data.sort_values(by=NR_COLUMNS)
    # py_summary_data[NR_COLUMNS] = py_summary_data[NR_COLUMNS].apply(pd.to_numeric, errors='coerce', downcast='float')
    # py_summary_data.index = range(0, len(py_summary_data))
    #
    # # insert the csv output data into SQL and read back, this is for testing against data pulled from SQL Server
    # test_result_survey = pd.read_csv(path_to_data + '/outputdata_final.csv', engine='python')
    # cf.delete_from_table(OUT_TABLE_NAME)
    # test_result_survey_sql = convert_dataframe_to_sql_format(OUT_TABLE_NAME, test_result_survey)
    # test_result_survey_sql = test_result_survey_sql.sort_values(by='SERIAL')
    # test_result_survey_sql.index = range(0, len(test_result_survey_sql))
    #
    # test_result_summary = pd.read_csv(path_to_data + '/summarydata_final.csv', engine='python')
    # cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)
    # test_result_summary_sql = convert_dataframe_to_sql_format(SUMMARY_OUT_TABLE_NAME, test_result_summary)
    # test_result_summary_sql = test_result_summary_sql.sort_values(by=NR_COLUMNS)
    # test_result_summary_sql[NR_COLUMNS] = test_result_summary_sql[NR_COLUMNS].apply(pd.to_numeric, errors='coerce', downcast='float')
    # test_result_summary_sql.index = range(0, len(test_result_summary_sql))
    #
    # # Assert dfs are equal
    # assert_frame_equal(py_survey_data, test_result_survey_sql, check_dtype=False, check_like=True,
    #                    check_less_precise=True)
    #
    # assert_frame_equal(py_summary_data, test_result_summary_sql, check_dtype=False, check_like=True,
    #                    check_less_precise=True)
    #
    #
    # # put the actual SQL data back in for the remaining steps
    # cf.delete_from_table(OUT_TABLE_NAME)
    # cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)
    # cf.insert_dataframe_into_table(OUT_TABLE_NAME, py_survey_data)
    # cf.insert_dataframe_into_table(SUMMARY_OUT_TABLE_NAME, py_summary_data)

    # # Update Survey Data With Non Response Wt Results
    # idm.update_survey_data_with_step_results(conn, step_config)
    #
    # # ###########################
    # # run checks 9
    # # ###########################
    #
    # table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    # assert table_len == EXPECTED_LEN
    #
    # table_len = len(cf.get_table_values(step_config["temp_table"]))
    # assert table_len == 0
    #
    # # Store Survey Data With NonResponse Wt Results
    # idm.store_survey_data_with_step_results(RUN_ID, conn, step_config)
    #
    # # ###########################
    # # run checks 10
    # # ###########################
    #
    # # Assert SURVEY_SUBSAMPLE_TABLE was populated
    # result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    # table_len = result.shape[0]
    # assert table_len == SURVEY_SUBSAMPLE_LENGTH
    #
    # # Assert all records for corresponding run_id were deleted from ps_table.
    # result = cf.select_data('*', step_config["ps_table"], 'RUN_ID', RUN_ID)
    #
    # # Indicating no dataframe was pulled from SQL.
    # if result == False:
    #     assert True
    #
    # # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    # table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    # assert table_len == 0
    #
    # # Store Non Response Wt Summary
    # idm.store_step_summary(RUN_ID, conn, step_config)
    #
    # # ###########################
    # # run checks 11
    # # ###########################
    #
    # # Assert summary was populated.
    # result = cf.select_data('*', step_config["ps_table"], 'RUN_ID', RUN_ID)
    # table_len = result.shape[0]
    # assert table_len == 207
    #
    # # Assert temp table was cleansed
    # table_len = len(cf.get_table_values(step_config["sas_ps_table"]))
    # assert table_len == 0