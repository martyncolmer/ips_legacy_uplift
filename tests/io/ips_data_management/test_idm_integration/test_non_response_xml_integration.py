import pytest
import json
import pandas as pd
import time
from main.io import import_data

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from main.io import import_traffic_data
from main.io import ips_data_management as idm
from main.utils import process_variables
from main.calculations import calculate_ips_minimums_weight

from main.calculations import calculate_ips_nonresponse_weight as non_resp

with open('data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

step_config = STEP_CONFIGURATION["NON_RESPONSE"]

RUN_ID = 'test_nonresponse_weight_xml'
TEST_DATA_DIR = r'tests\data\ips_data_management\non_response_weight_step'
STEP_NAME = 'NON_RESPONSE'

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


def setup_module():
    """ setup any state specific to the execution of the given module."""

    ist = time.time()
    #import_data_into_database()

    # populates test data within pv table
    #populate_test_pv_table()

    print("Setup")

def teardown_module(module):
    # Delete any previous records from the Survey_Subsample tables for the given run ID
    reset_tables()

    print("Teardown")

#TODO: write correctly the teardown
def reset_tables():
    pass
    # """ Cleanses tables within database. """
    # # List of tables to cleanse entirely
    # tables_to_unconditionally_cleanse = [idm.SAS_SURVEY_SUBSAMPLE_TABLE,
    #                                      idm.SAS_PROCESS_VARIABLES_TABLE]
    #
    # # Try to delete from each table in list.  If exception occurs, assume table is
    # # already empty, and continue deleting from tables in list.
    # for table in tables_to_unconditionally_cleanse:
    #     try:
    #         print("cf.delete_from_table({})".format(table))
    #         cf.delete_from_table(table)
    #     except Exception:
    #         continue
    #
    # # List of tables to cleanse where [RUN_ID] = RUN_ID
    # tables_to_cleanse = ['[dbo].[PROCESS_VARIABLE_PY]',
    #                      '[dbo].[PROCESS_VARIABLE_TESTING]']
    #
    # # Try to delete from each table in list where condition.  If exception occurs,
    # # assume table is already empty, and continue deleting from tables in list
    # for table in tables_to_cleanse:
    #     try:
    #         print("cf.delete_from_table({}, 'RUN_ID', '=', RUN_ID)".format(table))
    #         cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID)
    #     except Exception:
    #         continue
    #
    # # Try to delete from each table in list.  If exception occurs, assume table is
    # # already empty, and continue deleting from tables in list
    # for table in STEP_CONFIGURATION[STEP_NAME]['delete_tables']:
    #     try:
    #         cf.delete_from_table(table)
    #     except Exception:
    #         continue

#TODO: probably not required for this step
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
    survey_data_path = r'tests\data\ips_data_management\import_data\survey\ips1712bv4_amtspnd.csv'#TODO make sure it is correct
    # TODO: this should be the output of shiftweight where the shift_wt column is updated. pv's don't matter as they are nullified
    shift_data_path = r'tests\data\ips_data_management\import_data\external\december\Poss shifts Dec 2017.csv'
    nr_data_path = r'tests\data\ips_data_management\import_data\external\december\Dec17_NR.csv'
    sea_data_path = r'tests\data\ips_data_management\import_data\external\december\Sea Traffic Dec 2017.csv'
    tunnel_data_path = r'tests\data\ips_data_management\import_data\external\december\Tunnel Traffic Dec 2017.csv'
    air_data_path = r'tests\data\ips_data_management\import_data\external\december\Air Sheet Dec 2017 VBA.csv'
    unsampled_data_path = r'tests\data\ips_data_management\import_data\external\december\Unsampled Traffic Dec 2017.csv'

    # Delete table content
    cf.delete_from_table('SURVEY_SUBSAMPLE', 'RUN_ID', '=', RUN_ID)
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')

    import_data.import_survey_data(survey_data_path, RUN_ID)

    # following commented not required
    #df = pd.read_csv(survey_data_path, engine='python')
    # Add the generated run id to the dataset.
    #df['RUN_ID'] = pd.Series(RUN_ID, index=df.index)
    # Insert the imported data into the survey_subsample table on the database.
    #cf.insert_dataframe_into_table('SURVEY_SUBSAMPLE', df, fast=False)

    # Import the external files into the database.
    import_traffic_data.import_traffic_data(RUN_ID, shift_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, nr_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, sea_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, tunnel_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, air_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, unsampled_data_path)

def test_non_response_weight_step():

    # Get database connection
    conn = database_connection()

    # Run step 1 / 8
    #idm.populate_survey_data_for_step(RUN_ID, conn, step_config)

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
    assert table_len == 19980  # TODO get the right number here for this step

    # Run step 2 / 8
    idm.populate_step_data(RUN_ID, conn, step_config) #TODO: don't see any reason to test this simple insertion

    # Run step 3 / 8
    idm.copy_step_pvs_for_survey_data('TEMPLATE', conn, step_config)#TODO: change 'TEMPLATE' run id or update process variable py

    # Get all values from the sas_process_variables table
    results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)

    ########################
    #
    # do some testing for step
    #
    ########################
    # Copy the PV column names
    pv_names = step_config['pv_columns']

    # Strip quotation marks out of the pv_names to use in column comparisons
    for i in range(0, len(pv_names)):
        pv_names[i] = pv_names[i].replace("'", "")

    # Check number of PV records moved matches number passed in through step configuration.
    assert len(results) == len(step_config['pv_columns'])

    # Ensure the pv_names in the results data frame match the expected pv names
    for name in results['PROCVAR_NAME']:
        assert name.upper() in pv_names

    # Get the spv_table values and ensure all records have been deleted
    results = cf.get_table_values(step_config['spv_table'])
    assert len(results) == 0

    # Run step 4 / 8 : Apply Non Response Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_NON_RESPONSE_SPV',
                              in_id='serial')

    # Run step 5 / 8 : Update Survey Data with Non Response Wt PVs Output
    idm.update_survey_data_with_step_pv_output(conn, step_config)

    # Run step 6 / 8 : Copy Non Response Wt PVs for Non Response Data
    idm.copy_step_pvs_for_step_data('TEMPLATE', conn, step_config) # TODO: replcae template with actual run_id for run?

    #TODO: stopped here
    # Run step 7 / 8 : Apply Non Response Wt PVs On Non Response Data
    process_variables.process(dataset='non_response',
                                  in_table_name='SAS_NON_RESPONSE_DATA',
                                  out_table_name='SAS_NON_RESPONSE_PV',
                                  in_id='REC_ID')

    # Run step 8 / 8 : Update NonResponse Data With PVs Output
    idm.update_survey_data_with_step_pv_output(conn, step_config)

    # Calculate Non Response Weight
    SAS_NON_RESPONSE_DATA_TABLE_NAME = 'SAS_NON_RESPONSE_DATA'
    df_surveydata_import = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    df_nr_data_import = cf.get_table_values(SAS_NON_RESPONSE_DATA_TABLE_NAME)

    result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata_import, df_nr_data_import,
                                                          'NON_RESPONSE_WT', 'SERIAL')

    # Update Survey Data With Non Response Wt Results
    idm.update_survey_data_with_step_results(conn, step_config)

    # Store Survey Data With NonResponse Wt Results
    idm.store_survey_data_with_step_results(RUN_ID, conn, step_config)

    # Store Non Response Wt Summary
    idm.store_step_summary(RUN_ID, conn, step_config)

@pytest.mark.skip(reason="not testing this")
def test_non_response_weight_step_2():

    # Get database connection
    conn = database_connection()

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

    # Ensure summary output is equal to expected summary output
    assert_frame_equal(df_summary_actual, df_summary_target, check_dtype=False,check_like=True, check_less_precise=True)

    # Select the newly updated weight column from the dataframe and ensure it matches the expected weights
    assert_frame_equal(df_survey_actual, df_survey_target, check_dtype=False)

    print("Import runtime: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - ist))))
