import pytest
import json
import pandas as pd
import time

from pandas.util.testing import assert_frame_equal
from main.io import CommonFunctions as cf
from tests import common_testing_functions as ctf
from main.io import import_traffic_data
from main.io import ips_data_management as idm
from main.utils import process_variables
from main.calculations import calculate_ips_minimums_weight

from main.calculations import calculate_ips_nonresponse_weight as non_resp

with open('data/xml_steps_configuration.json') as config_file: STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test_nonresponse_weight_xml'
TEST_DATA_DIR = r'tests\data\ips_data_management\non_response_weight_step'
STEP_NAME = 'NON_RESPONSE'
SAS_NON_RESPONSE_DATA_TABLE_NAME = 'SAS_NON_RESPONSE_DATA'

step_config = STEP_CONFIGURATION["NON_RESPONSE"]

OUT_TABLE_NAME = "SAS_NON_RESPONSE_WT"  # output data
SUMMARY_OUT_TABLE_NAME = "SAS_PS_NON_RESPONSE"  # output data

PV_RUN_ID = 'TEMPLATE'

# data lengths for testing
SURVEY_SUBSAMPLE_LENGTH = 21638
EXPECTED_LEN = 19980
NON_RESPONSE_DATA_LENGTH = 694
NON_RESPONSE_SAS_PROCESS_VARIABLE_TABLE_LENGTH = 2

# columns to sort the summary results by in order to check calculated dataframes match expected results
NR_COLUMNS = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT',
              'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']

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

    # Deletes data from tables as necessary
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data
    survey_data_path = r'tests/data/ips_data_management/non_response_integration/december/nr_survey_data2.csv'
    ctf.import_survey_data_into_database(survey_data_path, RUN_ID)

    # Import external data
    import_data_dir = r'tests\data\import_data\dec'
    ctf.import_test_data_into_database(import_data_dir, RUN_ID, load_survey_data=False)

    # populates test data within pv table
    conn = database_connection()
    ctf.populate_test_pv_table(conn, RUN_ID, PV_RUN_ID)

    print("Setup")


def teardown_module(module):
    # Delete any previous records from the Survey_Subsample tables for the given run ID
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Cleanses Survey Subsample table.
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)


@pytest.mark.parametrize('path_to_data', [
    r'tests\data\calculations\december_2017\non_response_weight',
    #r'tests\data\calculations\november_2017\non_response_weight', # ignored as data not available
    #r'tests\data\calculations\october_2017\non_response_weight', # ignored as data not available
    ])
def test_non_response_weight_step(path_to_data):

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
    assert table_len == NON_RESPONSE_DATA_LENGTH

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

    # ###########################
    # run checks 3
    # ###########################

    # Run step 4  : Apply Non Response Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_NON_RESPONSE_SPV',
                              in_id='serial')

    # ###########################
    # run checks 4
    # ###########################

    table_len = len(cf.get_table_values(step_config["spv_table"]))
    assert table_len == EXPECTED_LEN

    # Run step 5 : Update Survey Data with Non Response Wt PVs Output
    idm.update_survey_data_with_step_pv_output(conn, step_config)

    # ###########################
    # run checks 5
    # ###########################

    # Check all columns in SAS_SURVEY_SUBSAMPLE have been altered
    result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    for column in step_config['pv_columns']:
        column_name = column.replace("'", "")
        assert len(result[column_name]) == EXPECTED_LEN
        assert result[column_name].sum() != 0

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
    assert table_len == NON_RESPONSE_SAS_PROCESS_VARIABLE_TABLE_LENGTH

    # Run step 7 : Apply Non Response Wt PVs On Non Response Data
    process_variables.process(dataset='non_response',
                                  in_table_name='SAS_NON_RESPONSE_DATA',
                                  out_table_name='SAS_NON_RESPONSE_PV',
                                  in_id='REC_ID')

    # ###########################
    # run checks 7
    # ###########################

    table_len = len(cf.get_table_values(step_config["pv_table"]))
    assert table_len == NON_RESPONSE_DATA_LENGTH

    # Run step 8 : Update NonResponse Data With PVs Output
    idm.update_step_data_with_step_pv_output(conn, step_config)

    # ###########################
    # run checks 8
    # ###########################

    # Assert data table was populated
    table_len = len(cf.get_table_values(step_config["data_table"]))
    assert table_len == NON_RESPONSE_DATA_LENGTH

    # Assert the following tables were cleansed
    deleted_tables = [step_config["pv_table"],
                      step_config["temp_table"],
                      idm.SAS_PROCESS_VARIABLES_TABLE,
                      step_config["sas_ps_table"]]

    for table in deleted_tables:
        table_len = len(cf.get_table_values(table))
        assert table_len == 0

    # ##############################
    # Calculate Non Response Weight
    # ##############################

    # import the data from SQL and sort
    df_surveydata_import_actual = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    df_surveydata_import_actual_sql = df_surveydata_import_actual.sort_values(by='SERIAL')
    df_surveydata_import_actual_sql.index = range(0, len(df_surveydata_import_actual_sql))

    df_nr_data_import_actual = cf.get_table_values(SAS_NON_RESPONSE_DATA_TABLE_NAME)

    # fix formatting in actual data
    df_surveydata_import_actual_sql.drop(['EXPENDCODE'], axis=1, inplace=True)
    df_surveydata_import_actual_sql['SHIFT_PORT_GRP_PV'] = \
        df_surveydata_import_actual_sql['SHIFT_PORT_GRP_PV'].apply(pd.to_numeric, errors='coerce')

    # do the calculation step
    result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata_import_actual_sql, df_nr_data_import_actual,
                                                          'NON_RESPONSE_WT', 'SERIAL')

    # ###########################
    # run checks
    # ###########################

    # Retrieve and sort python calculated dataframes
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    py_summary_data = result_py_data[1]
    py_summary_data.sort_values(by=NR_COLUMNS)
    py_summary_data[NR_COLUMNS] = py_summary_data[NR_COLUMNS].apply(pd.to_numeric, errors='coerce', downcast='float')
    py_summary_data.index = range(0, len(py_summary_data))

    # insert the csv output data into SQL and read back, this is for testing against data pulled from SQL Server
    test_result_survey = pd.read_csv(path_to_data + '/outputdata_final.csv', engine='python')
    cf.delete_from_table(OUT_TABLE_NAME)
    test_result_survey_sql = convert_dataframe_to_sql_format(OUT_TABLE_NAME, test_result_survey)
    test_result_survey_sql = test_result_survey_sql.sort_values(by='SERIAL')
    test_result_survey_sql.index = range(0, len(test_result_survey_sql))

    test_result_summary = pd.read_csv(path_to_data + '/summarydata_final.csv', engine='python')
    cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)
    test_result_summary_sql = convert_dataframe_to_sql_format(SUMMARY_OUT_TABLE_NAME, test_result_summary)
    test_result_summary_sql = test_result_summary_sql.sort_values(by=NR_COLUMNS)
    test_result_summary_sql[NR_COLUMNS] = test_result_summary_sql[NR_COLUMNS].apply(pd.to_numeric, errors='coerce', downcast='float')
    test_result_summary_sql.index = range(0, len(test_result_summary_sql))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey_sql, check_dtype=False, check_like=True,
                       check_less_precise=True)

    assert_frame_equal(py_summary_data, test_result_summary_sql, check_dtype=False, check_like=True,
                       check_less_precise=True)


    # put the actual SQL data back in for the remaining steps
    cf.delete_from_table(OUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)
    cf.insert_dataframe_into_table(OUT_TABLE_NAME, py_survey_data)
    cf.insert_dataframe_into_table(SUMMARY_OUT_TABLE_NAME, py_summary_data)

    # Update Survey Data With Non Response Wt Results
    idm.update_survey_data_with_step_results(conn, step_config)

    # ###########################
    # run checks 9
    # ###########################

    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(step_config["temp_table"]))
    assert table_len == 0

    # Store Survey Data With NonResponse Wt Results
    idm.store_survey_data_with_step_results(RUN_ID, conn, step_config)

    # ###########################
    # run checks 10
    # ###########################

    # Assert SURVEY_SUBSAMPLE_TABLE was populated
    result = cf.select_data('*', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == SURVEY_SUBSAMPLE_LENGTH

    # Assert all records for corresponding run_id were deleted from ps_table.
    result = cf.select_data('*', step_config["ps_table"], 'RUN_ID', RUN_ID)

    # Indicating no dataframe was pulled from SQL.
    if result == False:
        assert True

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0

    # Store Non Response Wt Summary
    idm.store_step_summary(RUN_ID, conn, step_config)

    # ###########################
    # run checks 11
    # ###########################

    # Assert summary was populated.
    result = cf.select_data('*', step_config["ps_table"], 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == 207

    # Assert temp table was cleansed
    table_len = len(cf.get_table_values(step_config["sas_ps_table"]))
    assert table_len == 0