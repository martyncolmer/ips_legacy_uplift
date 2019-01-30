import time

import json
import numpy as np
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_traffic_weight import do_ips_trafweight_calculation_with_R
from utils import common_functions as cf
from main.io import data_management as idm
from main.utils import process_variables
from tests import common_testing_functions as ctf

with open('data/steps_configuration.json') as config_file: STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'test_traffic_weight_xml'
STEP_NAME = 'TRAFFIC_WEIGHT'
step_config = STEP_CONFIGURATION[STEP_NAME]
PV_RUN_ID = 'TEMPLATE'

# data lengths for testing
SURVEY_SUBSAMPLE_LENGTH = 19980
EXPECTED_LEN = 17731
TRAFFIC_DATA_LENGTH = 238
TRAFFIC_SAS_PROCESS_VARIABLE_TABLE_LENGTH = 1

TRAFFIC_DATA_TABLE = 'TRAFFIC_DATA'
SAS_TRAFFIC_TABLE = 'SAS_TRAFFIC_DATA'

OUT_TABLE_NAME = "SAS_TRAFFIC_WT"  # output data
SUMMARY_OUT_TABLE_NAME = "SAS_PS_TRAFFIC"  # output data

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
    # import_data_into_database()

    # Deletes data from tables as necessary.
    ctf.reset_test_tables(RUN_ID, STEP_CONFIGURATION[STEP_NAME])

    # Import survey data
    survey_data_path = r'tests/data/ips_data_management/traffic_weight_integration/december/survey_trafficdata.csv'
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

    print("Teardown")


@pytest.mark.parametrize('path_to_data', [
    r'tests/data/ips_data_management/traffic_weight_integration\december'
    # r'tests/data/ips_data_management/traffic_weight_integration\october', # ignored as data not available
    # r'tests/data/ips_data_management/traffic_weight_integration\november', # ignored as data not available
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

    # ###################################################################
    # Get traffic data and compare to existing CSVs
    # ###################################################################

    # import the traffic data from SQL
    df_tr_data_import_actual = cf.get_table_values(SAS_TRAFFIC_TABLE)

    # read in the comparative traffic data csv
    df_test_traffic_data = pd.read_csv(path_to_data + r"\trafficdata_before_calculation.csv")

    # match the SQL data to the csv data it should match
    df_tr_data_import_actual['AM_PM_NIGHT'] = 0.0
    df_tr_data_import_actual.replace("", np.nan, inplace=True)
    df_tr_data_import_actual['VEHICLE'] = np.NaN

    # drop rec_id
    df_tr_data_import_actual = df_tr_data_import_actual.drop('REC_ID', axis=1)
    df_test_traffic_data = df_test_traffic_data.drop('REC_ID', axis=1)

    # get column list
    mylista = df_tr_data_import_actual.columns.values
    mylist = mylista.tolist()

    # sort the values
    df_tr_data_import_actual = df_tr_data_import_actual.sort_values(mylist)
    df_test_traffic_data = df_test_traffic_data.sort_values(mylist)

    # reindex
    df_tr_data_import_actual.index = range(0, len(df_tr_data_import_actual))
    df_test_traffic_data.index = range(0, len(df_test_traffic_data))

    assert_frame_equal(df_tr_data_import_actual, df_test_traffic_data, check_dtype=False, check_less_precise=True)

    # ###################################################################
    # Get survey data and compare to existing CSVs
    # ###################################################################

    # import the survey data from SQL and sort and reindex
    df_surveydata_import_actual = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    df_surveydata_import_actual_sql = df_surveydata_import_actual.sort_values(by='SERIAL')
    df_surveydata_import_actual_sql.index = range(0, len(df_surveydata_import_actual_sql))

    # data gotten only for testing purposes
    # df_test_survey_data = pd.read_csv(path_to_data + r'/surveydata_before_calculation.csv', engine='python')
    # df_test_survey_data.columns = df_test_survey_data.columns.str.upper()
    # df_test_survey_data = df_test_survey_data.sort_values(by='SERIAL')
    # df_test_survey_data.index = range(0, len(df_test_survey_data))

    # do the calculation
    df_output_merge_final, df_output_summary = do_ips_trafweight_calculation_with_R(df_surveydata_import_actual_sql,
                                                                                    df_tr_data_import_actual)

    # ###########################
    # run checks
    # ###########################

    # test the returned data matches expected
    df_test = pd.read_csv(path_to_data + '/output_final.csv', engine='python')
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_output_merge_final, df_test, check_dtype=False, check_less_precise=True)

    df_test2 = pd.read_csv(path_to_data + '/summary_final.csv', engine='python')
    df_test2.columns = df_test2.columns.str.upper()
    assert_frame_equal(df_output_summary, df_test2, check_dtype=False, check_less_precise=True)

    # Update Survey Data traffic weight Results
    idm.update_survey_data_with_step_results(conn, step_config)

    # ###########################
    # run checks 9
    # ###########################

    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == EXPECTED_LEN

    table_len = len(cf.get_table_values(step_config["temp_table"]))
    assert table_len == 0

    # Store Survey Data With traffic weight Results
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
    if result is False:
        assert True

    # Assert SAS_SURVEY_SUBSAMPLE_TABLE was cleansed
    table_len = len(cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE))
    assert table_len == 0

    # Store traffic Wt Summary
    idm.store_step_summary(RUN_ID, conn, step_config)

    # ###########################
    # run checks 11
    # ###########################

    # Assert summary was populated.
    result = cf.select_data('*', step_config["ps_table"], 'RUN_ID', RUN_ID)
    table_len = result.shape[0]
    assert table_len == 85

    # Assert temp table was cleansed
    table_len = len(cf.get_table_values(step_config["sas_ps_table"]))
    assert table_len == 0
