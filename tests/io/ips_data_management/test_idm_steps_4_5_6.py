import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
import utils.common_functions as cf
import main.io.data_management as idm
from main.io import import_survey_data
from main.io import import_reference_data
from utils.common_functions import get_sql_connection
import numpy.testing as npt
import os

TEST_DATA_DIR = 'tests/data/ips_data_management/'
STEP_PV_OUTPUT_PATH = TEST_DATA_DIR + 'update_survey_data_with_step_pv_output/'
COPY_PV_PATH = TEST_DATA_DIR + 'copy_step_pvs_for_step_data/'
UPDATE_STEP_DATA_WITH_STEP_PV_OUTPUT_PATH = TEST_DATA_DIR + "update_step_data_with_step_pv_output/"

@pytest.fixture()
def database_connection():
        '''
        This fixture provides the database connection. It is added to the function argument of each test
        and picked up by the test from there. The fixture allows us to re-use the same database connection
        over and over again.
        '''
        return get_sql_connection()

@pytest.mark.usefixtures("database_connection")
class TestIpsDataManagement:

    @staticmethod
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

    @staticmethod
    def amend_rec_id(dataframe, rec_id, ascend=True):
        '''
        This function retrieves REC_ID from text file and inputs to test result dataframe.
        '''

        if ascend:
            for row in range(0, len(dataframe['REC_ID'])):
                dataframe['REC_ID'][row] = rec_id
                rec_id = rec_id + 1
        else:
            for row in range(0, len(dataframe['REC_ID'])):
                dataframe['REC_ID'][row] = rec_id
                rec_id = rec_id - 1

        return dataframe

    @staticmethod
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
        import_survey_data.import_survey_data(survey_data_path, run_id)

        # Import Shift Data
        import_reference_data.import_traffic_data(run_id, shift_data_path)
        import_reference_data.import_traffic_data(run_id, nr_data_path)
        import_reference_data.import_traffic_data(run_id, sea_data_path)
        import_reference_data.import_traffic_data(run_id, tunnel_data_path)
        import_reference_data.import_traffic_data(run_id, air_data_path)
        import_reference_data.import_traffic_data(run_id, unsampled_data_path)

    def test_update_survey_data_with_step_pv_output_with_name_shift_weight(self, database_connection):

        step_config = {'name': "SHIFT_WEIGHT",
                       'spv_table': '[dbo].[SAS_SHIFT_SPV]',
                       "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'", "'SHIFT_FLAG_PV'",
                                      "'CROSSINGS_FLAG_PV'"]
                       }
        run_id = 'update-survey-pvs'

        # delete the data in the table so that we have no data in table for test
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # read and insert into the database the survey data
        test_survey_data = pd.read_pickle(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs.pkl')
        cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, test_survey_data, database_connection)

        # read and insert into the database table the dummy pvs
        test_nr_pv_data = pd.read_pickle(STEP_PV_OUTPUT_PATH + 'test_sw_pv_data.pkl')
        cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)

        # call the test function
        idm.update_survey_data_with_step_pv_output(database_connection, step_config)

        # get the newly updated table data
        results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        temp_output = STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs_result_results.csv'
        results.to_csv(temp_output, index=False)
        results = pd.read_csv(temp_output)

        # remove the temporary written file
        os.remove(temp_output)

        # check ONLY updated pv columns are as expected in results, check NaN values are handled correctly
        stripped_pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        stripped_pv_cols.insert(0, 'SERIAL')  # add the SERIAL column
        test_dummy_1 = results[stripped_pv_cols]

        # get the SERIAL column values as a list, and select rows from updated data that match input data
        serials = test_nr_pv_data['SERIAL']
        test_dummy_2 = test_dummy_1[test_dummy_1['SERIAL'].isin(serials)]

        # clean test data before actually testing results
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # check updated pv columns match the corresponding dummy values
        assert_frame_equal(test_dummy_2, test_nr_pv_data, check_dtype=False, check_like=True)

        # check that the non-pv column values are still the same by dropping pv columns
        columns_to_drop = [item.replace("'", "") for item in step_config['pv_columns']]

        updated_results_without_pvs = results.drop(columns_to_drop, axis=1)
        original_test_data = test_survey_data.drop(columns_to_drop, axis=1)

        assert_frame_equal(updated_results_without_pvs, original_test_data, check_dtype=False,check_like=True)

        # check that spv_table has been deleted
        results_2 = cf.get_table_values(step_config['spv_table'])
        assert len(results_2) == 0

        results_3 = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
        assert len(results_3) == 0

    def test_update_survey_data_with_step_pv_output_with_name_minimums_weight(self, database_connection):
        step_config = {'name': "MINIMUMS_WEIGHT",
                       'spv_table': '[dbo].[SAS_MINIMUMS_SPV]',
                       "pv_columns": ["'MINS_FLAG_PV'", "'MINS_PORT_GRP_PV'", "'MINS_CTRY_GRP_PV'", "'MINS_NAT_GRP_PV'",
                                      "'MINS_CTRY_PORT_GRP_PV'"],
                       "temp_table": "[dbo].[SAS_MINIMUMS_WT]",
                       "sas_ps_table": "[dbo].[SAS_PS_MINIMUMS]",
                       }

        run_id = 'update-survey-pvs'

        # delete the data in the table so that we have no data in table for test
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # read and insert into the database the survey data
        test_survey_data = pd.read_pickle(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs.pkl')
        cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, test_survey_data, database_connection)

        # read and insert into the database the pvs
        test_nr_pv_data = pd.read_csv(STEP_PV_OUTPUT_PATH + 'test_mw_pv_data.csv')
        cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)

        # call the test function
        idm.update_survey_data_with_step_pv_output(database_connection, step_config)

        # get the newly updated table data write the results back to csv to read back and resolve formatting
        results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        temp_output = STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs_result_results.csv'
        results.to_csv(temp_output, index=False)
        results = pd.read_csv(temp_output)

        # remove the temporary written file
        os.remove(temp_output)

        # clean test data before actually testing results
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # check ONLY updated pv columns are as expected in results, check NaN values are handled correctly
        stripped_pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        stripped_pv_cols.insert(0, 'SERIAL')  # add the SERIAL column
        test_dummy_1 = results[stripped_pv_cols]

        # get the SERIAL column values as a list, and select rows from updated data that match input data
        serials = test_nr_pv_data['SERIAL']
        test_dummy_2 = test_dummy_1[test_dummy_1['SERIAL'].isin(serials)]

        # clean test data before actually testing results
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # check updated pv columns match the corresponding dummy values
        assert_frame_equal(test_dummy_2, test_nr_pv_data, check_dtype=False, check_like=True)

        # check that the non-pv column values are still the same by dropping pv columns
        columns_to_drop = [item.replace("'", "") for item in step_config['pv_columns']]
        new_res = results.drop(columns_to_drop, axis=1)
        new_test_res = test_survey_data.drop(columns_to_drop, axis=1)

        assert_frame_equal(new_res, new_test_res, check_dtype=False, check_like=True)

        # check that spv_table has been deleted
        results_2 = cf.get_table_values(step_config['spv_table'])
        assert len(results_2) == 0

        results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
        assert len(results) == 0

        results = cf.get_table_values(step_config["temp_table"])
        assert len(results) == 0

        results = cf.get_table_values(step_config["sas_ps_table"])
        assert len(results) == 0

    def test_copy_step_pvs_for_step_data_shift_weight(self, database_connection):
        step_config = {'name': '[dbo].[SHIFT_DATA]'
            , 'pv_table': '[dbo].[SAS_SHIFT_PV]'
            , 'pv_columns': ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"]
            , 'order': 0
                       }
        run_id = 'copy-step-pvs-for-step-data'

        # clean the tables before putting in data
        cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
        cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)

        # read test data and insert into remote database table
        test_data = pd.read_pickle(COPY_PV_PATH + 'copy_shift_weight_pvs_for_shift_data.pkl')
        cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)

        # run the test function (this inserts into 'SAS_PROCESS_VARIABLE' table in remote database)
        idm.copy_step_pvs_for_step_data(run_id, database_connection, step_config)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        results = cf.get_table_values('SAS_PROCESS_VARIABLE')

        temp_output = COPY_PV_PATH + 'copy_shift_weight_pvs_for_shift_data_results.csv'
        results.to_csv(temp_output, index=False)
        results = pd.read_csv(temp_output)

        # remove the temporary file
        os.remove(temp_output)

        # from the test data make a dataframe of the expected results
        pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        test_inserted_data = test_data[test_data['PV_NAME'].isin(pv_cols)]
        test_inserted_data_2 = test_inserted_data[['PV_NAME', 'PV_DEF']]

        test_results = results[['PROCVAR_NAME', 'PROCVAR_RULE']]

        # check that the PROCVAR_NAME and PROCVAR_RULE string match the ones from test data for the required pvs only
        npt.assert_array_equal(test_inserted_data_2,test_results)

        # Assert step_configuration["pv_table"] has 0 records
        result = cf.get_table_values(step_config['pv_table'])
        assert len(result) == 0

        # Cleanse tables before continuing
        cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
        cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

    def test_copy_step_pvs_for_step_data_unsampled_weight(self, database_connection):
        step_config = {'name': '[dbo].[UNSAMPLED_WEIGHT]'
            , 'pv_table': '[dbo].[SAS_UNSAMPLED_OOH_PV]'
            , 'pv_columns': ["'UNSAMP_PORT_GRP_PV'", "'UNSAMP_REGION_GRP_PV'"]
            , 'order': 0
                       }
        run_id = 'copy-step-pvs-for-step-data'

        # clean the tables before putting in data
        cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
        cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)

        # read test data and insert into remote database table
        test_data = pd.read_csv(COPY_PV_PATH + 'copy_pvs_for_uw.csv')
        cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)

        # run the test function (this inserts into 'SAS_PROCESS_VARIABLE' table in remote database)
        idm.copy_step_pvs_for_step_data(run_id, database_connection, step_config)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        results = cf.get_table_values('SAS_PROCESS_VARIABLE')

        temp_output = COPY_PV_PATH + 'copy_uw_pvs_for_uw_data_results.csv'
        results.to_csv(temp_output, index=False)
        results = pd.read_csv(temp_output)

        # remove the temporary file
        os.remove(temp_output)

        # from the test data make a dataframe of the expected results
        pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        test_inserted_data = test_data[test_data['PV_NAME'].isin(pv_cols)]
        test_inserted_data_2 = test_inserted_data[['PV_NAME', 'PV_DEF']]

        test_results = results[['PROCVAR_NAME', 'PROCVAR_RULE']]

        # check that the PROCVAR_NAME and PROCVAR_RULE string match the ones from test data for the required pvs only
        npt.assert_array_equal(test_inserted_data_2,test_results)

        # Assert step_configuration["pv_table"] has 0 records
        result = cf.get_table_values(step_config['pv_table'])
        assert len(result) == 0

        # Cleanse tables before continuing
        cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
        cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

    def test_update_step_data_with_step_pv_output(self, database_connection):
        # step_config and variables
        step_config = {"pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
                       "pv_table": "[dbo].[SAS_SHIFT_PV]",
                       "data_table": "[dbo].[SAS_SHIFT_DATA]",
                       "temp_table": "[dbo].[SAS_SHIFT_WT]",
                       "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]"}

        # Set up test data/tables
        test_shift_pv_data = pd.read_csv(UPDATE_STEP_DATA_WITH_STEP_PV_OUTPUT_PATH + 'test_shift_pv_data.csv')

        # Get rec_id and amend test dataframe
        rec_id = self.get_rec_id("MAX", step_config["data_table"], database_connection)
        test_shift_pv_data = self.amend_rec_id(test_shift_pv_data, rec_id, ascend=False)

        cf.insert_dataframe_into_table(step_config['pv_table'], test_shift_pv_data, database_connection)

        # run the test function
        idm.update_step_data_with_step_pv_output(database_connection, step_config)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        results = cf.get_table_values(step_config['data_table'])


        temp_output = UPDATE_STEP_DATA_WITH_STEP_PV_OUTPUT_PATH + 'copy_update_step_data_with_step_pv_output.csv'
        results.to_csv(temp_output, index=False)
        results = pd.read_csv(temp_output)

        # get the unique REC_ID of the test_shift_pv_data
        rec_id = test_shift_pv_data["REC_ID"]

        # select all rows with matching updated rec_id
        results_1 = results[results['REC_ID'].isin(rec_id)]

        # create column list of pvs
        cols_temp = [item.replace("[", "") for item in step_config['pv_columns2']]
        cols_to_keep = [item.replace("]", "") for item in cols_temp]
        cols_to_keep.insert(0, "REC_ID")

        # keep only the required columns from results_1 and importantly reset index and drop it
        results_2 = results_1[cols_to_keep]
        results_3 = results_2.reset_index(drop=True)

        # sort rows in test_shift_pv_data by REC_ID and importantly reset index and drop it
        sorted_test_shift_pv_data_1 = test_shift_pv_data.sort_values(by=['REC_ID'])
        sorted_test_shift_pv_data_2 = sorted_test_shift_pv_data_1.reset_index(drop=True)

        # check that the two dataframes match
        assert_frame_equal(results_3, sorted_test_shift_pv_data_2, check_names=False, check_like=True, check_dtype=False)

        # Assert temp tables had been cleanse in function
        results = cf.get_table_values(step_config['pv_table'])
        assert len(results) == 0

        results = cf.get_table_values(step_config['temp_table'])
        assert len(results) == 0

        results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
        assert len(results) == 0

        results = cf.get_table_values(step_config['sas_ps_table'])
        assert len(results) == 0
