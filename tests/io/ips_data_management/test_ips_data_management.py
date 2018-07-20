import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
import main.io.CommonFunctions as cf
import main.io.ips_data_management as idm
from main.io import import_data
from main.io import import_traffic_data
from main.io.CommonFunctions import get_sql_connection
from main.main import shift_weight_step
import numpy.testing as npt

import sys

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

# #TODO: use an Oracle dataset to verify if this test works for unsampled weight
# def test_copy_step_pvs_for_step_data_unsampled_weight(self, database_connection):
#     step_config = {'name': '[dbo].[UNSAMPLED_WEIGHT]'
#         , 'pv_table': '[dbo].[SAS_UNSAMPLED_OOH_PV]'
#         , 'pv_columns': ["[UNSAMP_PORT_GRP_PV]", "[UNSAMP_REGION_GRP_PV]"]
#         , 'order': 1
#                    }
#     run_id = 'copy-step-pvs-for-step-data'
#
#     # read test data and insert into remote database table
#     test_data = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data.pkl')
#     cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)
#
#     # run the test function
#     idm.copy_step_pvs_for_step_data(run_id, database_connection, step_config)
#     results = cf.get_table_values('SAS_PROCESS_VARIABLE')
#
#     # write the results back to csv, and read the csv back (this solves the data type matching issues)
#     results.to_csv(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.csv', index=False)
#     results = pd.read_csv(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.csv')
#
#     # Assert step_configuration["pv_table"] has 0 records
#     result = cf.get_table_values(step_config['pv_table'])
#     assert len(result) == 0
#
#     # Cleanse tables before continuing
#     cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
#     cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
#
#     # Pickle some test results
#     test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.pkl')
#
#     # Assert equal
#     assert_frame_equal(results, test_results, check_dtype=False)

@pytest.mark.usefixtures("database_connection")
class TestIpsDataManagement:

    def get_rec_id(self, value, table, database_connection):
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

    def amend_rec_id(self, dataframe, rec_id, ascend=True):
        '''
        This function retrieves REC_ID from text file and inputs to test result dataframe.
        '''

        if ascend==True:
            for row in range(0, len(dataframe['REC_ID'])):
                dataframe['REC_ID'][row] = rec_id
                rec_id = rec_id + 1
        else:
            for row in range(0, len(dataframe['REC_ID'])):
                dataframe['REC_ID'][row] = rec_id
                rec_id = rec_id - 1

        return dataframe

    def import_data_into_database(self):
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

    @pytest.mark.skip(reason="for now")
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
        results.to_csv(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs_result_results.csv', index=False)
        results = pd.read_csv(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs_result_results.csv')

        # check ONLY updated pv columns are as expected in results, check NaN values are handled correctly
        stripped_pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        stripped_pv_cols.insert(0, 'SERIAL')  # add the SERIAL column
        test_dummy_1 = results[stripped_pv_cols]

        # TODO: select from the results the columns with corresponding matching serial columns and compare
        # TODO: test with different NaN/None values in columns and rows. Probably best to just ignore rows
        # TODO: with NaNs only
        assert_frame_equal(test_dummy_1.head(test_dummy_1.shape[0] - 1), test_nr_pv_data, check_dtype=False,
                           check_like=True)

        # clean test data before actually testing results
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # check that the updated pv columns match the correct dummy values. (discard the last row in this test
        # which is not read into results as it is a row of NaN)
        assert_frame_equal(test_dummy_1.head(test_dummy_1.shape[0] - 1), test_nr_pv_data, check_dtype=False,
                           check_like=True)

        # check that the non-pv column values are still the same by dropping pv columns
        columns_to_drop = [item.replace("'", "") for item in step_config['pv_columns']]
        new_res = results.drop(columns_to_drop, axis=1)
        new_test_res = test_survey_data.drop(columns_to_drop, axis=1)

        assert_frame_equal(new_res, new_test_res, check_dtype=False,check_like=True)

        # check that spv_table has been deleted
        results_2 = cf.get_table_values(step_config['spv_table'])
        assert len(results_2) == 0

        results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
        assert len(results) == 0

    @pytest.mark.skip(reason="for now")
    def test_update_survey_data_with_step_pv_output_with_name_minimums_weight(self, database_connection):
        step_config = {'name': "MINIMUMS_WEIGHT",
                       'spv_table': '[dbo].[SAS_MINIMUMS_SPV]',
                       "pv_columns": ["'MINS_FLAG_PV'", "'MINS_PORT_GRP_PV'", "'MINS_CTRY_GRP_PV'", "'MINS_NAT_GRP_PV'",
                                      "'MINS_CTRY_PORT_GRP_PV'"],
                       "weight_table": "[dbo].[SAS_MINIMUMS_WT]",
                       "sas_ps_table": "[dbo].[SAS_PS_MINIMUMS]",
                       }

        run_id = 'update-survey-pvs'

        # delete the data in the table so that we have no data in table for test
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # read and insert into the database the survey data
        test_survey_data = pd.read_pickle(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs.pkl')
        cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, test_survey_data, database_connection)

        test_nr_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_sw_pv_data.pkl')
        cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)

        idm.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)

        # get the newly updated table data write the results back to csv to read back and resolve formatting
        results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        results.to_csv(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs_result_results.csv', index=False)
        results = pd.read_csv(STEP_PV_OUTPUT_PATH + 'update_survey_data_pvs_result_results.csv')

        # clean test data before actually testing results
        cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
        cf.delete_from_table(step_config['spv_table'])

        # check ONLY updated pv columns are as expected in results, check NaN values are handled correctly
        stripped_pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        stripped_pv_cols.insert(0, 'SERIAL')  # add the SERIAL column
        test_dummy_1 = results[stripped_pv_cols]

        # TODO: select from the results the columns with corresponding matching serial columns and compare
        # TODO: test with different NaN/None values in columns and rows
        assert_frame_equal(test_dummy_1.head(test_dummy_1.shape[0] - 1), test_nr_pv_data, check_dtype=False,
                           check_like=True)

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

        results = cf.get_table_values(step_config["weight_table"])
        assert len(results) == 0

        results = cf.get_table_values(step_config["sas_ps_table"])
        assert len(results) == 0

    @pytest.mark.skip(reason="for now")
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
        test_data = pd.read_pickle(TEST_DATA_DIR + 'shift_weight/copy_shift_weight_pvs_for_shift_data.pkl')
        cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)

        # Plug it in to copy_step_pvs_for_step_data(run_id, conn, step_configuration)
        idm.copy_step_pvs_for_step_data(run_id, database_connection, step_config)

        # clean the table with matching run_id before testing
        cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        results = cf.get_table_values('SAS_PROCESS_VARIABLE')
        results.to_csv(TEST_DATA_DIR + 'shift_weight/copy_shift_weight_pvs_for_shift_data_results.csv', index=False)
        results = pd.read_csv(TEST_DATA_DIR + 'shift_weight/copy_shift_weight_pvs_for_shift_data_results.csv')

        # TODO: check that only required pvs are copied over

        # from the test data make a dataframe of the expected results
        pv_cols = [item.replace("'", "") for item in step_config['pv_columns']]
        test_inserted_data = test_data[test_data['PV_NAME'].isin(pv_cols)]
        test_inserted_data_2 = test_inserted_data[['PV_NAME', 'PV_DEF']]

        test_results = results[['PROCVAR_NAME', 'PROCVAR_RULE']]

        # check that the PROCVAR_NAME and PROCVAR_RULE string match the ones from test data for the required pvs only
        #assert_frame_equal(test_inserted_data_2, test_results, check_names=False)
        npt.assert_array_equal(test_inserted_data_2,test_results)

        # Assert step_configuration["pv_table"] has 0 records
        result = cf.get_table_values(step_config['pv_table'])
        assert len(result) == 0

        # Cleanse tables before continuing
        cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
        cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

        # Pickle some test results
        test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.pkl')

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
                       "weight_table": "[dbo].[SAS_SHIFT_WT]",
                       "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]"}

        # Set up test data/tables
        test_shift_pv_data = pd.read_csv(UPDATE_STEP_DATA_WITH_STEP_PV_OUTPUT_PATH + '/test_shift_pv_data.csv')

        # Get rec_id and amend test dataframe
        rec_id = self.get_rec_id("MAX", step_config["data_table"], database_connection)
        test_shift_pv_data = self.amend_rec_id(test_shift_pv_data, rec_id, ascend=False)

        cf.insert_dataframe_into_table(step_config['pv_table'], test_shift_pv_data, database_connection)

        # run the test function
        idm.update_step_data_with_step_pv_output(database_connection, step_config)

        # write the results back to csv, and read the csv back (this solves the data type matching issues)
        results = cf.get_table_values(step_config['data_table'])
        results.to_csv(UPDATE_STEP_DATA_WITH_STEP_PV_OUTPUT_PATH + 'copy_update_step_data_with_step_pv_output.csv', index=False)
        results = pd.read_csv(UPDATE_STEP_DATA_WITH_STEP_PV_OUTPUT_PATH + 'copy_update_step_data_with_step_pv_output.csv')

        # get the unique REC_ID of the test_shift_pv_data
        rec_id = test_shift_pv_data["REC_ID"]

        # select all rows with matching updated rec_id
        results_1 = results[results['REC_ID'].isin(rec_id)]

        # create column list
        cols_temp = [item.replace("[", "") for item in step_config['pv_columns2']]
        cols_to_keep = [item.replace("]", "") for item in cols_temp]
        cols_to_keep.insert(0, "REC_ID")

        # select only the required columns from results_1
        results_2 = results_1[cols_to_keep]
        results_3 = results_2.reset_index(drop=True)

        #results_2 = results_1[['REC_ID', 'SHIFT_PORT_GRP_PV', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV']]

        # sort rows in test_shift_pv_data by REC_ID
        sorted_test_shift_pv_data_1 = test_shift_pv_data.sort_values(by=['REC_ID'])
        sorted_test_shift_pv_data_2 = sorted_test_shift_pv_data_1.reset_index(drop=True)

        # check that the two dataframes match
        assert_frame_equal(results_3, sorted_test_shift_pv_data_2, check_names=False, check_like=True, check_dtype=False)

        #npt.assert_array_equal(results_2, sorted_test_shift_pv_data)

        # # from the updated results table get the dataframe of the expected results
        # pv_cols = [item.replace("'", "") for item in step_config['pv_columns2']]
        # test_results = results[results['REC_ID'].isin(pv_cols)]
        # test_inserted_data_2 = test_inserted_data[['PV_NAME', 'PV_DEF']]
        #
        # test_results = results[['PROCVAR_NAME', 'PROCVAR_RULE']]


        # sql = """
        # SELECT TOP(5)[REC_ID]
        #   ,[PORTROUTE]
        #   ,[WEEKDAY]
        #   ,[ARRIVEDEPART]
        #   ,[TOTAL]
        #   ,[AM_PM_NIGHT]
        #   ,[SHIFT_PORT_GRP_PV]
        #   ,[AM_PM_NIGHT_PV]
        #   ,[WEEKDAY_END_PV]
        # FROM [ips_test].[dbo].[SAS_SHIFT_DATA]
        # ORDER BY REC_ID DESC
        # """
        # results = pd.read_sql(sql, database_connection)
        #
        # # Create expected test results and assert equal
        # test_results = pd.read_pickle(TEST_DATA_DIR + 'update_shift_data_pvs_result.pkl')
        # test_results = amend_rec_id(test_results, rec_id, ascend=False)

        # print("results: {}".format(results))
        # print("test_results: {}".format(test_results))
        #
        # assert_frame_equal(results, test_results, check_dtype=False)

        # Assert temp tables had been cleanse in function
        results = cf.get_table_values(step_config['pv_table'])
        assert len(results) == 0

        results = cf.get_table_values(step_config['weight_table'])
        assert len(results) == 0

        results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
        assert len(results) == 0

        results = cf.get_table_values(step_config['sas_ps_table'])
        assert len(results) == 0


    @pytest.mark.xfail
    def test_update_survey_data_with_step_results(database_connection):
        # step_config and variables
        step_config = {"name": "SHIFT_WEIGHT",
                       "weight_table": "[dbo].[SAS_SHIFT_WT]",
                       "results_columns": ["[SHIFT_WT]"]}

        # set up test data/tables - export fake data into SAS_SURVEY_SUBSAMPLE
        sas_survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'sas_survey_subsample_test_input.pkl')
        cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection)

        # and SAS_SHIFT_WT with matching SERIAL numbers
        sas_shift_wt_input = pd.read_pickle(TEST_DATA_DIR + 'sas_shift_wt_test_input.pkl')
        cf.insert_dataframe_into_table(step_config["weight_table"], sas_shift_wt_input, database_connection)

        # Run that badger
        idm.update_survey_data_with_step_results(database_connection, step_config)
        results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

        # Create expected test results and test against result
        test_results = pd.read_pickle(TEST_DATA_DIR + 'test_results_of_update_survey_data_with_step_results.pkl')

        assert_frame_equal(results, test_results, check_dtype=False)

        # Assert temp tables had been cleansed in function
        result = cf.get_table_values(step_config['weight_table'])
        assert len(result) == 0


    @pytest.mark.xfail
    def test_store_survey_data_with_step_results(database_connection):
        # step_config and variables
        step_config = {"name": "SHIFT_WEIGHT",
                       "nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]",
                                       "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
                       "ps_table": "[dbo].[PS_SHIFT_DATA]"}
        run_id = 'tst-store-survey-data-with-shift-wt-res'

        # Set up records in SURVEY_SUBSAMPLE with above run_id
        survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'survey_subsample_test_input.pkl')
        cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, survey_subsample_input, database_connection)

        # Set up records in SAS)SURVEY_SUBSAMPLE with same SERIAL as above
        sas_survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'sas_survey_subsample_test_store_input.pkl')
        cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection)

        # run that badger
        idm.store_survey_data_with_step_results(run_id, database_connection, step_config)

        # Assert temp tables had been cleansed in function
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
        WHERE SERIAL IN ('999999999991', '999999999992', '999999999993', '999999999994', '999999999995')
        AND RUN_ID = '{}'
        """.format(idm.SURVEY_SUBSAMPLE_TABLE, run_id)
        results = pd.read_sql(sql, database_connection)

        # Cleanse and delete from_survey_subsample where run_id = run_id
        cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)

        # Create expected test results and test against result
        test_results = pd.read_pickle(TEST_DATA_DIR + 'test_results_store_survey_data_with_step_results.pkl')

        assert_frame_equal(results, test_results, check_dtype=False)

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
        idm.store_step_summary(run_id, database_connection, step_config)
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