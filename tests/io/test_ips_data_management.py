import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
import main.io.CommonFunctions as cf
import main.io.ips_data_management as idm
from main.io import import_data
from main.io import import_traffic_data
from main.io.CommonFunctions import get_sql_connection
from main.main import shift_weight_step

import sys

TEST_DATA_DIR = 'tests/data/ips_data_management/'
STEP_PV_OUTPUT_PATH = TEST_DATA_DIR + 'update_survey_data_with_step_pv_output/'

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
        # TODO: test with different NaN/None values in columns and rows
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

        # read and insert into the database the pvs
        test_nr_pv_data = pd.read_csv(STEP_PV_OUTPUT_PATH + 'test_mw_pv_data.csv')
        cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)

        # call the test function
        idm.update_survey_data_with_step_pv_output(database_connection, step_config)

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

    # def test_copy_step_pvs_for_step_data(self, database_connection):
    #     step_config = {'name': '[dbo].[SHIFT_DATA]'
    #         , 'pv_table': '[dbo].[SAS_SHIFT_PV]'
    #         , 'pv_columns': ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"]
    #         , 'order': 0
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
    #
    # # TODO: use oracle to get the actual data
    # def test_update_step_data_with_step_pv_output(self, database_connection):
    #     # step_config and variables #TODO: might be worth checking against original XML
    #     step_config = {"pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
    #                    "pv_table": "[dbo].[SAS_SHIFT_PV]",
    #                    "data_table": "[dbo].[SAS_SHIFT_DATA]",
    #                    "weight_table": "[dbo].[SAS_SHIFT_WT]",
    #                    "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]"}
    #
    #     # get the right data to check against from Oracle
    #     # Set up test data/tables
    #     test_shift_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_shift_pv_data.pkl')
    #
    #     # Get rec_id and amend test dataframe
    #     rec_id = self.get_rec_id("MAX", step_config["data_table"], database_connection)
    #     test_shift_pv_data = self.amend_rec_id(test_shift_pv_data, rec_id, ascend=False)
    #
    #     # insert the data into the database table
    #     cf.insert_dataframe_into_table(step_config['pv_table'], test_shift_pv_data, database_connection)
    #
    #     # call the function to be tested
    #     idm.update_step_data_with_step_pv_output(database_connection, step_config)
    #
    #     # Grab the data to check results
    #     sql = """
    #     SELECT TOP(5)[REC_ID]
    #       ,[PORTROUTE]
    #       ,[WEEKDAY]
    #       ,[ARRIVEDEPART]
    #       ,[TOTAL]
    #       ,[AM_PM_NIGHT]
    #       ,[SHIFT_PORT_GRP_PV]
    #       ,[AM_PM_NIGHT_PV]
    #       ,[WEEKDAY_END_PV]
    #     FROM [ips_test].[dbo].[SAS_SHIFT_DATA]
    #     ORDER BY REC_ID DESC
    #     """
    #     results = pd.read_sql(sql, database_connection)
    #
    #     # Create expected test results and assert equal
    #     test_results = pd.read_pickle(TEST_DATA_DIR + 'update_shift_data_pvs_result.pkl')
    #     test_results = self.amend_rec_id(test_results, rec_id, ascend=False)
    #
    #     print("results: {}".format(results))
    #     print("test_results: {}".format(test_results))
    #
    #     assert_frame_equal(results, test_results, check_dtype=False)
    #
    #     # Assert temp tables had been cleanse in function
    #     results = cf.get_table_values(step_config['pv_table'])
    #     assert len(results) == 0
    #
    #     results = cf.get_table_values(step_config['weight_table'])
    #     assert len(results) == 0
    #
    #     results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
    #     assert len(results) == 0
    #
    #     results = cf.get_table_values(step_config['sas_ps_table'])
    #     assert len(results) == 0

#
#     def test_nullify_survey_subsample_pv_values(database_connection):
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'nullify_pv_survey_data.pkl')
#         # test to make sure our test data is different from the data after applying the function
#         assert test_data['SHIFT_PORT_GRP_PV'].isnull().sum() == 0
#         assert test_data['WEEKDAY_END_PV'].isnull().sum() == 0
#
#         # Insert the imported data into the survey_subsample table on the database.
#         cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)
#         idm.nullify_survey_subsample_pv_values("nullify-test", database_connection, ["[SHIFT_PORT_GRP_PV]",
#                                                                                      "[WEEKDAY_END_PV]"])
#
#         # COMMENTED AS CAUSING TEST TO FAIL.  RECORDS BEING INSERTED AND THEN DELETED BEFORE TESTING - ET
#         # cleanse tables before testing output
#         # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'nullify-test')
#
#         result = cf.select_data('SHIFT_PORT_GRP_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', 'nullify-test')
#         assert result['SHIFT_PORT_GRP_PV'].isnull().sum() == len(result)
#         result = cf.select_data('WEEKDAY_END_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "nullify-test")
#         assert result['WEEKDAY_END_PV'].isnull().sum() == len(result)
#
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'nullify-test')
#
#
#     def test_move_survey_subsample_to_sas_table(database_connection):
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample.pkl')
#
#         cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         idm.move_survey_subsample_to_sas_table('move-survey-test', database_connection, step_name="")
#
#         result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')
#
#         # one record has a value beyond the RESPNSE range
#         assert len(result) == (len(test_data)-1)
#         assert result.columns.isin(idm.COLUMNS_TO_MOVE).sum() == len(idm.COLUMNS_TO_MOVE)
#
#         test_result = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample_result.pkl')
#         assert_frame_equal(result, test_result)
#
#
#     def test_move_survey_subsample_to_sas_table_traffic_weight(database_connection):
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample.pkl')
#
#         cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         idm.move_survey_subsample_to_sas_table('move-survey-test', database_connection, step_name="TRAFFIC_WEIGHT")
#
#         result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')
#
#         # two records have a value beyond the RESPNSE range
#         assert len(result) == (len(test_data)-2)
#         assert result.columns.isin(idm.COLUMNS_TO_MOVE).sum() == len(idm.COLUMNS_TO_MOVE)
#
#         test_result = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample_traffic_result.pkl')
#         assert_frame_equal(result, test_result)
#
#
#     def test_populate_survey_data_for_step(database_connection):
#         # this is an integration of the above tests so we will keep things simple
#
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample.pkl')
#         cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)
#
#         step_config = {'nullify_pvs': ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]"],
#                        'name': 'SHIFT_WEIGHT',
#                        'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"]}
#         idm.populate_survey_data_for_step('move-survey-test', database_connection, step_config)
#
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')
#
#         result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#         test_result = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample_result.pkl')
#         assert_frame_equal(result, test_result)
#
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         result = cf.get_table_values("SAS_SHIFT_WT")
#         assert len(result) == 0
#
#         result = cf.get_table_values("SAS_PS_SHIFT_DATA")
#         assert len(result) == 0
#
#
#     def test_populate_step_data(database_connection):
#         step_config = {"table_name": "[dbo].[SHIFT_DATA]",
#                        "data_table": "[dbo].[SAS_SHIFT_DATA]",
#                        "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
#                                               "[AM_PM_NIGHT]"],
#                        }
#         run_id = 'populate-step-data'
#         rec_id = get_rec_id("MAX", step_config['data_table'], database_connection) + 1
#
#         # setup test data/tables
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'populate_step_data.pkl')
#
#         # Reorder columns to match db and insert
#         test_data.columns = test_data.columns.str.upper()
#         test_data = test_data[
#             ['RUN_ID', 'YEAR', 'MONTH', 'DATA_SOURCE_ID', 'PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'TOTAL', 'AM_PM_NIGHT']]
#         cf.insert_dataframe_into_table(step_config["table_name"], test_data)
#
#         # Assign run_id, and input test data to function
#         idm.populate_step_data(run_id, database_connection, step_config)
#         sql = """
#         SELECT TOP (5) [REC_ID]
#           ,[PORTROUTE]
#           ,[WEEKDAY]
#           ,[ARRIVEDEPART]
#           ,[TOTAL]
#           ,[AM_PM_NIGHT]
#           ,[SHIFT_PORT_GRP_PV]
#           ,[AM_PM_NIGHT_PV]
#           ,[WEEKDAY_END_PV]
#         FROM [ips_test].[dbo].[SAS_SHIFT_DATA]
#         """
#         result = pd.read_sql_query(sql, database_connection)
#
#         # Amend rec_id within test result data
#         test_result = pd.read_pickle(TEST_DATA_DIR + 'populate_step_data_result.pkl')
#         test_result = amend_rec_id(test_result, rec_id)
#
#         # tear-down and cleanse
#         cf.delete_from_table(step_config['table_name'], 'RUN_ID', '=', run_id)
#
#         assert_frame_equal(result, test_result)
#
#
#     @pytest.mark.parametrize('step_name, expected_results_file, pv_columns, spv_table', [
#         ("SHIFT_WEIGHT", 'copy_pvs_shift_weight_result.pkl', ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"], "[dbo].[SAS_SHIFT_SPV]"),
#         ("UNSAMPLED_WEIGHT", 'copy_pvs_unsampled_weight_result.pkl', ["'UNSAMP_PORT_GRP_PV'", "'UNSAMP_REGION_GRP_PV'"], "[dbo].[SAS_UNSAMPLED_OOH_SPV]"),
#     ])
#     def test_copy_step_pvs_for_survey_data(step_name, expected_results_file, pv_columns,
#                                            spv_table, database_connection):
#         # This test is parameterised. The values for the arguments of this test function
#         # are taken from the parameters specified in pytest.mark.parametrize
#         # see https://docs.pytest.org/en/latest/parametrize.html
#         step_config = {'name': step_name,
#                        'spv_table': spv_table,
#                        'pv_columns': pv_columns}
#         run_id = 'copy-step-pvs'
#
#         # set up test data/tables
#         test_process_variables = pd.read_pickle(TEST_DATA_DIR + 'process_variables.pkl')
#         cf.insert_dataframe_into_table('PROCESS_VARIABLE_PY', test_process_variables, database_connection)
#
#         idm.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)
#
#         results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#
#         # clean test data before actually testing results
#         cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
#         cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
#
#         test_results = pd.read_pickle(TEST_DATA_DIR + expected_results_file)
#         results = results.sort_values(by='PROCVAR_NAME')
#         test_results = test_results.sort_values(by='PROCVAR_NAME')
#         results.index = range(0, len(results))
#         test_results.index = range(0, len(test_results))
#         assert_frame_equal(results, test_results)
#
#         results = cf.get_table_values(step_config['spv_table'])
#         assert len(results) == 0
#
#     def test_copy_step_pvs_for_survey_data(database_connection):
#         step_config = {'name': "SHIFT_WEIGHT",
#                        "spv_table": "[dbo].[SAS_SHIFT_SPV]",
#                        "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'", "'SHIFT_FLAG_PV'",
#                                       "'CROSSINGS_FLAG_PV'"]}
#         run_id = 'copy-step-pvs'
#
#         # set up test data/tables
#         test_process_variables = pd.read_pickle(TEST_DATA_DIR + 'process_variables.pkl')
#         cf.insert_dataframe_into_table('PROCESS_VARIABLE_PY', test_process_variables, database_connection)
#
#         idm.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)
#
#         results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#
#         # clean test data before actually testing results
#         cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
#         cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
#
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_pvs_shift_data_result.pkl')
#         # we need to massage the data frames a little to ensure outputs are the same
#         results = results.sort_values(by='PROCVAR_NAME')
#         test_results = test_results.sort_values(by='PROCVAR_NAME')
#         results.index = range(0, len(results))
#         test_results.index = range(0, len(test_results))
#
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         results = cf.get_table_values(step_config['spv_table'])
#         assert len(results) == 0
#
#
#     def test_update_survey_data_with_step_pv_output(database_connection):
#         step_config = {'name': "SHIFT_WEIGHT",
#                        'spv_table': '[dbo].[SAS_SHIFT_SPV]',
#                        "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'", "'SHIFT_FLAG_PV'", "'CROSSINGS_FLAG_PV'"]
#                        }
#         run_id = 'update-survey-pvs'
#
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         # set up test data/tables
#         test_survey_data = pd.read_pickle(TEST_DATA_DIR + 'update_survey_data_pvs.pkl')
#         cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, test_survey_data, database_connection)
#
# #<<<<<<< HEAD
#         test_nr_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_sw_pv_data.pkl')
#         cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)
#
#         idm.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)
# #=======
#         result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#         print("result: {}".format(result))
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')
#
#         # one record has a value beyond the RESPNSE range
#         # assert len(result) == (len(test_data)-1)
#         assert result.columns.isin(idm.COLUMNS_TO_MOVE).sum() == len(idm.COLUMNS_TO_MOVE)
# #>>>>>>> e5958e2ef7e850979435b67e27f927076db8eabd
#
#         results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         # clean test data before actually testing results
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'update_survey_data_pvs_result.pkl')
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         results = cf.get_table_values(step_config['spv_table'])
#         assert len(results) == 0
#
#         results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#         assert len(results) == 0
#
#
#     def test_copy_step_pvs_for_step_data(database_connection):
#         step_config = {'name': '[dbo].[SHIFT_DATA]'
#                        , 'pv_table': '[dbo].[SAS_SHIFT_PV]'
#                        , 'pv_columns': ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"]
#                        , 'order': 0
#                        }
#         run_id = 'copy-step-pvs-for-step-data'
#
#         # Pickle some test data
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data.pkl')
#         cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)
#
# #<<<<<<< HEAD
#         # Plug it in to copy_step_pvs_for_step_data(run_id, conn, step_configuration)
#         idm.copy_step_pvs_for_step_data(run_id, database_connection, step_config)
#         results = cf.get_table_values('SAS_PROCESS_VARIABLE')
#
#         # Assert step_configuration["pv_table"] has 0 records
#         result = cf.get_table_values(step_config['pv_table'])
#         assert len(result) == 0
#
#         # Cleanse tables before continuing
#         cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
#         cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
#
#         # Pickle some test results
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.pkl')
#
#     def test_populate_survey_data_for_step(database_connection):
#         # this is an integration of the above tests so we will keep things simple
#         # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE)
#
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample.pkl')
#         cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)
#
#         step_config = {"nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]",
#                                        "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
#                        'name': 'SHIFT_WEIGHT',
#                        'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"]}
#         idm.populate_survey_data_for_step('move-survey-test', database_connection, step_config)
#
#         # Assert that columns are null?
#
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')
#
#         result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#         result.to_csv(TEST_DATA_DIR + 'result.csv')
#         test_result = pd.read_pickle(TEST_DATA_DIR + 'move_survey_subsample_result.pkl')
#
#         assert_frame_equal(result, test_result, check_dtype=False)
#
#         # cleanse tables before testing output
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#     #>>>>>>> e5958e2ef7e850979435b67e27f927076db8eabd
#
#             # Assert equal
#             assert_frame_equal(results, test_results, check_dtype=False)
#
#
#     def test_update_step_data_with_step_pv_output(database_connection):
#         # step_config and variables
#         step_config = {"pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
#                        "pv_table": "[dbo].[SAS_SHIFT_PV]",
#                        "data_table": "[dbo].[SAS_SHIFT_DATA]",
#                        "weight_table": "[dbo].[SAS_SHIFT_WT]",
#                        "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]"}
#
#         # Set up test data/tables
#         test_shift_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_shift_pv_data.pkl')
#
#         # Get rec_id and amend test dataframe
#         rec_id = get_rec_id("MAX", step_config["data_table"], database_connection)
#         test_shift_pv_data = amend_rec_id(test_shift_pv_data, rec_id, ascend=False)
#
#         cf.insert_dataframe_into_table(step_config['pv_table'], test_shift_pv_data, database_connection)
#
#         idm.update_step_data_with_step_pv_output(database_connection, step_config)
#         sql = """
#         SELECT TOP(5)[REC_ID]
#           ,[PORTROUTE]
#           ,[WEEKDAY]
#           ,[ARRIVEDEPART]
#           ,[TOTAL]
#           ,[AM_PM_NIGHT]
#           ,[SHIFT_PORT_GRP_PV]
#           ,[AM_PM_NIGHT_PV]
#           ,[WEEKDAY_END_PV]
#         FROM [ips_test].[dbo].[SAS_SHIFT_DATA]
#         ORDER BY REC_ID DESC
#         """
#         results = pd.read_sql(sql, database_connection)
#
#         # Create expected test results and assert equal
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'update_shift_data_pvs_result.pkl')
#         test_results = amend_rec_id(test_results, rec_id, ascend=False)
#
#         print("results: {}".format(results))
#         print("test_results: {}".format(test_results))
#
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         # Assert temp tables had been cleanse in function
#         results = cf.get_table_values(step_config['pv_table'])
#         assert len(results) == 0
#
#         results = cf.get_table_values(step_config['weight_table'])
#         assert len(results) == 0
#
#         results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#         assert len(results) == 0
#
#         results = cf.get_table_values(step_config['sas_ps_table'])
#         assert len(results) == 0
#
#
#     @pytest.mark.xfail
#     def test_update_survey_data_with_step_results(database_connection):
#         # step_config and variables
#         step_config = {"name": "SHIFT_WEIGHT",
#                        "weight_table": "[dbo].[SAS_SHIFT_WT]",
#                        "results_columns": ["[SHIFT_WT]"]}
#
#         # set up test data/tables - export fake data into SAS_SURVEY_SUBSAMPLE
#         sas_survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'sas_survey_subsample_test_input.pkl')
#         cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection)
#
#         # and SAS_SHIFT_WT with matching SERIAL numbers
#         sas_shift_wt_input = pd.read_pickle(TEST_DATA_DIR + 'sas_shift_wt_test_input.pkl')
#         cf.insert_dataframe_into_table(step_config["weight_table"], sas_shift_wt_input, database_connection)
#
#         # Run that badger
#         idm.update_survey_data_with_step_results(database_connection, step_config)
#         results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         # Create expected test results and test against result
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'test_results_of_update_survey_data_with_step_results.pkl')
#
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         # Assert temp tables had been cleansed in function
#         result = cf.get_table_values(step_config['weight_table'])
#         assert len(result) == 0
#
#
#     @pytest.mark.xfail
#     def test_store_survey_data_with_step_results(database_connection):
#         # step_config and variables
#         step_config = {"name": "SHIFT_WEIGHT",
#                        "nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]",
#                                        "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
#                        "ps_table": "[dbo].[PS_SHIFT_DATA]"}
#         run_id = 'tst-store-survey-data-with-shift-wt-res'
#
#         # Set up records in SURVEY_SUBSAMPLE with above run_id
#         survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'survey_subsample_test_input.pkl')
#         cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, survey_subsample_input, database_connection)
#
#         # Set up records in SAS)SURVEY_SUBSAMPLE with same SERIAL as above
#         sas_survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'sas_survey_subsample_test_store_input.pkl')
#         cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection)
#
#         # run that badger
#         idm.store_survey_data_with_step_results(run_id, database_connection, step_config)
#
#         # Assert temp tables had been cleansed in function
#         sql = """
#         SELECT * FROM {}
#         WHERE RUN_ID = '{}'""".format(step_config['ps_table'], run_id)
#
#         cur = database_connection.cursor()
#         result = cur.execute(sql).fetchone()
#         assert result == None
#
#         result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#         assert len(result) == 0
#
#         # Retrieve results produced by function
#         sql = """
#         SELECT * FROM {}
#         WHERE SERIAL IN ('999999999991', '999999999992', '999999999993', '999999999994', '999999999995')
#         AND RUN_ID = '{}'
#         """.format(idm.SURVEY_SUBSAMPLE_TABLE, run_id)
#         results = pd.read_sql(sql, database_connection)
#
#         # Cleanse and delete from_survey_subsample where run_id = run_id
#         cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)
#
#         # Create expected test results and test against result
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'test_results_store_survey_data_with_step_results.pkl')
#
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#     def test_store_step_summary(database_connection):
#         # step_config and variables
#         step_config = {"ps_table": "[dbo].[PS_SHIFT_DATA]",
#                        "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
#                        "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
#                                       "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
#                                       "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"]}
#         run_id = 'store-shift-data-summary'
#
#         # set up test data/tables
#         test_ps_data = pd.read_pickle(TEST_DATA_DIR + 'store_ps_summary.pkl')
#         cf.insert_dataframe_into_table(step_config["sas_ps_table"], test_ps_data, database_connection)
#
#         # Run function return results
#         idm.store_step_summary(run_id, database_connection, step_config)
#         results = cf.get_table_values(step_config["ps_table"])
#
#         # Create expected test results and test against result
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'store_shift_data_summary_test_result.pkl')
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         # Assert temp tables had been cleansed in function
#         results = cf.get_table_values(step_config['sas_ps_table'])
#         assert len(results) == 0
#
#
#     @pytest.mark.skip('this takes very long')
#     def test_shift_weight_step(database_connection):
#
#         # import the necessary data into the database
#         # note that this has not been tested to work repeatedly
#         import_data_into_database()
#
#         step_config = {"nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]", "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
#                        'name': 'SHIFT_WEIGHT',
#                        'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"],
#                        "table_name": "[dbo].[SHIFT_DATA]",
#                        "data_table": "[dbo].[SAS_SHIFT_DATA]",
#                        "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
#                                               "[AM_PM_NIGHT]"],
#                        "spv_table": "[dbo].[SAS_SHIFT_SPV]",
#                        "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"],
#                        "pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
#                        "order": 0,
#                        "pv_table": "[dbo].[SAS_SHIFT_PV]",
#                        "weight_table": "[dbo].[SAS_SHIFT_WT]",
#                        "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
#                        "results_columns": ["[SHIFT_WT]"],
#                        "ps_table": "[dbo].[PS_SHIFT_DATA]",
#                        "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
#                                       "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
#                                       "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"],
#
#                        }
#
#         shift_weight_step('9e5c1872-3f8e-4ae5-85dc-c67a602d011e', database_connection, step_config)
#
#
#     def test_copy_step_pvs_for_survey_data(database_connection):
#         step_config = {'name': "SHIFT_WEIGHT",
#                        "spv_table": "[dbo].[SAS_SHIFT_SPV]",
#                        "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'", "'SHIFT_FLAG_PV'",
#                                       "'CROSSINGS_FLAG_PV'"]}
#         run_id = 'copy-step-pvs'
#
#         # set up test data/tables
#         test_process_variables = pd.read_pickle(TEST_DATA_DIR + 'process_variables.pkl')
#         cf.insert_dataframe_into_table('PROCESS_VARIABLE_PY', test_process_variables, database_connection)
#
#         idm.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)
#
#         results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#
#         # clean test data before actually testing results
#         cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
#         cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
#
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_pvs_shift_data_result.pkl')
#         # we need to massage the data frames a little to ensure outputs are the same
#         results = results.sort_values(by='PROCVAR_NAME')
#         test_results = test_results.sort_values(by='PROCVAR_NAME')
#         results.index = range(0, len(results))
#         test_results.index = range(0, len(test_results))
#
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         results = cf.get_table_values(step_config['spv_table'])
#         assert len(results) == 0
#
#
#     # Nassir
#     @pytest.mark.xfail
#     def test_update_survey_data_with_step_pv_output(database_connection):
#         step_config = {'name': "SHIFT_WEIGHT",
#                        'spv_table': '[dbo].[SAS_SHIFT_SPV]',
#                        "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'", "'SHIFT_FLAG_PV'", "'CROSSINGS_FLAG_PV'"]
#                        }
#         run_id = 'update-survey-pvs'
#
#         # delete the data in the table so that we have no data in table for test
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         # read and insert into the database the survey data
#         test_survey_data = pd.read_pickle(TEST_DATA_DIR + 'update_survey_data_pvs.pkl')
#         cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, test_survey_data, database_connection)
#
#         # read and insert into the database the pvs
#         test_nr_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_sw_pv_data.pkl')
#         cf.insert_dataframe_into_table(step_config['spv_table'], test_nr_pv_data, database_connection)
#
#         # is this calling the right function?
#         idm.update_survey_data_with_step_pv_output(database_connection, step_config)
#
#         # get the newly updated table data
#         results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         # clean test data before actually testing results
#         cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#
#         # test_results = pd.read_pickle(TEST_DATA_DIR + 'update_survey_data_pvs_result.pkl')
#         test_results = pd.read_csv(TEST_DATA_DIR + 'update_survey_data_pvs_result.csv')
#         assert_frame_equal(results, test_results, check_dtype=False)
#
#         # check that spv_table has been deleted
#         results_2 = cf.get_table_values(step_config['spv_table'])
#         assert len(results_2) == 0
#
#         # should not be required
#         #results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#         #assert len(results) == 0
#
#     # Nassir
#     def test_copy_step_pvs_for_step_data(database_connection):
#         step_config = {'name': '[dbo].[SHIFT_DATA]'
#                        , 'pv_table': '[dbo].[SAS_SHIFT_PV]'
#                        , 'pv_columns': ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"]
#                        , 'order': 0
#                        }
#         run_id = 'copy-step-pvs-for-step-data'
#
#         # Pickle some test data
#         test_data = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data.pkl')
#         cf.insert_dataframe_into_table("PROCESS_VARIABLE_PY", test_data, database_connection)
#
#         # Plug it in to copy_step_pvs_for_step_data(run_id, conn, step_configuration)
#         idm.copy_step_pvs_for_step_data(run_id, database_connection, step_config)
#         results = cf.get_table_values('SAS_PROCESS_VARIABLE')
#
#         # Assert step_configuration["pv_table"] has 0 records
#         result = cf.get_table_values(step_config['pv_table'])
#         assert len(result) == 0
#
#         # Cleanse tables before continuing
#         cf.delete_from_table(idm.SAS_PROCESS_VARIABLES_TABLE)
#         cf.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
#
#         # Pickle some test results
#         test_results = pd.read_pickle(TEST_DATA_DIR + 'copy_shift_weight_pvs_for_shift_data_results.pkl')
#
#         # Assert equal
#         assert_frame_equal(results, test_results, check_dtype=False)
#
# Nassir
# def test_update_step_data_with_step_pv_output(database_connection):
#     # step_config and variables
#     step_config = {"pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
#                    "pv_table": "[dbo].[SAS_SHIFT_PV]",
#                    "data_table": "[dbo].[SAS_SHIFT_DATA]",
#                    "weight_table": "[dbo].[SAS_SHIFT_WT]",
#                    "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]"}
#
#     # Set up test data/tables
#     test_shift_pv_data = pd.read_pickle(TEST_DATA_DIR + 'test_shift_pv_data.pkl')
#
#     # Get rec_id and amend test dataframe
#     rec_id = get_rec_id("MAX", step_config["data_table"], database_connection)
#     test_shift_pv_data = amend_rec_id(test_shift_pv_data, rec_id, ascend=False)
#
#     cf.insert_dataframe_into_table(step_config['pv_table'], test_shift_pv_data, database_connection)
#
#     idm.update_step_data_with_step_pv_output(database_connection, step_config)
#     sql = """
#     SELECT TOP(5)[REC_ID]
#       ,[PORTROUTE]
#       ,[WEEKDAY]
#       ,[ARRIVEDEPART]
#       ,[TOTAL]
#       ,[AM_PM_NIGHT]
#       ,[SHIFT_PORT_GRP_PV]
#       ,[AM_PM_NIGHT_PV]
#       ,[WEEKDAY_END_PV]
#     FROM [ips_test].[dbo].[SAS_SHIFT_DATA]
#     ORDER BY REC_ID DESC
#     """
#     results = pd.read_sql(sql, database_connection)
#
#     # Create expected test results and assert equal
#     test_results = pd.read_pickle(TEST_DATA_DIR + 'update_shift_data_pvs_result.pkl')
#     test_results = amend_rec_id(test_results, rec_id, ascend=False)
#
#     print("results: {}".format(results))
#     print("test_results: {}".format(test_results))
#
#     assert_frame_equal(results, test_results, check_dtype=False)
#
#     # Assert temp tables had been cleanse in function
#     results = cf.get_table_values(step_config['pv_table'])
#     assert len(results) == 0
#
#     results = cf.get_table_values(step_config['weight_table'])
#     assert len(results) == 0
#
#     results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)
#     assert len(results) == 0
#
#     results = cf.get_table_values(step_config['sas_ps_table'])
#     assert len(results) == 0
#
#
# def test_update_survey_data_with_step_results(database_connection):
#     # step_config and variables
#     step_config = {"name": "SHIFT_WEIGHT",
#                    "weight_table": "[dbo].[SAS_SHIFT_WT]",
#                    "results_columns": ["[SHIFT_WT]"]}
#
#     # set up test data/tables - export fake data into SAS_SURVEY_SUBSAMPLE
#     sas_survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'sas_survey_subsample_test_input.pkl')
#     cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection)
#
#     # and SAS_SHIFT_WT with matching SERIAL numbers
#     sas_shift_wt_input = pd.read_pickle(TEST_DATA_DIR + 'sas_shift_wt_test_input.pkl')
#     cf.insert_dataframe_into_table(step_config["weight_table"], sas_shift_wt_input, database_connection)
#
#     # Run that badger
#     idm.update_survey_data_with_step_results(database_connection, step_config)
#     results = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#     results.to_csv(TEST_DATA_DIR + 'results_of_update_survey_data_with_step_results.csv', index=False)
#
#     # Create expected test results and test against result
#     test_results = pd.read_pickle(TEST_DATA_DIR + 'test_results_of_update_survey_data_with_step_results.pkl')
#     results = pd.read_csv(TEST_DATA_DIR + 'results_of_update_survey_data_with_step_results.csv')
#
#     assert_frame_equal(results, test_results, check_dtype=False)
#
#     # Assert temp tables had been cleansed in function
#     result = cf.get_table_values(step_config['weight_table'])
#     assert len(result) == 0
#
#
# def test_store_survey_data_with_step_results(database_connection):
#     # step_config and variables
#     step_config = {"name": "SHIFT_WEIGHT",
#                    "nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]",
#                                    "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
#                    "ps_table": "[dbo].[PS_SHIFT_DATA]"}
#     run_id = 'tst-store-survey-data-with-shift-wt-res'
#
#     # Set up records in SURVEY_SUBSAMPLE with above run_id
#     survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'survey_subsample_test_input.pkl')
#     cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, survey_subsample_input, database_connection)
#
#     # Set up records in SAS)SURVEY_SUBSAMPLE with same SERIAL as above
#     sas_survey_subsample_input = pd.read_pickle(TEST_DATA_DIR + 'sas_survey_subsample_test_store_input.pkl')
#     cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, sas_survey_subsample_input, database_connection)
#
#     # run that badger
#     idm.store_survey_data_with_step_results(run_id, database_connection, step_config)
#
#     # Assert temp tables had been cleansed in function
#     sql = """
#     SELECT * FROM {}
#     WHERE RUN_ID = '{}'""".format(step_config['ps_table'], run_id)
#
#     cur = database_connection.cursor()
#     result = cur.execute(sql).fetchone()
#     assert result == None
#
#     result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
#     assert len(result) == 0
#
#     # Retrieve results produced by function
#     sql = """
#     SELECT * FROM {}
#     WHERE SERIAL IN ('999999999991', '999999999992', '999999999993', '999999999994', '999999999995')
#     AND RUN_ID = '{}'
#     """.format(idm.SURVEY_SUBSAMPLE_TABLE, run_id)
#     results = pd.read_sql(sql, database_connection)
#     results.to_csv(TEST_DATA_DIR + 'results_store_survey_data_with_step_results.csv', index=False)
#
#     # Cleanse and delete from_survey_subsample where run_id = run_id
#     cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)
#
#     # Create expected test results and test against result
#     test_results = pd.read_pickle(TEST_DATA_DIR + 'test_results_store_survey_data_with_step_results.pkl')
#     results = pd.read_csv(TEST_DATA_DIR + 'results_store_survey_data_with_step_results.csv')
#
#     assert_frame_equal(results, test_results, check_dtype=False)
#
#
# def test_store_step_summary(database_connection):
#     # step_config and variables
#     step_config = {"ps_table": "[dbo].[PS_SHIFT_DATA]",
#                    "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
#                    "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
#                                   "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
#                                   "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"]}
#     run_id = 'store-shift-data-summary'
#
#     # set up test data/tables
#     test_ps_data = pd.read_pickle(TEST_DATA_DIR + 'store_ps_summary.pkl')
#     cf.insert_dataframe_into_table(step_config["sas_ps_table"], test_ps_data, database_connection)
#
#     # Run function return results
#     idm.store_step_summary(run_id, database_connection, step_config)
#     results = cf.get_table_values(step_config["ps_table"])
#
#     # Create expected test results and test against result
#     test_results = pd.read_pickle(TEST_DATA_DIR + 'store_shift_data_summary_test_result.pkl')
#     assert_frame_equal(results, test_results, check_dtype=False)
#
#     # Assert temp tables had been cleansed in function
#     results = cf.get_table_values(step_config['sas_ps_table'])
#     assert len(results) == 0
#
#
# @pytest.mark.skip('this takes very long')
# def test_shift_weight_step(database_connection):
#
#     # import the necessary data into the database
#     # note that this has not been tested to work repeatedly
#     import_data_into_database()
#
#     step_config = {"nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]", "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
#                    'name': 'SHIFT_WEIGHT',
#                    'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"],
#                    "table_name": "[dbo].[SHIFT_DATA]",
#                    "data_table": "[dbo].[SAS_SHIFT_DATA]",
#                    "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
#                                           "[AM_PM_NIGHT]"],
#                    "spv_table": "[dbo].[SAS_SHIFT_SPV]",
#                    "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"],
#                    "pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
#                    "order": 0,
#                    "pv_table": "[dbo].[SAS_SHIFT_PV]",
#                    "weight_table": "[dbo].[SAS_SHIFT_WT]",
#                    "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
#                    "results_columns": ["[SHIFT_WT]"],
#                    "ps_table": "[dbo].[PS_SHIFT_DATA]",
#                    "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
#                                   "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
#                                   "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"],
#
#                    }
#
#     shift_weight_step('9e5c1872-3f8e-4ae5-85dc-c67a602d011e', database_connection, step_config)
# # e5958e2ef7e850979435b67e27f927076db8eabd
