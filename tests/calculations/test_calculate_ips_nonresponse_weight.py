'''
Created on 17th April 2018
Modified last: 14th Sep 2018

@author: James Burr / Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations import calculate_ips_nonresponse_weight as non_resp
import tests.config
import main.io.ips_data_management as idm
import main.io.CommonFunctions as cf

path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/non_response_weight"


def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)

def test_calculate():

    # define the table names
    SAS_NON_RESPONSE_DATA_TABLE_NAME = 'SAS_NON_RESPONSE_DATA' # the input data
    OUT_TABLE_NAME = "SAS_NON_RESPONSE_WT" # output data
    SUMMARY_OUT_TABLE_NAME = "SAS_PS_NON_RESPONSE" # output data

    # columns to sort the summary results by in order to check calculated dataframes match expected results
    NR_COLUMNS = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT',
       'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']

    # clear the input SQL server tables for the step
    cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    cf.delete_from_table(SAS_NON_RESPONSE_DATA_TABLE_NAME)

    # clear the output tables and summary tables
    cf.delete_from_table(OUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)

    # read in data from csv
    df_surveydata = pd.read_csv(path_to_data + '/surveydata.csv', engine='python')
    df_nr_data = pd.read_csv(path_to_data + '/nonresponsedata.csv', engine='python')

    # --------------------------------------
    # put dataframes into SQL server
    # --------------------------------------

    # If the data contains a REC_ID column, drop it as the value is generated once the record is added to the SQL table.
    if 'REC_ID' in df_nr_data.columns:
        df_nr_data.drop(['REC_ID'], axis=1, inplace=True)

    cf.insert_dataframe_into_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE, df_surveydata)
    cf.insert_dataframe_into_table(SAS_NON_RESPONSE_DATA_TABLE_NAME, df_nr_data)

    # Read the data from SQL (as it will in the production ready system)
    df_surveydata_import = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    df_nr_data_import = cf.get_table_values(SAS_NON_RESPONSE_DATA_TABLE_NAME)

    # run the calculation and test
    result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata_import, df_nr_data_import,
                                                          'NON_RESPONSE_WT', 'SERIAL')

    # Retrieve and sort python calculated dataframes
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    py_summary_data = result_py_data[1]
    py_summary_data.sort_values(by=NR_COLUMNS)
    py_summary_data.index = range(0, len(py_summary_data))

    # insert the csv output data into SQL and read back, this is for testing against data pulled from SQL Server
    test_result_survey = pd.read_csv(path_to_data + '/outputdata_final.csv', engine='python')
    test_result_survey_sql = convert_dataframe_to_sql_format(OUT_TABLE_NAME, test_result_survey)
    test_result_survey_sql = test_result_survey_sql.sort_values(by='SERIAL')
    test_result_survey_sql.index = range(0, len(test_result_survey_sql))

    test_result_summary = pd.read_csv(path_to_data + '/summarydata_final.csv', engine='python')
    test_result_summary_sql = convert_dataframe_to_sql_format(SUMMARY_OUT_TABLE_NAME, test_result_summary)
    test_result_summary_sql = test_result_summary_sql.sort_values(by=NR_COLUMNS)
    test_result_summary_sql.index = range(0, len(test_result_summary_sql))

    # clear the SQL server tables
    cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    cf.delete_from_table(SAS_NON_RESPONSE_DATA_TABLE_NAME)

    # clear the output tables and summary tables
    cf.delete_from_table(OUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey_sql, check_dtype=False, check_like=True, check_less_precise=True)
    assert_frame_equal(py_summary_data, test_result_summary_sql, check_dtype=False, check_like=True, check_less_precise=True)

    # Retrieve SAS Survey Data output and cleanse
    # test_result_survey = pd.read_csv(path_to_data + '/outputdata_final.csv', engine='python')
    # test_result_survey.columns = test_result_survey.columns.str.upper()
    # test_result_survey = test_result_survey.sort_values(by='SERIAL')
    # test_result_survey.index = range(0, len(test_result_survey))
    # test_result_survey.replace("", np.NaN, inplace=True)

    # Retrieve SAS Summary Data output and cleanse
    # test_result_summary = pd.read_csv(path_to_data + '/summarydata_final.csv', engine='python')
    # test_result_summary.columns = test_result_summary.columns.str.upper()
    # #test_result_summary = test_result_summary.sort_values(by=summary_columns)
    # test_result_summary.index = range(0, len(test_result_summary))


    # summary_columns = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT',
    #    'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']
    #49
    # # run the calculation tests from csv - this works
    # result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata, df_nr_data,
    #                                                       'NON_RESPONSE_WT', 'SERIAL')

    # # read pickle files and test - this works
    # test_survey = pd.read_pickle(path_to_data + '/surveydata.pkl')
    # test_nr_data = pd.read_pickle(path_to_data + '/nonresponsedata.pkl')
    # result_py_data = non_resp.do_ips_nrweight_calculation(test_survey, test_nr_data,
    #                                                       'NON_RESPONSE_WT', 'SERIAL')
    #
    # # run the calculation tests from csv - this works
    # result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata, df_nr_data,
    #                                                       'NON_RESPONSE_WT', 'SERIAL')



    # This is an integration test as it runs the entire step
    # test_survey = pd.read_pickle(path_to_data + '/non_response_survey_data.pkl')
    # test_nr_data = pd.read_pickle(path_to_data + '/non_response_data.pkl')
    # result_py_data = non_resp.do_ips_nrweight_calculation(test_survey, test_nr_data,
    #                                                       'NON_RESPONSE_WT', 'SERIAL')
    #
    # # Retrieve and clean Python survey data output
    # py_survey_data = result_py_data[0]
    # py_survey_data = py_survey_data.sort_values(by='SERIAL')
    # py_survey_data.index = range(0, len(py_survey_data))
    #
    # # From Python output data, retrieve Summary Data and cleanse
    # py_summary_data = result_py_data[1]
    # py_summary_data.index = range(0, len(py_summary_data))
    #
    # # Retrieve SAS Survey Data output and cleanse
    # test_result_survey = pd.read_pickle(path_to_data + '/non_response_weight_output.pkl')
    # test_result_survey.columns = test_result_survey.columns.str.upper()
    # test_result_survey = test_result_survey.sort_values(by='SERIAL')
    # test_result_survey.index = range(0, len(test_result_survey))
    # # test_result_survey.replace("", np.NaN, inplace=True)
    #
    # # Retrieve SAS Summary Data output and cleanse
    # test_result_summary = pd.read_pickle(path_to_data + '/non_response_weight_summary.pkl')
    # test_result_summary.columns = test_result_summary.columns.str.upper()
    # test_result_summary.index = range(0, len(test_result_summary))
    #
    # # Assert dfs are equal
    # assert_frame_equal(py_survey_data, test_result_survey, check_dtype=False, check_like=True)
    # assert_frame_equal(py_summary_data, test_result_summary, check_dtype=False, check_like=True)
