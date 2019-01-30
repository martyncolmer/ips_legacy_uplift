'''
Created on 17th April 2018
Modified last: 14th Sep 2018

@author: James Burr / Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations import calculate_ips_nonresponse_weight as non_resp
import main.io.data_management as idm
import utils.common_functions as cf
import pytest

# define the table names
SAS_NON_RESPONSE_DATA_TABLE_NAME = 'SAS_NON_RESPONSE_DATA'  # the input data
OUT_TABLE_NAME = "SAS_NON_RESPONSE_WT"  # output data
SUMMARY_OUT_TABLE_NAME = "SAS_PS_NON_RESPONSE"  # output data

# columns to sort the summary results by in order to check calculated dataframes match expected results
NR_COLUMNS = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT',
              'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']

def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)

def clear_tables():

    # clear the input SQL server tables for the step
    cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    cf.delete_from_table(SAS_NON_RESPONSE_DATA_TABLE_NAME)

    # clear the output tables and summary tables
    cf.delete_from_table(OUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)

@pytest.mark.parametrize('path_to_data', [
    r'tests\data\calculations\december_2017\non_response_weight',
    r'tests\data\calculations\november_2017\non_response_weight',
    #r'tests\data\calculations\october_2017\non_response_weight', # ignored as summary data test unavailable
    ])
def test_calculate(path_to_data):

    # clear the input and output tables
    clear_tables()

    # read in data from csv
    df_surveydata = pd.read_csv(path_to_data + '/surveydata.csv', engine='python')
    df_nr_data = pd.read_csv(path_to_data + '/nonresponsedata.csv', engine='python')

    # --------------------------------------
    # put dataframes into SQL server
    # --------------------------------------

    # If the data contains a REC_ID column, drop it as the value is generated once the record is added to the SQL table.
    if 'REC_ID' in df_nr_data.columns:
        df_nr_data.drop(['REC_ID'], axis=1, inplace=True)

    df_surveydata_import = convert_dataframe_to_sql_format(idm.SAS_SURVEY_SUBSAMPLE_TABLE, df_surveydata)
    df_nr_data_import = convert_dataframe_to_sql_format(SAS_NON_RESPONSE_DATA_TABLE_NAME, df_nr_data)

    # --------------------------------------
    # run the calculation and test
    # --------------------------------------
    result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata_import, df_nr_data_import,
                                                          'NON_RESPONSE_WT', 'SERIAL')

    # Retrieve and sort python calculated dataframes
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    py_survey_data.to_csv(r'S:\CASPA\IPS\Testing\scratch\integration docs\non_response_outputdata_final_py.csv')

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

    # clear the input and output tables
    clear_tables()

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey_sql, check_dtype=False, check_like=True, check_less_precise=True)
    assert_frame_equal(py_summary_data, test_result_summary_sql, check_dtype=False, check_like=True, check_less_precise=True)