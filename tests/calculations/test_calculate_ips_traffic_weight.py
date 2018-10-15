'''
Created on 12 March 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_traffic_weight import do_ips_trafweight_calculation_with_R
import main.io.CommonFunctions as cf
import pytest
import main.io.ips_data_management as idm

POP_TOTALS = "SAS_TRAFFIC_DATA"
SAS_SURVEY_SUBSAMPLE_TABLE = idm.SAS_SURVEY_SUBSAMPLE_TABLE

OUT_TABLE_NAME = "SAS_TRAFFIC_WT"  # output data
SUMMARY_OUT_TABLE_NAME = "SAS_PS_TRAFFIC"  # output data

SURVEY_TRAFFIC_AUX_TABLE = "[dbo].[survey_traffic_aux]"
POP_PROWVEC_TABLE = "[dbo].[poprowvec_traffic]"

R_TRAFFIC_TABLE = "[dbo].[r_traffic]"

def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)

def clear_tables():

    # clear the input SQL server tables for the step
    cf.delete_from_table(SAS_SURVEY_SUBSAMPLE_TABLE)
    cf.delete_from_table(POP_TOTALS)

    # clear the output tables and summary tables
    cf.delete_from_table(OUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_OUT_TABLE_NAME)

    # clear the auxillary tables
    cf.delete_from_table(SURVEY_INPUT_AUX_TABLE)

    # drop aux tables and r created tables
    cf.drop_table(POP_PROWVEC_TABLE)
    cf.drop_table(R_TRAFFIC_TABLE)


@pytest.mark.parametrize('path_to_data', [
    r'tests\data\calculations\december_2017\traffic_weight',
    #r'tests\data\calculations\november_2017\traffic_weight',
    #r'tests\data\calculations\october_2017\traffic_weight', # ignored as summary data test unavailable
    ])
def test_calculate(path_to_data):

    # clear the input and output tables
    clear_tables()

    # read in data from csv
    df_surveydata = pd.read_csv(path_to_data + '/surveydata.csv', engine='python')
    df_tr_data = pd.read_csv(path_to_data + '/trafficdata.csv', engine='python')

    # --------------------------------------
    # put dataframes into SQL server
    # --------------------------------------

    # If the data contains a REC_ID column, drop it as the value is generated once the record is added to the SQL table.
    if 'REC_ID' in df_tr_data.columns:
        df_tr_data.drop(['REC_ID'], axis=1, inplace=True)

    df_surveydata_import = convert_dataframe_to_sql_format(SAS_SURVEY_SUBSAMPLE_TABLE, df_surveydata)
    df_tr_data_import = convert_dataframe_to_sql_format(POP_TOTALS, df_tr_data)

    # do the calculation
    result_py_data = do_ips_trafweight_calculation_with_R(df_surveydata_import, df_tr_data_import)

    # Retrieve and sort python calculated dataframes
    # py_survey_data = result_py_data[0]
    # py_survey_data = py_survey_data.sort_values(by='SERIAL')
    # py_survey_data.index = range(0, len(py_survey_data))
    #
    # py_summary_data = result_py_data[1]
    # py_summary_data.sort_values(by=NR_COLUMNS)
    # py_summary_data.index = range(0, len(py_summary_data))
    #
    # # insert the csv output data into SQL and read back, this is for testing against data pulled from SQL Server
    # test_result_survey = pd.read_csv(path_to_data + '/outputdata_final.csv', engine='python')
    # test_result_survey_sql = convert_dataframe_to_sql_format(OUT_TABLE_NAME, test_result_survey)
    # test_result_survey_sql = test_result_survey_sql.sort_values(by='SERIAL')
    # test_result_survey_sql.index = range(0, len(test_result_survey_sql))
    #
    # test_result_summary = pd.read_csv(path_to_data + '/summarydata_final.csv', engine='python')
    # test_result_summary_sql = convert_dataframe_to_sql_format(SUMMARY_OUT_TABLE_NAME, test_result_summary)
    # test_result_summary_sql = test_result_summary_sql.sort_values(by=NR_COLUMNS)
    # test_result_summary_sql.index = range(0, len(test_result_summary_sql))
    #
    # # clear the input and output tables
    # clear_tables()
    #
    # # Assert dfs are equal
    # assert_frame_equal(py_survey_data, test_result_survey_sql, check_dtype=False, check_like=True,
    #                    check_less_precise=True)
    # assert_frame_equal(py_summary_data, test_result_summary_sql, check_dtype=False, check_like=True,
    #                    check_less_precise=True)


    # (df_output_merge_final_rounded, df_summary_merge_sum_traftot) = calculate(SurveyData='sas_survey_subsample'
    #                                                                           , var_serialNum='serial'.upper()
    #                                                                           , var_shiftWeight='shift_wt'.upper()
    #                                                                           , var_NRWeight='non_response_wt'.upper()
    #                                                                           , var_minWeight='mins_wt'.upper()
    #                                                                           , PopTotals='sas_traffic_data'
    #                                                                           , GWeightVar='traffic_wt'.upper()
    #                                                                           , minCountThresh=30)
    #
    # path_to_test = path_to_data + r"/output_rounded.pkl"
    # test_df_output_merge_final_rounded = pd.read_pickle(path_to_test)
    # test_df_output_merge_final_rounded.columns = test_df_output_merge_final_rounded.columns.str.upper()
    # assert_frame_equal(df_output_merge_final_rounded, test_df_output_merge_final_rounded)
    #
    # path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    # test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    # test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    # assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot, check_dtype=False,check_column_type=False)


def test_do_ips_trafweight_calculation():
    path_to_test = path_to_data + r"/survey_input.pkl"
    df_survey = pd.read_pickle(path_to_test)
    df_survey.columns = df_survey.columns.str.upper()

    path_to_test = path_to_data + r"/trtotals.pkl"
    df_trtotals = pd.read_pickle(path_to_test)
    df_trtotals.columns = df_trtotals.columns.str.upper()

    (df_output_merge_final_rounded, df_summary_merge_sum_traftot) = do_ips_trafweight_calculation(
        df_survey
        , var_serialNum='serial'.upper()
        , var_shiftWeight='shift_wt'.upper()
        , var_NRWeight='non_response_wt'.upper()
        , var_minWeight='mins_wt'.upper()
        , PopTotals=df_trtotals
        , GWeightVar='traffic_wt'.upper()
        , minCountThresh=30)

    path_to_test = path_to_data + r"/output_rounded.pkl"
    test_df_output_merge_final_rounded = pd.read_pickle(path_to_test)
    test_df_output_merge_final_rounded.columns = test_df_output_merge_final_rounded.columns.str.upper()
    assert_frame_equal(df_output_merge_final_rounded, test_df_output_merge_final_rounded)

    path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot)


def test_do_ips_ges_weighting():

    path_to_test = path_to_data + r"/in_1.pkl"
    df_survey = pd.read_pickle(path_to_test)
    df_survey.columns = df_survey.columns.str.upper()

    (df_output_merge_final, df_survey_serialNum_sort) = do_ips_ges_weighting(df_survey
                                                                             , var_serialNum='serial'.upper()
                                                                             , df_popTotals="assign"
                                                                             , GWeightVar='traffic_wt'.upper()
                                                                             , CalWeight="assign")

    path_to_test = path_to_data + r"/output_merge_final.pkl"
    test_df_output_merge_final = pd.read_pickle(path_to_test)
    test_df_output_merge_final.columns = test_df_output_merge_final.columns.str.upper()
    assert_frame_equal(df_output_merge_final, test_df_output_merge_final)

    path_to_test = path_to_data + r"/survey_serialNum_sort.pkl"
    test_df_survey_serialNum_sort = pd.read_pickle(path_to_test)
    test_df_survey_serialNum_sort.columns = test_df_survey_serialNum_sort.columns.str.upper()
    assert_frame_equal(df_survey_serialNum_sort, test_df_survey_serialNum_sort)


def test_generate_ips_tw_summary():

    path_to_test = path_to_data + r"/in_1.pkl"
    df_survey = pd.read_pickle(path_to_test)
    df_survey.columns = df_survey.columns.str.upper()

    path_to_test = path_to_data + r"/output_merge_final.pkl"
    df_output_merge_final = pd.read_pickle(path_to_test)
    df_output_merge_final.columns = df_output_merge_final.columns.str.upper()

    path_to_test = path_to_data + r"/poptotals_summary_1.pkl"
    df_poptotals_summary_1 = pd.read_pickle(path_to_test)
    df_poptotals_summary_1.columns = df_poptotals_summary_1.columns.str.upper()

    df_summary_merge_sum_traftot = generate_ips_tw_summary(df_survey
                                                           , df_output_merge_final
                                                           , var_serialNum='serial'.upper()
                                                           , var_trafficWeight='traffic_wt'.upper()
                                                           , df_popTotals=df_poptotals_summary_1
                                                           , minCountThresh=30)

    path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot, check_column_type=False)