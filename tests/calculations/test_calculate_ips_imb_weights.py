'''
Created on 6 Mar 2018

@author: thorne1
'''
from main.calculations import calculate_ips_imb_weight as imb
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import main.io.CommonFunctions as cf

OUTPUT_TABLE_NAME = 'SAS_IMBALANCE_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_IMBALANCE'


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\imbalance_weight',
    r'tests\data\calculations\november_2017\imbalance_weight',
    r'tests\data\calculations\october_2017\imbalance_weight',
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 13 Sep 2018
    Purpose       : Tests the calculation function of the town stay and expenditure step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Clear the survey import table
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"\surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')
    cf.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', df_surveydata)

    # Read the data from SQL (as it will in the production ready system)
    df_surveydata = cf.get_table_values('SAS_SURVEY_SUBSAMPLE')

    # Run the calculation step
    output_data, summary_data = imb.do_ips_imbweight_calculation(df_surveydata
                                                                 , var_serialNum="SERIAL"
                                                                 , var_shiftWeight="SHIFT_WT"
                                                                 , var_NRWeight="NON_RESPONSE_WT"
                                                                 , var_minWeight="MINS_WT"
                                                                 , var_trafficWeight="TRAFFIC_WT"
                                                                 , var_OOHWeight="UNSAMP_TRAFFIC_WT"
                                                                 , var_imbalanceWeight="IMBAL_WT")

    def convert_dataframe_to_sql_format(table_name, dataframe):
        cf.insert_dataframe_into_table(table_name, dataframe)
        return cf.get_table_values(table_name)

    # Write the test result data to SQL then pull it back for comparison
    df_survey_result = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, output_data)
    df_summary_result = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, summary_data)

    # Clear down the result tables
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_survey_expected = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, df_survey_expected)

    # Sort the dataframes for comparison
    df_survey_result = df_survey_result.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))
    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(df_survey_result, df_survey_expected)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\summary_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_summary_expected = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, df_summary_expected)

    # Remove NULL flow calculated value (This only exists in imbalance weight - 13/09/2018)
    df_summary_expected = df_summary_expected.loc[df_summary_expected['FLOW'].notnull()]

    # Sort the dataframes for comparison
    df_summary_result = df_summary_result.sort_values(['FLOW'])
    df_summary_result.index = range(0, len(df_summary_result))
    df_summary_expected = df_summary_expected.sort_values(['FLOW'])
    df_summary_expected.index = range(0, len(df_summary_expected))

    assert_frame_equal(df_summary_result, df_summary_expected)

#

# def test_calculate():
#     # This is an integration test as it runs the entire step
#     test_survey = pd.read_pickle(path_to_data + '/imb_weight_input.pkl')
#     # Retrieve Python output data
#     result_py_data = imb.do_ips_imbweight_calculation(test_survey
#                                                       , var_serialNum="SERIAL"
#                                                       , var_shiftWeight="SHIFT_WT"
#                                                       , var_NRWeight="NON_RESPONSE_WT"
#                                                       , var_minWeight="MINS_WT"
#                                                       , var_trafficWeight="TRAFFIC_WT"
#                                                       , var_OOHWeight="UNSAMP_TRAFFIC_WT"
#                                                       , var_imbalanceWeight="IMBAL_WT")
#
#     # From Python output data, retrieve Survey Data and cleanse
#     py_survey_data = result_py_data[0]
#     py_survey_data = py_survey_data.sort_values(by='SERIAL')
#     py_survey_data.index = range(0, len(py_survey_data))
#
#     # From Python output data, retrieve Summary Data and cleanse
#     py_summary_data = result_py_data[1]
#     py_summary_data.index = range(0, len(py_summary_data))
#
#     # Retrieve SAS Survey Data output and cleanse
#     test_result_survey = pd.read_pickle(path_to_data + '/imb_weight_surveydata_output.pkl')
#     test_result_survey.columns = test_result_survey.columns.str.upper()
#     test_result_survey = test_result_survey.sort_values(by='SERIAL')
#     test_result_survey.index = range(0, len(test_result_survey))
#
#     # Retrieve SAS Summary Data output and cleanse
#     test_result_summary = pd.read_pickle(path_to_data + '/imb_weight_summarydata_output.pkl')
#     test_result_summary.columns = test_result_summary.columns.str.upper()
#     test_result_summary.index = range(0, len(test_result_summary))
#
#     # Assert dfs are equal
#     assert_frame_equal(py_survey_data, test_result_survey, check_dtype=False)
#     assert_frame_equal(py_summary_data, test_result_summary, check_dtype=False)
