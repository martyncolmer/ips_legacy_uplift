# '''
# Created on April 2018
#
# @author: Nassir Mohammad
# '''
#
# import pandas as pd
# from pandas.util.testing import assert_frame_equal
# from main.calculations.calculate_ips_unsampled_weight import calculate, do_ips_unsampled_weight_calculation
# import pytest
# import tests.config
#
# path_to_data = "tests/data/calculations/" + tests.config.TEST_MONTH + "/unsampled_weight"
#
#
# @pytest.mark.unsampled
# def test_calculate():
#     print("Started testing IPS unsampled weight - calculate()")
#     (output_dataframe, summary_dataframe) = calculate(SurveyData='SAS_SURVEY_SUBSAMPLE',
#                                                       var_serialNum='SERIAL',
#                                                       var_shiftWeight='SHIFT_WT',
#                                                       var_NRWeight='NON_RESPONSE_WT',
#                                                       var_minWeight='MINS_WT',
#                                                       var_trafficWeight='TRAFFIC_WT',
#                                                       var_OOHWeight="UNSAMP_TRAFFIC_WT",
#                                                       minCountThresh=30)
#
#     test_df = pd.read_pickle(path_to_data + r"/output_final.pkl")
#     test_df.columns = test_df.columns.str.upper()
#     assert_frame_equal(output_dataframe, test_df)
#
#     test_summary = pd.read_pickle(path_to_data + r"/summary_final.pkl")
#     test_summary.columns = test_summary.columns.str.upper()
#     assert_frame_equal(summary_dataframe, test_summary, check_like=True, check_dtype=False)
#
# @pytest.mark.unsampled
# def test_do_ips_unsampled_weight_calculation():
#
#     df_surveydata = pd.read_pickle(path_to_data + r"/survey_input.pkl")
#     df_ustotals = pd.read_pickle(path_to_data + r"/ustotals.pkl")
#
#     df_surveydata.columns = df_surveydata.columns.str.upper()
#     df_ustotals.columns = df_ustotals.columns.str.upper()
#
#     output_dataframe, summary_dataframe = do_ips_unsampled_weight_calculation(df_surveydata,
#                                                                               var_serialNum='SERIAL',
#                                                                               var_shiftWeight='SHIFT_WT',
#                                                                               var_NRWeight='NON_RESPONSE_WT',
#                                                                               var_minWeight='MINS_WT',
#                                                                               var_trafficWeight='TRAFFIC_WT',
#                                                                               var_OOHWeight="UNSAMP_TRAFFIC_WT",
#                                                                               df_ustotals=df_ustotals,
#                                                                               minCountThresh=30)
#
#     test_df = pd.read_pickle(path_to_data + r"/output_final.pkl")
#     test_df.columns = test_df.columns.str.upper()
#     assert_frame_equal(output_dataframe, test_df)
#
#     test_summary = pd.read_pickle(path_to_data + r"/summary_final.pkl")
#     test_summary.columns = test_summary.columns.str.upper()
#     assert_frame_equal(summary_dataframe, test_summary, check_like=True, check_dtype=False)

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_unsampled_weight import do_ips_unsampled_weight_calculation
import main.io.CommonFunctions as cf
import pytest
import tests.config

path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/unsampled_weight"

OUTPUT_TABLE_NAME = 'SAS_UNSAMPLED_OOH_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_UNSAMPLED_OOH'


def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)


def sort_and_set_index(df, sort_columns):
    df = df.sort_values(sort_columns)
    df.index = range(0, len(df))
    return df


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\unsampled_weight',
    r'tests\data\calculations\november_2017\unsampled_weight',
    r'tests\data\calculations\october_2017\unsampled_weight',
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 11 Sep 2018
    Purpose       : Tests the calculation function of the minimums weight step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Clear the survey import table
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
    cf.delete_from_table('SAS_UNSAMPLED_OOH_DATA')
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"\surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    path_to_unsampleddata = data_path + r"\unsampleddata.csv"
    df_ustotals = pd.read_csv(path_to_unsampleddata, engine='python')

    # Drop the REC_ID column as it is an identity column (will be automatically populated)
    df_ustotals = df_ustotals.drop(['REC_ID'], axis=1)

    # Read the data from SQL (as it will in the production ready system)
    df_surveydata = convert_dataframe_to_sql_format('SAS_SURVEY_SUBSAMPLE', df_surveydata)
    df_ustotals = convert_dataframe_to_sql_format('SAS_UNSAMPLED_OOH_DATA', df_ustotals)

    # Run the calculation step
    output_data, summary_data = do_ips_unsampled_weight_calculation(df_surveydata,
                                                                    var_serialNum='SERIAL',
                                                                    var_shiftWeight='SHIFT_WT',
                                                                    var_NRWeight='NON_RESPONSE_WT',
                                                                    var_minWeight='MINS_WT',
                                                                    var_trafficWeight='TRAFFIC_WT',
                                                                    var_OOHWeight="UNSAMP_TRAFFIC_WT",
                                                                    df_ustotals=df_ustotals,
                                                                    minCountThresh=30)

    # Write the test result data to SQL then pull it back for comparison
    df_survey_result = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, output_data)
    df_summary_result = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, summary_data)

    # Clear down the result tables
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\outputdata_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_survey_expected = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, df_survey_expected)

    # Sort the dataframes for comparison
    df_survey_result = sort_and_set_index(df_survey_result, 'SERIAL')
    df_survey_expected = sort_and_set_index(df_survey_expected, 'SERIAL')

    assert_frame_equal(df_survey_result, df_survey_expected)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\summarydata_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_summary_expected = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, df_summary_expected)

    # Sort the dataframes for comparison
    df_summary_result = sort_and_set_index(df_summary_result, ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART'])
    df_summary_expected = sort_and_set_index(df_summary_expected, ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART'])

    assert_frame_equal(df_summary_result, df_summary_expected)

