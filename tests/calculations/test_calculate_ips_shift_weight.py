import pytest
import utils.common_functions as cf
from main.calculations.calculate_ips_shift_weight import do_ips_shift_weight_calculation
import pandas as pd
from pandas.util.testing import assert_frame_equal


OUTPUT_TABLE_NAME = 'SAS_SHIFT_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_SHIFT_DATA'


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\shift_weight',
    r'tests\data\calculations\november_2017\shift_weight',
    r'tests\data\calculations\october_2017\shift_weight',
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 14 Sep 2018
    Purpose       : Tests the calculation function of the shift weight step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Clear the survey import table
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
    cf.delete_from_table('SAS_SHIFT_DATA')
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"\surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    path_to_shiftdata = data_path + r'\shiftsdata.csv'
    df_shiftsdata = pd.read_csv(path_to_shiftdata, engine='python')

    # Drop the REC_ID column as it is an identity column (will be automatically populated)
    df_shiftsdata = df_shiftsdata.drop(['REC_ID'], axis=1)

    cf.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', df_surveydata)
    cf.insert_dataframe_into_table('SAS_SHIFT_DATA', df_shiftsdata)

    # Read the data from SQL (as it will in the production ready system)
    df_surveydata = cf.get_table_values('SAS_SURVEY_SUBSAMPLE')
    df_shiftsdata = cf.get_table_values('SAS_SHIFT_DATA')

    # Run the calculation step
    output_data, summary_data = do_ips_shift_weight_calculation(df_surveydata,
                                                                df_shiftsdata,
                                                                var_serialNum='SERIAL',
                                                                var_shiftWeight='SHIFT_WT')

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
    path_to_survey_result = data_path + r"\outputdata_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_survey_expected = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, df_survey_expected)

    # Sort the dataframes for comparison
    df_survey_result = df_survey_result.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))
    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(df_survey_result, df_survey_expected)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\summarydata_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_summary_expected = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, df_summary_expected)

    # Sort the dataframes for comparison
    df_summary_result = df_summary_result.sort_values(['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV'])
    df_summary_result.index = range(0, len(df_summary_result))
    df_summary_expected = df_summary_expected.sort_values(['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV'])
    df_summary_expected.index = range(0, len(df_summary_expected))

    assert_frame_equal(df_summary_result, df_summary_expected)











#
# import math
# import numpy as np
# import pandas as pd
# import pytest
# from pandas.util.testing import assert_frame_equal
#
# from main.calculations.calculate_ips_shift_weight import calculate_factor, calculate, \
#                                                          do_ips_shift_weight_calculation, calculate_ips_crossing_factor, \
#                                                          calculate_ips_shift_factor
# import tests.config
#
# path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/shift_weight"
#
#
# @pytest.mark.shiftweight
# class TestCalculateFactor(object):
#
#     def test_calculate_factor_with_non_zero_numerator_and_denominator(self):
#         d = {'NUMERATOR': [1, 2, 3, 4, 5], 'DENOMINATOR': [9, 2, -1, 4, 5], 'flag': [1, 1, 2, 1, 1]}
#         df = pd.DataFrame(data=d)
#         df_new = df.apply(calculate_factor, axis=1, args=('flag',))
#
#         # check all values match manual calculation
#         df_man = df['NUMERATOR'] / df['DENOMINATOR']
#         assert (all(np.where(df_new == df_man, True, False)))
#
#     def test_calculate_factor_for_zero_division(self):
#         d = {'NUMERATOR': [1, 2, 3, 4, 0], 'DENOMINATOR': [0, 4, 0, -4, 0], 'flag': [1, 1, 2, 1, 1]}
#         df = pd.DataFrame(data=d)
#         df_new = df.apply(calculate_factor, axis=1, args=('flag',))
#
#         assert(df_new[0] == float("inf"))
#         assert (df_new[1] == 0.5)
#         assert (df_new[2] == float("inf"))
#         assert (df_new[3] == -1.0)
#
#         # check for 0/0
#         assert (math.isnan(df_new[4]))
#
#     def test_calculate_factor_with_zero_flag_parameter(self):
#         d = {'NUMERATOR': [1, 2, 3, 4, 5], 'DENOMINATOR': [1, 2, 3, 4, 5], 'flag': [1, 0, 0, 1, 1]}
#         df = pd.DataFrame(data=d)
#         df_new = df.apply(calculate_factor, axis=1, args=('flag',))
#         assert (math.isnan(df_new[1]))
#         assert (math.isnan(df_new[2]))
#
#     def test_calculate_factor_with_all_zero_flag_parameter(self):
#         d = {'NUMERATOR': [1, 2, 3, 4, 5], 'DENOMINATOR': [1, 2, 3, 4, 5], 'flag': [0, 0, 0, 0, 0]}
#         df = pd.DataFrame(data=d)
#         df_new = df.apply(calculate_factor, axis=1, args=('flag',))
#         assert (math.isnan(df_new[0]))
#         assert (math.isnan(df_new[1]))
#         assert (math.isnan(df_new[2]))
#         assert (math.isnan(df_new[3]))
#         assert (math.isnan(df_new[4]))
#
# @pytest.mark.shiftweight
# def test_calculate():
#
#     (df_surveydata, df_summary) = calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE'
#                                              , ShiftsData = 'SAS_SHIFT_DATA',
#                                              var_serialNum = 'SERIAL',
#                                              var_shiftWeight = 'SHIFT_WT')
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\out_2.pkl")
#     df_test.columns = df_test.columns.str.upper()
#
#     # sort by 'SERIAL only for comparison to SAS
#     df_test_2 = df_test.sort_values(['SERIAL'])
#     df_test_2.index = range(df_test_2.shape[0])
#     assert_frame_equal(df_surveydata, df_test_2, check_dtype=False)
#     # test code end
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\summary_3.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     assert_frame_equal(df_summary, df_test, check_like=True)
#     # test code end
#
#
# @pytest.mark.shiftweight
# def test_do_ips_shift_weight_calculation():
#
#     df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
#     df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")
#
#     # uppercase all columns
#     df_surveydata.columns = df_surveydata.columns.str.upper()
#     df_shiftsdata.columns = df_shiftsdata.columns.str.upper()
#
#     df_surveydata_out, df_summary_out = do_ips_shift_weight_calculation(df_surveydata,
#                                                                         df_shiftsdata,
#                                                                         var_serialNum='SERIAL',
#                                                                         var_shiftWeight='SHIFT_WT')
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\out_2.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     df_test_2 = df_test.sort_values(['SERIAL'])
#     df_test_2.index = range(df_test_2.shape[0])
#     assert_frame_equal(df_surveydata_out, df_test_2, check_dtype=False)
#     # test code end
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\summary_3.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     assert_frame_equal(df_summary_out, df_test, check_like=True)
#     # test code end
#
# @pytest.mark.shiftweight
# def test_calculate_ips_shift_factor():
#
#     df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
#     df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")
#
#     # uppercase all columns
#     df_surveydata.columns = df_surveydata.columns.str.upper()
#     df_shiftsdata.columns = df_shiftsdata.columns.str.upper()
#
#     (df_totalsampledshifts, df_possibleshifts, df_surveydata_merge) = calculate_ips_shift_factor(df_shiftsdata,
#                                                                                         df_surveydata)
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\totalSampledShifts.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     df_test = df_test.drop(['_TYPE_', '_FREQ_'], axis=1)
#     assert_frame_equal(df_totalsampledshifts, df_test, check_dtype=False)
#     # test code end
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\possibleShifts.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     df_test = df_test.drop(['_TYPE_', '_FREQ_'], axis=1)
#     assert_frame_equal(df_possibleshifts, df_test, check_dtype=False)
#     # test code end
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\outputdata_sf.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     # df_test = df_test.drop(['_TYPE_', '_FREQ_'], axis=1)
#     assert_frame_equal(df_surveydata_merge, df_test, check_dtype=False)
#     # test code end
#
# @pytest.mark.shiftweight
# def test_calculate_ips_crossing_factor():
#
#     df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
#     df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")
#
#     # uppercase all columns
#     df_surveydata.columns = df_surveydata.columns.str.upper()
#     df_shiftsdata.columns = df_shiftsdata.columns.str.upper()
#
#     df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
#     df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")
#
#     # uppercase all columns
#     df_surveydata.columns = df_surveydata.columns.str.upper()
#     df_shiftsdata.columns = df_shiftsdata.columns.str.upper()
#
#     # get the survey data input from calculate_ips_shift_factor()
#     (_, _, df_surveydata_for_crossings) = calculate_ips_shift_factor(df_shiftsdata,
#                                                                      df_surveydata,
#                                                                      )
#
#     (df_totalSampledCrossings, df_surveydata_merge) = calculate_ips_crossing_factor(df_shiftsdata,
#                                                                                 df_surveydata_for_crossings)
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\totalSampledCrossings.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     assert_frame_equal(df_totalSampledCrossings, df_test.drop(['_TYPE_', '_FREQ_'], axis=1), check_dtype=False)
#     # test code end
#
#     # test code start
#     df_test = pd.read_pickle(path_to_data + r"\outputdata_cf.pkl")
#     df_test.columns = df_test.columns.str.upper()
#     assert_frame_equal(df_surveydata_merge, df_test)
#     # test code end
#
#
#
#
