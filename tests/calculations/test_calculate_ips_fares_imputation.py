# '''
# Created on 15 Mar 2018
#
# @author: burrj
# '''
# import pandas as pd
# from pandas.util.testing import assert_frame_equal
# from main.calculations.calculate_ips_fares_imputation import do_ips_fares_imputation
#
# import pytest
# import tests.config
#
# path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH +  "/fares"
#
#
# @pytest.mark.skip("Known failure due to rounding")
# def test_calculate():
#     test_survey = pd.read_pickle(path_to_data + '/fares_imp_input.pkl')
#
#     result_data = do_ips_fares_imputation(test_survey, var_serial='SERIAL',
#                                           num_levels=9, measure='mean')
#
#     test_result_summary = pd.read_pickle(path_to_data + '/fares_imp_output.pkl')
#     test_result_summary.columns = test_result_summary.columns.str.upper()
#
#     test_result_summary = test_result_summary.sort_values(by='SERIAL')
#     test_result_summary.index = range(0, len(test_result_summary))
#     result_data = result_data.sort_values(by='SERIAL')
#     result_data.index = range(0, len(result_data))
#
#     # The dataframes have different column orders; check_like is True which will
#     # ignore that fact when checking if the data is the same.
#     assert_frame_equal(result_data, test_result_summary, check_like=True)


from main.calculations.calculate_ips_fares_imputation import do_ips_fares_imputation
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import main.io.CommonFunctions as cf

OUTPUT_TABLE_NAME = 'SAS_FARES_IMP'


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\fares',
    r'tests\data\calculations\november_2017\fares',
    r'tests\data\calculations\october_2017\fares',
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 14 Sep 2018
    Purpose       : Tests the calculation function of the fares imputation step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Clear the survey import table
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
    cf.delete_from_table(OUTPUT_TABLE_NAME)

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"\surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')
    cf.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', df_surveydata)

    # Read the data from SQL (as it will in the production ready system)
    df_surveydata = cf.get_table_values('SAS_SURVEY_SUBSAMPLE')

    # Run the calculation step
    output_data = do_ips_fares_imputation(df_surveydata,
                                          var_serial='SERIAL',
                                          num_levels=9,
                                          measure='mean')

    def convert_dataframe_to_sql_format(table_name, dataframe):
        cf.insert_dataframe_into_table(table_name, dataframe)
        return cf.get_table_values(table_name)

    # Write the test result data to SQL then pull it back for comparison
    df_survey_result = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, output_data)

    # Clear down the result tables
    cf.delete_from_table(OUTPUT_TABLE_NAME)

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
