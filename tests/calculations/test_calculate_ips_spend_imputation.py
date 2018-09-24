# '''
# Created on 12 Mar 2018
#
# @author: thorne1
# '''
# import pandas as pd
# from pandas.util.testing import assert_frame_equal
# from main.calculations import calculate_ips_spend_imputation as spend
#
# import pytest
# import tests.config
#
# path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/spend"
#
#
# @pytest.mark.skip("Known failure due to rounding")
# def test_calculate():
#     # This is an integration test as it runs the entire step
#     test_survey = pd.read_pickle(path_to_data + '/spend_imp_surveydata.pkl')
#
#     # Retrieve Python output data
#     py_survey_data = spend.do_ips_spend_imputation(test_survey, var_serial="SERIAL", measure="mean")
#     # Cleanse
#     py_survey_data = py_survey_data.sort_values(by='SERIAL')
#     py_survey_data.index = range(0, len(py_survey_data))
#
#     # Retrieve SAS Survey Data output and cleanse
#     test_result_survey = pd.read_pickle(path_to_data + '/spend_imp_output_merge_eligible.pkl')
#     test_result_survey.columns = test_result_survey.columns.str.upper()
#     test_result_survey = test_result_survey.sort_values(by='SERIAL')
#     test_result_survey.index = range(0, len(test_result_survey))
#
#     # Assert dfs are equal
#     assert_frame_equal(py_survey_data
#                        , test_result_survey
#                        , check_column_type=False)


from main.calculations.calculate_ips_spend_imputation import do_ips_spend_imputation
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import main.io.CommonFunctions as cf

OUTPUT_TABLE_NAME = 'SAS_SPEND_IMP'


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\spend - new',
    r'tests\data\calculations\november_2017\spend - new',
    r'tests\data\calculations\october_2017\spend - new',
    ])
def test_calculate(data_path):
    """
    Author        : Elinor Thorne
    Date          : 21 Sept 2018
    Purpose       : Tests the calculation function of the spent imputation step works as expected.
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
    output_data = do_ips_spend_imputation(df_surveydata,
                                          var_serial="SERIAL",
                                          measure="mean")

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