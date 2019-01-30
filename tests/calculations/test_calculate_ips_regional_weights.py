# def test_calculate():
#
#     test_survey = pd.read_pickle(path_to_data + '/regional_wt_input.pkl')
#
#     result_data = do_ips_regional_weight_calculation(test_survey, 'SERIAL', 'FINAL_WT')
#
#     test_result_summary = pd.read_pickle(path_to_data + '/regional_wt_output.pkl')
#     test_result_summary.columns = test_result_summary.columns.str.upper()
#
#     test_result_summary = test_result_summary.sort_values(by='SERIAL')
#     test_result_summary.index = range(0, len(test_result_summary))
#     result_data = result_data.sort_values(by='SERIAL')
#     result_data.index = range(0, len(result_data))
#
#     str_columns = test_result_summary.dtypes[test_result_summary.dtypes == 'object'].index.tolist()
#     test_result_summary[str_columns] = test_result_summary[str_columns].replace(np.NaN, '')
#
#     assert_frame_equal(result_data, test_result_summary, check_like=True)


from main.calculations.calculate_ips_regional_weights import do_ips_regional_weight_calculation
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import utils.common_functions as cf

OUTPUT_TABLE_NAME = 'SAS_REGIONAL_IMP'


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\regional_weights',
    r'tests\data\calculations\november_2017\regional_weights',
    r'tests\data\calculations\october_2017\regional_weights',
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

    output_data = do_ips_regional_weight_calculation(df_surveydata, 'SERIAL', 'FINAL_WT')

    def convert_dataframe_to_sql_format(table_name, dataframe):
        cf.insert_dataframe_into_table(table_name, dataframe)
        return cf.get_table_values(table_name)

    def sort_and_set_index(df, sort_columns):
        df = df.sort_values(sort_columns)
        df.index = range(0, len(df))
        return df

    output_data = sort_and_set_index(output_data, 'SERIAL')

    # Write the test result data to SQL then pull it back for comparison
    df_survey_result = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, output_data)

    # Clear down the result tables
    cf.delete_from_table(OUTPUT_TABLE_NAME)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python', dtype={'VISIT_WTK': str,
                                                                                    'STAY_WTK': str,
                                                                                    'EXPENDITURE_WTK': str})

    df_survey_expected = sort_and_set_index(df_survey_expected, 'SERIAL')
    df_survey_expected = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, df_survey_expected)

    # Sort the dataframes for comparison (SECOND SORT - is this needed?
    # (dependent on how the data is returned from oracle - if sorted on return no need to resort))
    df_survey_result = sort_and_set_index(df_survey_result, 'SERIAL')
    df_survey_expected = sort_and_set_index(df_survey_expected, 'SERIAL')

    df_survey_result = df_survey_result[['SERIAL','VISIT_WT','STAY_WT','EXPENDITURE_WT']]
    df_survey_expected = df_survey_expected[['SERIAL','VISIT_WT','STAY_WT','EXPENDITURE_WT']]

    assert_frame_equal(df_survey_result, df_survey_expected)
