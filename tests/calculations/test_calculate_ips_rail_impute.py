
from main.calculations.calculate_ips_rail_imputation import do_ips_railex_imp
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import utils.common_functions as cf

OUTPUT_TABLE_NAME = 'SAS_RAIL_IMP'


@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\rail',
    r'tests\data\calculations\november_2017\rail',
    #r'tests\data\calculations\october_2017\rail', # NO DATA FOR OCTOBER
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 25 Sept 2018
    Purpose       : Tests the calculation function of the rail imputation step works as expected.
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
    output_data = do_ips_railex_imp(df_surveydata,
                                    var_serial='SERIAL',
                                    var_final_weight='FINAL_WT',
                                    minimum_count_threshold=30)

    def convert_dataframe_to_sql_format(table_name, dataframe):
        cf.insert_dataframe_into_table(table_name, dataframe)
        return cf.get_table_values(table_name)

    def sort_and_set_index(df, sort_columns):
        df = df.sort_values(sort_columns)
        df.index = range(0, len(df))
        return df

    # Write the test result data to SQL then pull it back for comparison
    df_survey_result = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, output_data)

    # Clear down the result tables
    cf.delete_from_table(OUTPUT_TABLE_NAME)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_survey_expected = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, df_survey_expected)

    # Sort the dataframes for comparison
    df_survey_result = sort_and_set_index(df_survey_result, 'SERIAL')
    df_survey_expected = sort_and_set_index(df_survey_expected, 'SERIAL')

    assert_frame_equal(df_survey_result, df_survey_expected)
