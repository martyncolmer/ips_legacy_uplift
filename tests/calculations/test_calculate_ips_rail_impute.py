import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_rail_imputation import do_ips_railex_imp


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/rail',
    r'../data/calculations/november_2017/rail',
    # r'tests\data\calculations\october_2017\rail', # NO DATA FOR OCTOBER
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 25 Sept 2018
    Purpose       : Tests the calculation function of the rail imputation step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    # Run the calculation step
    output_data = do_ips_railex_imp(df_surveydata,
                                    var_serial='SERIAL',
                                    var_final_weight='FINAL_WT',
                                    minimum_count_threshold=30)

    def sort_and_set_index(df, sort_columns):
        df = df.sort_values(sort_columns)
        df.index = range(0, len(df))
        return df

    path_to_survey_result = data_path + r"/output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    output_data = sort_and_set_index(output_data, 'SERIAL')
    df_survey_expected = sort_and_set_index(df_survey_expected, 'SERIAL')

    assert_frame_equal(output_data, df_survey_expected)
