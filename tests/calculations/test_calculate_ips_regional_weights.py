import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from ips import do_ips_regional_weight_calculation


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/regional_weights',
    r'../data/calculations/november_2017/regional_weights',
    r'../data/calculations/october_2017/regional_weights',
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 14 Sep 2018
    Purpose       : Tests the calculation function of the fares imputation step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    output_data = do_ips_regional_weight_calculation(df_surveydata, 'SERIAL', 'FINAL_WT')

    def sort_and_set_index(df, sort_columns):
        df = df.sort_values(sort_columns)
        df.index = range(0, len(df))
        return df

    output_data = sort_and_set_index(output_data, 'SERIAL')

    path_to_survey_result = data_path + r"/output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python', dtype={'VISIT_WTK': str,
                                                                                    'STAY_WTK': str,
                                                                                    'EXPENDITURE_WTK': str})

    df_survey_expected = sort_and_set_index(df_survey_expected, 'SERIAL')

    output_data = output_data[['SERIAL', 'VISIT_WT', 'STAY_WT', 'EXPENDITURE_WT']]
    df_survey_expected = df_survey_expected[['SERIAL', 'VISIT_WT', 'STAY_WT', 'EXPENDITURE_WT']]

    assert_frame_equal(output_data, df_survey_expected)
