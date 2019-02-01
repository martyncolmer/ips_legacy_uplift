import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_fares_imputation import do_ips_fares_imputation


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/fares',
    r'../data/calculations/november_2017/fares',
    r'../data/calculations/october_2017/fares',
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 26 Sep 2018
    Purpose       : Tests the calculation function of the fares imputation step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python', na_values=['£', '?', ' '])

    output_data = do_ips_fares_imputation(df_surveydata,
                                          var_serial='SERIAL',
                                          num_levels=9,
                                          measure='mean')

    path_to_survey_result = data_path + r"\outputdata_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    df_survey_result = output_data.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))

    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    # Check the output matches our expected results
    assert_frame_equal(df_survey_result, df_survey_expected, check_like=True, check_dtype=False)
