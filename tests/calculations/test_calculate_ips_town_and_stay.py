import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from ips.calculations import calculate_ips_town_and_stay_expenditure as tse


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/town_and_stay',
    r'../data/calculations/november_2017/town_and_stay',
    r'../data/calculations/october_2017/town_and_stay',
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 13 Sep 2018
    Purpose       : Tests the calculation function of the town stay and expenditure step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    # Run the calculation step
    surveydata_out = tse.do_ips_town_exp_imp(df_survey_data=df_surveydata,
                                             var_serial="SERIAL",
                                             var_final_wt="FINAL_WT")

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"/output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    surveydata_out.sort_values('SERIAL', inplace=True)
    surveydata_out.index = range(0, len(surveydata_out))

    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(surveydata_out, df_survey_expected)
