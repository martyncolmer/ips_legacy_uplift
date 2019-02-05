import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_final_weight import do_ips_final_wt_calculation


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/final_weight',
    r'../data/calculations/november_2017/final_weight',
    r'../data/calculations/october_2017/final_weight',
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests the calculation function of the final weight step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    # Run the calculation step
    surveydata_out, summary_out = do_ips_final_wt_calculation(df_surveydata,
                                                              serial_num='SERIAL',
                                                              shift_weight='SHIFT_WT',
                                                              non_response_weight='NON_RESPONSE_WT',
                                                              min_weight='MINS_WT',
                                                              traffic_weight='TRAFFIC_WT',
                                                              unsampled_weight='UNSAMP_TRAFFIC_WT',
                                                              imbalance_weight='IMBAL_WT',
                                                              final_weight='FINAL_WT')

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"/output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    df_survey_result = surveydata_out.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))

    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    df_survey_expected.columns = df_survey_expected.columns.str.upper()
    # PS: round the values for comparison
    df_survey_result['FINAL_WT'] = df_survey_result['FINAL_WT'].apply(lambda x: round(x, 3))
    df_survey_expected['FINAL_WT'] = df_survey_expected['FINAL_WT'].apply(lambda x: round(x, 3))

    assert_frame_equal(df_survey_result, df_survey_expected, check_like=True, check_dtype=False)

