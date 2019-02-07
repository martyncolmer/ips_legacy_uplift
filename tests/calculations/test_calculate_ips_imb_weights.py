import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from ips import calculate_ips_imb_weight as imb


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/imbalance_weight',
    r'../data/calculations/november_2017/imbalance_weight',
    r'../data/calculations/october_2017/imbalance_weight',
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 13 Sep 2018
    Purpose       : Tests the calculation function of the town stay and expenditure step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    # Run the calculation step
    output_data, summary_data = imb.do_ips_imbweight_calculation(df_surveydata
                                                                 , var_serialNum="SERIAL"
                                                                 , var_shiftWeight="SHIFT_WT"
                                                                 , var_NRWeight="NON_RESPONSE_WT"
                                                                 , var_minWeight="MINS_WT"
                                                                 , var_trafficWeight="TRAFFIC_WT"
                                                                 , var_OOHWeight="UNSAMP_TRAFFIC_WT"
                                                                 , var_imbalanceWeight="IMBAL_WT")

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"/output_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    df_survey_result = output_data.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))
    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    df_survey_result = df_survey_result.apply(lambda x: round(x, 3))
    df_survey_expected = df_survey_expected.apply(lambda x: round(x, 3))

    assert_frame_equal(df_survey_result, df_survey_expected, check_dtype=False)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"/summary_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Remove NULL flow calculated value (This only exists in imbalance weight - 13/09/2018)
    df_summary_expected = df_summary_expected.loc[df_summary_expected['FLOW'].notnull()]

    # Sort the dataframes for comparison
    df_summary_result = summary_data.sort_values(['FLOW'])
    df_summary_result.index = range(0, len(df_summary_result))
    df_summary_expected = df_summary_expected.sort_values(['FLOW'])
    df_summary_expected.index = range(0, len(df_summary_expected))

    # do some rounding
    df_summary_result = df_summary_result.apply(lambda x: round(x, 3))
    df_summary_expected = df_summary_expected.apply(lambda x: round(x, 3))

    df_summary_expected.columns = df_summary_expected.columns.str.upper()

    assert_frame_equal(df_summary_result, df_summary_expected, check_dtype=False)
