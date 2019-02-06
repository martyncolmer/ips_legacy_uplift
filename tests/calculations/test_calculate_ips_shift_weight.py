import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from ips.calculations.calculate_ips_shift_weight import do_ips_shift_weight_calculation


@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/shift_weight',
    r'../data/calculations/november_2017/shift_weight',
    r'../data/calculations/october_2017/shift_weight',
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 14 Sep 2018
    Purpose       : Tests the calculation function of the shift weight step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    path_to_shiftdata = data_path + r'/shiftsdata.csv'
    df_shiftsdata = pd.read_csv(path_to_shiftdata, engine='python')

    # Run the calculation step
    output_data, summary_data = do_ips_shift_weight_calculation(df_surveydata,
                                                                df_shiftsdata,
                                                                var_serialNum='SERIAL',
                                                                var_shiftWeight='SHIFT_WT')

    df_survey_result = output_data
    df_summary_result = summary_data

    path_to_survey_result = data_path + r"/outputdata_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    df_survey_result = df_survey_result.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))
    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(df_survey_result, df_survey_expected)

    path_to_survey_result = data_path + r"/summarydata_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    df_summary_result = df_summary_result.sort_values(
        ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV'])
    df_summary_result.index = range(0, len(df_summary_result))

    df_summary_expected = df_summary_expected.sort_values(
        ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV'])
    df_summary_expected.index = range(0, len(df_summary_expected))

    assert_frame_equal(df_summary_expected, df_summary_result, check_less_precise=True)
