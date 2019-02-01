import numpy as np
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

import tests.config
from main.calculations.calculate_ips_minimums_weight import do_ips_minweight_calculation

path_to_data = r"../data/calculations/" + tests.config.TEST_MONTH + "/min_weight"


# SAS data for november and october is incorrect.
# November lacks the NR weight calculation required to run this step.
# October is missing the final summary output of the step stopping us
# from comparing the summaries produced.

@pytest.mark.parametrize('data_path', [
    r'../data/calculations/december_2017/min_weight',
    # r'tests\data\calculations\november_2017\min_weight',
    # r'tests\data\calculations\october_2017\min_weight',
])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 11 Sep 2018
    Purpose       : Tests the calculation function of the minimums weight step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"/surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')

    # Run the calculation step
    output_data, summary_data = do_ips_minweight_calculation(df_surveydata=df_surveydata,
                                                             var_serialNum='SERIAL',
                                                             var_shiftWeight='SHIFT_WT',
                                                             var_NRWeight='NON_RESPONSE_WT',
                                                             var_minWeight='MINS_WT')

    # The previous test added data to the database and then retrieved it thereby
    # addition three additional empty columns. We will do the same otherwise
    # the test will fail as the expected data contains those columns
    summary_data['ARRIVEDEPART'] = np.nan
    summary_data['MINS_NAT_GRP_PV'] = np.nan
    summary_data['MINS_CTRY_PORT_GRP_PV'] = np.nan

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"/outputdata_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    df_survey_result = output_data.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))

    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(df_survey_result, df_survey_expected)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"/summarydata_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')

    # Sort the dataframes for comparison
    summary_data.columns = summary_data.columns.str.upper()
    df_summary_result = summary_data.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    df_summary_result.index = range(0, len(df_summary_result))

    df_summary_expected.columns = df_summary_expected.columns.str.upper()
    df_summary_expected = df_summary_expected.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    df_summary_expected.index = range(0, len(df_summary_expected))

    assert_frame_equal(df_summary_result, df_summary_expected, check_like=True, check_dtype=False)
