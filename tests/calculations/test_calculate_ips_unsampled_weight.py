'''
Created on April 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_unsampled_weight import calculate, do_ips_unsampled_weight_calculation
import pytest

path_to_data = '../../tests/data/unsampled_weight'


@pytest.mark.unsampled
def test_calculate():
    print("Started testing IPS unsampled weight - calculate()")
    (output_dataframe, summary_dataframe) = calculate(SurveyData='SAS_SURVEY_SUBSAMPLE',
                                                      var_serialNum='SERIAL',
                                                      var_shiftWeight='SHIFT_WT',
                                                      var_NRWeight='NON_RESPONSE_WT',
                                                      var_minWeight='MINS_WT',
                                                      var_trafficWeight='TRAFFIC_WT',
                                                      minCountThresh=30)

    test_df = pd.read_pickle(path_to_data + r"/output_final.pkl")
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(output_dataframe, test_df)

    test_summary = pd.read_pickle(path_to_data + r"/summary_final.pkl")
    test_summary.columns = test_summary.columns.str.upper()
    assert_frame_equal(summary_dataframe, test_summary, check_like=True, check_dtype=False)

@pytest.mark.unsampled_weight
def test_do_ips_unsampled_weight_calculation():

    df_surveydata = pd.read_pickle(path_to_data + r"/survey_input.pkl")
    df_ustotals = pd.read_pickle(path_to_data + r"/ustotals.pkl")

    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_ustotals.columns = df_ustotals.columns.str.upper()

    output_dataframe, summary_dataframe = do_ips_unsampled_weight_calculation(df_surveydata,
                                                                              var_serialNum='SERIAL',
                                                                              var_shiftWeight='SHIFT_WT',
                                                                              var_NRWeight='NON_RESPONSE_WT',
                                                                              var_minWeight='MINS_WT',
                                                                              var_trafficWeight='TRAFFIC_WT',
                                                                              df_ustotals=df_ustotals,
                                                                              minCountThresh=30)

    test_df = pd.read_pickle(path_to_data + r"/output_final.pkl")
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(output_dataframe, test_df)

    test_summary = pd.read_pickle(path_to_data + r"/summary_final.pkl")
    test_summary.columns = test_summary.columns.str.upper()
    assert_frame_equal(summary_dataframe, test_summary, check_like=True, check_dtype=False)