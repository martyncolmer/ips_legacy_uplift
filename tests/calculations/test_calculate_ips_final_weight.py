'''
Created on 17 April 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_final_weight import calculate, do_ips_final_wt_calculation
import pytest

path_to_data = r"../data/final_weight"


def test_calculate():
    print("Started testing IPS final weight - calculate()")

    (surveydata_dataframe, summary_dataframe) = calculate(SurveyData='SAS_SURVEY_SUBSAMPLE'
                                                                      , var_serialNum='SERIAL'
                                                                      , var_shiftWeight='SHIFT_WT'
                                                                      , var_NRWeight='NON_RESPONSE_WT'
                                                                      , var_minWeight='MINS_WT'
                                                                      , var_trafficWeight='TRAFFIC_WT'
                                                                      , var_unsampWeight='UNSAMP_TRAFFIC_WT'
                                                                      , var_imbWeight='IMBAL_WT'
                                                                      , var_finalWeight='FINAL_WT')

    path_to_test = path_to_data + r"/output_final.pkl"
    test_df_output_final = pd.read_pickle(path_to_test)
    test_df_output_final.columns = test_df_output_final.columns.str.upper()
    assert_frame_equal(surveydata_dataframe, test_df_output_final, check_dtype=False)

    # unable to test summary_dataframe as it contains a random sample

    print("test_calculate() finished successfully")


def test_do_ips_final_wt_calculation():

    print("Started testing IPS final weight - do_ips_final_wt_calculation()")

    # Setup path to the base directory containing data files
    path_to_surveydata = path_to_data + r"\surveydata.pkl"
    df_surveydata = pd.read_pickle(path_to_surveydata)
    df_surveydata.columns = df_surveydata.columns.str.upper()

    (df_output, df_summary) = do_ips_final_wt_calculation(df_surveydata
                                                            , var_serialNum = 'SERIAL'
                                                            , var_shiftWeight = 'SHIFT_WT'
                                                            , var_NRWeight = 'NON_RESPONSE_WT'
                                                            , var_minWeight = 'MINS_WT'
                                                            , var_trafficWeight = 'TRAFFIC_WT'
                                                            , var_unsampWeight = 'UNSAMP_TRAFFIC_WT'
                                                            , var_imbWeight = 'IMBAL_WT'
                                                            , var_finalWeight = 'FINAL_WT')

    path_to_test = path_to_data + r"/output_final.pkl"
    test_df_output_final = pd.read_pickle(path_to_test)
    test_df_output_final.columns = test_df_output_final.columns.str.upper()
    assert_frame_equal(df_output, test_df_output_final, check_dtype=False)

    # unable to test df_summary as it contains a random sample

    print("test_do_ips_final_wt_calculation finished successfully")

