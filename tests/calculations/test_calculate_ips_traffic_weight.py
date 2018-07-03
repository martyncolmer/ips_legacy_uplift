'''
Created on 12 March 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_traffic_weight import calculate, do_ips_ges_weighting, \
    do_ips_trafweight_calculation, generate_ips_tw_summary
import tests.config

path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/traffic_weight"

def test_calculate():
    (df_output_merge_final_rounded, df_summary_merge_sum_traftot) = calculate(SurveyData='sas_survey_subsample'
                                                                              , var_serialNum='serial'.upper()
                                                                              , var_shiftWeight='shift_wt'.upper()
                                                                              , var_NRWeight='non_response_wt'.upper()
                                                                              , var_minWeight='mins_wt'.upper()
                                                                              , PopTotals='sas_traffic_data'
                                                                              , GWeightVar='traffic_wt'.upper()
                                                                              , minCountThresh=30)

    path_to_test = path_to_data + r"/output_rounded.pkl"
    test_df_output_merge_final_rounded = pd.read_pickle(path_to_test)
    test_df_output_merge_final_rounded.columns = test_df_output_merge_final_rounded.columns.str.upper()
    assert_frame_equal(df_output_merge_final_rounded, test_df_output_merge_final_rounded)

    path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot, check_dtype=False,check_column_type=False)


def test_do_ips_trafweight_calculation():
    path_to_test = path_to_data + r"/survey_input.pkl"
    df_survey = pd.read_pickle(path_to_test)
    df_survey.columns = df_survey.columns.str.upper()

    path_to_test = path_to_data + r"/trtotals.pkl"
    df_trtotals = pd.read_pickle(path_to_test)
    df_trtotals.columns = df_trtotals.columns.str.upper()

    (df_output_merge_final_rounded, df_summary_merge_sum_traftot) = do_ips_trafweight_calculation(
        df_survey
        , var_serialNum='serial'.upper()
        , var_shiftWeight='shift_wt'.upper()
        , var_NRWeight='non_response_wt'.upper()
        , var_minWeight='mins_wt'.upper()
        , PopTotals=df_trtotals
        , GWeightVar='traffic_wt'.upper()
        , minCountThresh=30)

    path_to_test = path_to_data + r"/output_rounded.pkl"
    test_df_output_merge_final_rounded = pd.read_pickle(path_to_test)
    test_df_output_merge_final_rounded.columns = test_df_output_merge_final_rounded.columns.str.upper()
    assert_frame_equal(df_output_merge_final_rounded, test_df_output_merge_final_rounded)

    path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot)


def test_do_ips_ges_weighting():

    path_to_test = path_to_data + r"/in_1.pkl"
    df_survey = pd.read_pickle(path_to_test)
    df_survey.columns = df_survey.columns.str.upper()

    (df_output_merge_final, df_survey_serialNum_sort) = do_ips_ges_weighting(df_survey
                                                                             , var_serialNum='serial'.upper()
                                                                             , df_popTotals="assign"
                                                                             , GWeightVar='traffic_wt'.upper()
                                                                             , CalWeight="assign")

    path_to_test = path_to_data + r"/output_merge_final.pkl"
    test_df_output_merge_final = pd.read_pickle(path_to_test)
    test_df_output_merge_final.columns = test_df_output_merge_final.columns.str.upper()
    assert_frame_equal(df_output_merge_final, test_df_output_merge_final)

    path_to_test = path_to_data + r"/survey_serialNum_sort.pkl"
    test_df_survey_serialNum_sort = pd.read_pickle(path_to_test)
    test_df_survey_serialNum_sort.columns = test_df_survey_serialNum_sort.columns.str.upper()
    assert_frame_equal(df_survey_serialNum_sort, test_df_survey_serialNum_sort)


def test_generate_ips_tw_summary():

    path_to_test = path_to_data + r"/in_1.pkl"
    df_survey = pd.read_pickle(path_to_test)
    df_survey.columns = df_survey.columns.str.upper()

    path_to_test = path_to_data + r"/output_merge_final.pkl"
    df_output_merge_final = pd.read_pickle(path_to_test)
    df_output_merge_final.columns = df_output_merge_final.columns.str.upper()

    path_to_test = path_to_data + r"/poptotals_summary_1.pkl"
    df_poptotals_summary_1 = pd.read_pickle(path_to_test)
    df_poptotals_summary_1.columns = df_poptotals_summary_1.columns.str.upper()

    df_summary_merge_sum_traftot = generate_ips_tw_summary(df_survey
                                                           , df_output_merge_final
                                                           , var_serialNum='serial'.upper()
                                                           , var_trafficWeight='traffic_wt'.upper()
                                                           , df_popTotals=df_poptotals_summary_1
                                                           , minCountThresh=30)

    path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot, check_column_type=False)