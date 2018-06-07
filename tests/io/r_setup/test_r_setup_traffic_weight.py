'''
Created on 04 Jun 2018

@author: David Powell
'''

import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal
from main.io.r_setup_traffic_weight import r_survey_input, r_population_input
import sys


def test_r_survey_input():

    # Import the test data
    df_survey_input = pd.read_pickle('../tests/data/r_setup/December_2017/traffic_weight/survey_input.pkl')

    # Import the test data
    df_ges_input = pd.read_pickle('../tests/data/r_setup/December_2017/traffic_weight/ges_input.pkl')

    # Run the test
    df_test_result = r_survey_input(df_survey_input, df_ges_input)

    # Expected result
    test_file = r"../tests/data/r_setup/December_2017/traffic_weight/df_r_ges_input.pkl"

    df_expected_result = pd.read_pickle(test_file)

    df_expected_result['SAMP_PORT_GRP_PV'] = df_expected_result['SAMP_PORT_GRP_PV'].str.decode("utf-8")

    df_test_result['SERIAL'] = df_test_result.SERIAL.astype(np.float64)
    df_test_result['T1'] = df_test_result.T1.astype(np.float64)
    df_test_result['SAMP_PORT_GRP_PV'] = df_test_result.SAMP_PORT_GRP_PV.astype(np.str)

    # Check if the test and result dataframes match
    assert_frame_equal(df_test_result, df_expected_result, check_dtype=True, check_like=True)
    print("DONE")


def test_r_population_input():

    # Import the test data
    df_survey_input = pd.read_pickle('../tests/data/r_setup/December_2017/traffic_weight/survey_input.pkl')

    # Import the test data
    df_tr_totals = pd.read_pickle('../tests/data/r_setup/December_2017/traffic_weight/trtotals.pkl')

    # Import the test data
    lookup = pd.read_pickle('../tests/data/r_setup/December_2017/traffic_weight/lookup.pkl')

    # Run the test
    df_test_result = r_population_input(df_survey_input, df_tr_totals, lookup)

    # Expected result
    test_file = r"../tests/data/r_setup/December_2017/traffic_weight/PopRowVec.pkl"

    df_expected_result = pd.read_pickle(test_file)

    df_test_result.index = range(0, len(df_test_result))

    df_expected_result['C_group'] = df_expected_result['C_group'].str.decode("utf-8")
    df_test_result['C_group'] = df_test_result.C_group.astype(np.str)

    # Check if the test and result dataframes match
    assert_frame_equal(df_test_result, df_expected_result, check_dtype=False, check_like=True)
    print("DONE")