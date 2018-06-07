'''
Created on 04 Jun 2018

@author: David Powell
'''

import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal
from main.io.r_setup_unsampled_weight import r_survey_input, r_population_input
import sys


def test_r_survey_input():

    # Import the test data
    df_survey_input = pd.read_pickle('../tests/data/r_setup/December_2017/unsampled_weight/survey_input.pkl')

    # Import the test data
    df_ges_input = pd.read_pickle('../tests/data/r_setup/December_2017/unsampled_weight/ges_input.pkl')

    # Run the test
    df_test_result = r_survey_input(df_survey_input, df_ges_input)

    # Expected result
    test_file = r"../tests/data/r_setup/December_2017/unsampled_weight/df_r_ges_input.pkl"

    df_expected_result = pd.read_pickle(test_file)

    # Check if the test and result dataframes match
    assert_frame_equal(df_test_result, df_expected_result, check_dtype=False, check_like=True)
    print("DONE")


def test_r_population_input():

    # Import the test data
    df_pop_row_vec = pd.read_pickle('../tests/data/r_setup/December_2017/unsampled_weight/poprowvec.pkl')

    # Run the test
    df_test_result = r_population_input(df_pop_row_vec)

    # Expected result
    test_file = r"../tests/data/r_setup/December_2017/unsampled_weight/poprowvec.pkl"

    df_expected_result = pd.read_pickle(test_file)

    # Check if the test and result dataframes match
    assert_frame_equal(df_test_result, df_expected_result, check_dtype=False, check_like=True)
    print("DONE")