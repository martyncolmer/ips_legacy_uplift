'''
Created on 04 Jun 2018

@author: David Powell
'''

import pandas as pd
import numpy as np
import pytest as pytest
from sas7bdat import SAS7BDAT
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_unsampled_weight import r_survey_input, r_population_input
import sys


def test_r_survey_input():

    # Import the test data
    survey_input = pd.read_pickle('tests/data/r_setup/October_2017/unsampled_weight/survey_input.pkl')

    # Run the test
    df_test_result = r_survey_input(survey_input)

    # Expected result
    test_file = r"tests/data/r_setup/October_2017/unsampled_weight/df_r_ges_input.pkl"

    df_expected_result = pd.read_pickle(test_file)

    df_test_result = df_test_result.sort_values(['SERIAL'])
    df_expected_result = df_test_result.sort_values(['SERIAL'])

    # Check if the test and result dataframes match
    assert_frame_equal(df_test_result, df_expected_result, check_dtype=False, check_like=True)
    print("DONE")


def test_r_population_input():

    # Import the test data
    survey_input = pd.read_pickle('tests/data/r_setup/October_2017/unsampled_weight/survey_input.pkl')

    ustotals = pd.read_pickle('tests/data/r_setup/October_2017/unsampled_weight/ustotals.pkl')

    # Run the test
    df_test_result = r_population_input(survey_input, ustotals)

    # Expected result
    test_file = r"tests/data/r_setup/October_2017/unsampled_weight/poprowvec.pkl"

    df_test_result.index = range(0, len(df_test_result))

    df_expected_result = pd.read_pickle(test_file)

    df_expected_result['C_group'] = df_expected_result['C_group'].str.decode("utf-8")
    df_test_result['C_group'] = df_test_result.C_group.astype(np.str)

    # Check if the test and result dataframes match
    assert_frame_equal(df_test_result, df_expected_result, check_dtype=False, check_like=True)
    print("DONE")