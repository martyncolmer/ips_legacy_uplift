"""
Created on 6 Nov 2018

@author: Nassir Mohammad
"""

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from ips import do_ips_trafweight_calculation_with_R


@pytest.mark.parametrize('path_to_data', [
    r'../data/calculations/december_2017/traffic_weight',
    r'../data/calculations/november_2017/traffic_weight',
    r'../data/calculations/october_2017/traffic_weight',
])
def test_calculate(path_to_data):

    # read in data from csv
    df_surveydata = pd.read_csv(path_to_data + '/surveydata.csv', engine='python')
    df_tr_data = pd.read_csv(path_to_data + '/trafficdata.csv', engine='python')

    # do the calculation
    df_output_merge_final, df_output_summary = do_ips_trafweight_calculation_with_R(df_surveydata, df_tr_data)

    # test start - turn on when testing/refactoring intermediate steps
    df_test = pd.read_csv(path_to_data + '/output_final.csv', engine='python')
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_output_merge_final, df_test, check_dtype=False, check_less_precise=True)

    df_test2 = pd.read_csv(path_to_data + '/summary_final.csv', engine='python')
    df_test2.columns = df_test2.columns.str.upper()
    assert_frame_equal(df_output_summary, df_test2, check_dtype=False, check_less_precise=True)
