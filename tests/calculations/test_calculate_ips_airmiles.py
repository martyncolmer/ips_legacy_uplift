"""
Created on 17 Sep 2019

@author: Nassir Mohammad
"""

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_airmiles import do_ips_airmiles_calculation


@pytest.mark.parametrize('path_to_data', [
    r'../data/calculations/december_2017/air_miles',
    r'../data/calculations/november_2017/air_miles',
    r'../data/calculations/october_2017/air_miles',
])
def test_calculate(path_to_data):
    # read in data from csv
    df_airmiles_import = pd.read_csv(path_to_data + '/airmiles.csv', engine='python')

    # Run the calculation
    output_data = do_ips_airmiles_calculation(df_surveydata=df_airmiles_import, var_serial='SERIAL')

    # Retrieve and sort python calculated dataframes
    py_out_data = output_data.sort_values(by='SERIAL')
    py_out_data.index = range(0, len(py_out_data))

    test_result = pd.read_csv(path_to_data + '/airmiles_output.csv', engine='python')
    test_result = test_result.sort_values(by='SERIAL')
    test_result.index = range(0, len(test_result))

    test_result.columns = test_result.columns.str.upper()

    assert_frame_equal(py_out_data, test_result, check_dtype=False, check_like=True)
