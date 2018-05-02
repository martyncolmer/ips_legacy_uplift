'''
Created on 15 Mar 2018

@author: burrj
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_fares_imputation import do_ips_fares_imputation


def test_calculate():

    test_survey = pd.read_pickle('tests/data/fares_imp_input.pkl')

    result_data = do_ips_fares_imputation(test_survey, var_serial='SERIAL',
                                          num_levels=9, measure='mean')

    test_result_summary = pd.read_pickle('tests/data/fares_imp_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()

    test_result_summary = test_result_summary.sort_values(by='SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by='SERIAL')
    result_data.index = range(0, len(result_data))

    # The dataframes have different column orders; check_like is True which will
    # ignore that fact when checking if the data is the same.
    assert_frame_equal(result_data, test_result_summary, check_like=True)

