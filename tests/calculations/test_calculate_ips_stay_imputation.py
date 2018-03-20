'''
Created on 15 Mar 2018

@author: burrj
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal
from IPS_Stored_Procedures.ips_stay_imp import do_ips_stay_imputation

def test_calculate():
    # This is an integration test as it runs the entire step

    test_survey = pd.read_pickle(r'..\data\stay_imp_input.pkl')

    result_data = do_ips_stay_imputation(test_survey
                                , output = 'SAS_STAY_IMP'
                                , var_serial_num = 'SERIAL', var_stem = 'VARS'
                                , thresh_stem = 'THRESH', num_levels = 1
                                , donor_var = 'NUMNIGHTS', output_var = 'STAY'
                                , measure = 'mean'
                                , var_eligible_flag = 'STAY_IMP_ELIGIBLE_PV'
                                , var_imp_flag = 'STAY_IMP_FLAG_PV'
                                , var_imp_level = 'STAYK')

    test_result_summary = pd.read_pickle(r'../data/stay_imp_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    
    test_result_summary = test_result_summary.sort_values(by = 'SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by = 'SERIAL')
    result_data.index = range(0, len(result_data))
    
    # The dataframes have different column orders; check_like is True which will
    # ignore that fact when checking if the data is the same.
    assert_frame_equal(result_data, test_result_summary, check_like = True)

test_calculate()