'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_regional_weights import do_ips_regional_weight_calculation
import numpy as np


def test_calculate():
    # This is an integration test as it runs the entire step

    strata1 = ['FLOW',
               'PURPOSE_PV',
               'STAYIMPCTRYLEVEL1_PV']
    strata2 = ['FLOW',
               'PURPOSE_PV',
               'STAYIMPCTRYLEVEL2_PV']
    strata3 = ['FLOW',
               'PURPOSE_PV',
               'STAYIMPCTRYLEVEL3_PV']
    strata4 = ['FLOW',
               'PURPOSE_PV',
               'STAYIMPCTRYLEVEL4_PV']

    strata_levels = [strata1, strata2, strata3, strata4]
    
    test_survey = pd.read_pickle('../data/regional_wt_input.pkl')
    
    result_data = do_ips_regional_weight_calculation(test_survey, 'SERIAL', 4,
                                                     'STAY', 'SPEND', 'FINAL_WT', 'STAY_WT',
                                                     'VISIT_WT', 'EXPENDITURE_WT', 'STAY_WTK',
                                                     'VISIT_WTK', 'EXPENDITURE_WTK', 'REG_IMP_ELIGIBLE_PV',
                                                     strata_levels)

    test_result_summary = pd.read_pickle('../data/regional_wt_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    
    test_result_summary = test_result_summary.sort_values(by='SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by='SERIAL')
    result_data.index = range(0, len(result_data))

    str_columns = test_result_summary.dtypes[test_result_summary.dtypes == 'object'].index.tolist()
    test_result_summary[str_columns] = test_result_summary[str_columns].replace(np.NaN, '')

    assert_frame_equal(result_data, test_result_summary, check_like=True)


test_calculate()
