'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from IPS_Test_Modules.calculate_ips_rail_impute import calculate

def test_calculate():
    # This is an integration test as it runs the entire step

    test_survey = pd.read_pickle(r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Rail Imputation\surveydata.pkl')

    result_data = calculate(test_survey,
                            OutputData = 'SAS_RAIL_IMP', 
                            ResponseTable = 'SAS_RESPONSE', 
                            var_serial = 'SERIAL', 
                            var_flow = 'FLOW', 
                            var_fweight = 'FINAL_WT', 
                            var_count = 'COUNT',
                            strata = ['FLOW', 
                                      'RAIL_CNTRY_GRP_PV'],
                            var_railexercise = 'RAIL_EXERCISE_PV',
                            var_spend = 'SPEND',
                            minCountThresh = 30)

    test_result_summary = pd.read_pickle(r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Rail Imputation\output_test.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    
    test_result_summary = test_result_summary.sort_values(by = 'SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by = 'SERIAL')
    result_data.index = range(0, len(result_data))
        
    assert_frame_equal(result_data, test_result_summary,)
