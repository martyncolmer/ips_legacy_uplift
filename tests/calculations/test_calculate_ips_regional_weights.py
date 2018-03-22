'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_regional_weights import do_ips_regional_weight_calculation

def test_calculate():
    # This is an integration test as it runs the entire step

    # Set all of the columns imported to uppercase
    df_surveydata.columns = df_surveydata.columns.str.upper()

    # Set up strata lists
    strata_levels = [strata1, strata2, strata3, strata4]
    
    # Calculate the unsampled weights of the imported dataset.
    print("Start - Calculate Regional Weights.")     
    output_dataframe = do_ips_regional_weight_calculation(df_surveydata, var_serial, maxLevel, strataBase, 
                                                          var_stay, var_spend, var_final_wt, var_stay_wt, 
                                                          var_visit_wt, var_expenditure_wt, var_stay_wtk, 
                                                          var_visit_wtk, var_expenditure_wtk, var_eligible_flag,
                                                          strata_levels)


    test_survey = pd.read_pickle('data/rail_imp_input.pkl')

    result_data = do_ips_regional_weight_calculation(test_survey,
                            output = 'SAS_RAIL_IMP',
                            var_serial = 'SERIAL', 
                            var_eligible = 'FLOW',
                            var_fweight = 'FINAL_WT', 
                            var_count = 'COUNT',
                            strata = ['FLOW', 
                                      'RAIL_CNTRY_GRP_PV'],
                            var_railfare = 'RAIL_EXERCISE_PV',
                            var_spend = 'SPEND',
                            minCountThresh = 30)

    test_result_summary = pd.read_pickle('data/rail_imp_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    
    test_result_summary = test_result_summary.sort_values(by = 'SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by = 'SERIAL')
    result_data.index = range(0, len(result_data))
        
    assert_frame_equal(result_data, test_result_summary,)
