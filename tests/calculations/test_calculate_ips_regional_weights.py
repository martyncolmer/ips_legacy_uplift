'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_regional_weights import do_ips_regional_weight_calculation
import sys

def compare_dfs(test_name, sas_file, df, col_list = False, save_index = False,drop_sas_col = True):
        
    import winsound
    
    def beep():
        frequency = 500  # Set Frequency To 2500 Hertz
        duration = 200  # Set Duration To 1000 ms == 1 second
        winsound.Beep(frequency, duration)
    
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Regional Weights"
    print sas_root + "\\" + sas_file
    csv = pd.read_sas(sas_root + "\\" + sas_file)
    
    fdir = r"\\NDATA12\mahont1$\My Documents\GIT_Repositories\Test_Drop"
    sas = "_sas.csv"
    py = "_py.csv"
    
    print("TESTING " + test_name)
    
    # Set all of the columns imported to uppercase
    csv.columns = csv.columns.str.upper()
    
    if drop_sas_col:
        if '_TYPE_' in csv.columns:
            csv = csv.drop(columns = ['_TYPE_'])
            
        
        if '_FREQ_' in csv.columns:
            csv = csv.drop(columns = ['_FREQ_'])
    
    if col_list == False:
        csv.to_csv(fdir+"\\"+test_name+sas, index = save_index)
        df.to_csv(fdir+"\\"+test_name+py, index = save_index)
    else:
        csv[col_list].to_csv(fdir+"\\"+test_name+sas)
        df[col_list].to_csv(fdir+"\\"+test_name+py)
    
    
    print(test_name + " COMPLETE")
    beep()
    print("") 


def test_calculate():
    # This is an integration test as it runs the entire step

    strata1  = ['FLOW', 
                  'PURPOSE_PV', 
                  'STAYIMPCTRYLEVEL1_PV']
    strata2  = ['FLOW', 
                  'PURPOSE_PV', 
                  'STAYIMPCTRYLEVEL2_PV']
    strata3  = ['FLOW', 
                  'PURPOSE_PV', 
                  'STAYIMPCTRYLEVEL3_PV']
    strata4  = ['FLOW', 
                  'PURPOSE_PV', 
                  'STAYIMPCTRYLEVEL4_PV']

    # Set up strata lists
    strata_levels = [strata1, strata2, strata3, strata4]
    
    test_survey = pd.read_pickle('../data/regional_wt_input.pkl')
    
    result_data = do_ips_regional_weight_calculation(test_survey, 'SERIAL', 4, 'STRATA', 
                                                          'STAY', 'SPEND', 'FINAL_WT', 'STAY_WT', 
                                                          'VISIT_WT', 'EXPENDITURE_WT', 'STAY_WTK', 
                                                          'VISIT_WTK', 'EXPENDITURE_WTK', 'REG_IMP_ELIGIBLE_PV',
                                                          strata_levels)

    test_result_summary = pd.read_pickle('../data/regional_wt_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    
    test_result_summary = test_result_summary.sort_values(by = 'SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by = 'SERIAL')
    result_data.index = range(0, len(result_data))
    
    assert_frame_equal(result_data, test_result_summary,check_like=True)


test_calculate()