    
import sys
import os
import logging
import inspect
import math
import numpy as np
import pandas as pd
from sas7bdat import SAS7BDAT
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict
import survey_support
from IPSTransformation import CommonFunctions as cf
import IPS_Stored_Procedures.ips_ges_weighting

output_counter = 0
 
def compare_dfs(test_name, sas_file, df, col_list = False, save_index = False,drop_sas_col = True):
    
    global output_counter
    test_name = str(output_counter) + '_' + test_name
    output_counter += 1
    
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
    
    
def ips_correct_regional_nights(input,stay):
    
    # Adjust regional night figures so that they match overall stay
    def compute(row, stay):
        if(row['INFO_PRESENT_MKR'] == 1):
            known_town_nk_nights=0
            nights_sum=0;
            stay_sum=0;
            
            # Compute nights_sum and known_town_nk_nights for this record
            for x in range(1,9):
                if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]):
                    if not math.isnan(row['NIGHTS' + str(x)]):
                        nights_sum = nights_sum + row['NIGHTS' + str(x)]
                    else:
                        known_town_nk_nights = known_town_nk_nights + 1;
            
            
            if known_town_nk_nights == 0:
                # Check if sum of nights is not equal to stay
                if nights_sum != row[stay]:
                    stay_sum = (row[stay]/nights_sum)
                    
                    for x in range(1,9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]):
                            row['NIGHTS' + str(x)] = row['NIGHTS' + str(x)] * stay_sum
                            row['STAY' + str(x) + 'K'] = 'K'
            else:
                # If town has known code add stay to total nights_sum
                # if town is null adds 1 to unknown
                if nights_sum >= row[stay]:
                    
                    for x in range(1,9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]) and math.isnan(row['NIGHTS' + str(x)]):
                            row['NIGHTS' + str(x)] = 1
                            nights_sum = nights_sum + row['NIGHTS' + str(x)]
                    
                    # Calculate nights uplift factor
                    stay_sum = (row[stay]/nights_sum)
            
                    for x in range(1,9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]):
                            row['NIGHTS' + str(x)] = row['NIGHTS' + str(x)] * stay_sum
                            row['STAY' + str(x) + 'K'] = 'L'
                            
                else:
                    for x in range(1,9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]) and math.isnan(row['NIGHTS' + str(x)]):
                            row['NIGHTS' + str(x)] = (row[stay] - nights_sum) / known_town_nk_nights
                            row['STAY' + str(x) + 'K'] = 'M'
                            
            
        return row
    
    df_output = input.apply(compute,axis = 1,args = (stay,))
    
    return df_output
    

def do_ips_regional_weight_calculation(inputData, outputData, var_serial, maxLevel, strataBase, 
                               var_stay, var_spend, var_final_wt, var_stay_wt, var_visit_wt, 
                               var_expenditure_wt, var_stay_wtk, var_visit_wtk, 
                               var_expenditure_wtk, var_eligible_flag, strata_levels):  
    """
    Author       : Thomas Mahoney
    Date         : 12 / 03 / 2018
    Purpose      : Calculates regional weights for IPS.
    Parameters   : inputData - the IPS survey records for the period.
                   outputData - dataset to hold output data
                   summary - dataset to hold summary output
                   var_serial - Variable holding the record serial number
                   maxLevel - the number of the last imputation level (1-up)
                   strataBase - the stem for the strata parameters
                   var_stay - the name of the stay variable
                   var_spend - the name of the spend variable
                   var_final_wt - the name of the final weight variable
                   var_stay_wt - the name of the stay weight output variable
                   var_visit_wt - the name of the visit weight output variable
                   var_expenditure_wt - the name of the spend weight output variable
                   var_stay_wtk - the name of the stay weight key output variable
                   var_visit_wtk - the name of the visit weight key output variable
                   var_expenditure_wtk - the name of the spend weight key output variable
                   var_eligible_flag - the name of the eligibility flag variable
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Do some pre-processing
    impute_towns = inputData[inputData[var_eligible_flag] == 1]
    
    impute_towns.loc[:,var_stay_wt] = 1
    impute_towns.loc[:,var_stay_wtk] = ''
    
    impute_towns.loc[:,var_visit_wt] = 1
    impute_towns.loc[:,var_visit_wtk] = ''
    
    impute_towns.loc[:,var_expenditure_wt] = 1
    impute_towns.loc[:,var_expenditure_wtk] = ''

    def check_info(row):
        
        if row['TOWNCODE1'] == 99999  or math.isnan(row['TOWNCODE1']): 
            row['INFO_PRESENT_MKR'] = 0
        else:
            row['INFO_PRESENT_MKR'] = 1
            
        return row

    impute_towns = impute_towns.apply(check_info,axis = 1)
    
    compare_dfs('00_impute_towns', 'impute_towns.sas7bdat', impute_towns, save_index= False)                                            #Match
        
    # Correct nights information so that it matches stay
    temp1 = ips_correct_regional_nights(impute_towns, var_stay)
    
    temp1 = temp1[['FLOW', 'SERIAL', 
              'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 
              'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8',
              'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 
              'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']].sort_values(by = var_serial)
    
    
    # Update towns info
    impute_towns_ext = impute_towns.sort_values(by = var_serial)
    #impute_towns_ext.index = range(0, len(impute_towns_ext))
    
    impute_towns_ext.update(temp1)

    compare_dfs('01_impute_towns_ext', 'impute_towns_ext.sas7bdat', impute_towns_ext, save_index= False)                                #Match
        
    seg = [0] * 4
    temp2 = [0] * 4
    temp3 = [0] * 4
    segment = [0] * 4
    cseg = [0] * 4
    trunc_segment = [0] * 4
    
    # Loop over imputation levels
    for level in range(1,maxLevel+1):
        strata = strata_levels[level-1]
        
        # Look for records that have already been uplifted
        def check_if_uplifted(row):
            if row[var_visit_wtk] != '':
                row['VISIT_WTK_NONMISS'] = 1
            else:
                row['VISIT_WTK_NONMISS'] = np.NaN
            row['VISIT_WTK_ALL'] = 1
            return row
        
        
        impute_towns_ext = impute_towns_ext.apply(check_if_uplifted, axis = 1)
        
        compare_dfs('02_impute_towns_ext_' + str(level), 'impute_towns_ext_' + str(level) + '.sas7bdat', impute_towns_ext)              # Match loop: 1
        
        # Sort impute_towns_ext by strata
        impute_towns_ext = impute_towns_ext.sort_values(by = strata)
                
        

        # Replace blank values with -1 as python drops blanks during the aggregation process.  
        impute_towns_ext[strata] = impute_towns_ext[strata].fillna(-1)
        
        # Calculate the number of records in each segment that have previously
        # been uplifted and the total number of records in each segment  
        impute_towns_ext1 = impute_towns_ext.groupby(strata)['VISIT_WTK_NONMISS'].agg({
            'VISIT_WT_COUNT' : 'count'})
        impute_towns_ext2 = impute_towns_ext.groupby(strata)['VISIT_WTK_ALL'].agg({
            'TOTAL_COUNT' : 'count'})
        
        # Flattens the column structure after adding the new columns above
        impute_towns_ext1 = impute_towns_ext1.reset_index()
        impute_towns_ext2 = impute_towns_ext2.reset_index()
        
        # Merge the two data sets generated
        seg[level-1] = pd.merge(impute_towns_ext1,impute_towns_ext2 ,on = strata, how = 'inner')
                
        # Replace the previously added -1 values with their original blank values  
        impute_towns_ext[strata] = impute_towns_ext[strata].replace(-1, np.NaN)
        
        seg[level-1][strata] = seg[level-1][strata].replace(-1, np.NaN) 
        
        compare_dfs('03_seg_'+str(level), 'seg_'+str(level)+'.sas7bdat', seg[level-1])                                                  # Match loop: 1
                
        # Calculate visit, stay and expenditure weights
        impute_towns_ext_mod = impute_towns_ext.copy()
        
        impute_towns_ext_mod['FIN'] = impute_towns_ext_mod[var_final_wt] * impute_towns_ext_mod[var_visit_wt]
        impute_towns_ext_mod['STY'] = impute_towns_ext_mod[var_stay] * impute_towns_ext_mod[var_final_wt] * impute_towns_ext_mod[var_stay_wt]
        impute_towns_ext_mod['EXP'] = impute_towns_ext_mod[var_spend] * impute_towns_ext_mod[var_final_wt] * impute_towns_ext_mod[var_expenditure_wt]
        
        compare_dfs('04_impute_towns_ext_mod_'+str(level), 'impute_towns_ext_mod_'+str(level) + '.sas7bdat', impute_towns_ext_mod)
        
        
        
        # Compute weight totals over good records
        temp2[level-1] = impute_towns_ext_mod.loc[impute_towns_ext_mod['INFO_PRESENT_MKR'] == 1]
        
        # Replace blank values with -1 as python drops blanks during the aggregation process.  
        temp2[level-1][strata] = temp2[level-1][strata].fillna(-1)
        
        temp2_count = temp2[level-1].groupby(strata)['FIN'].agg({
            'TOWN_COUNT' : 'count'})
        temp2_fin = temp2[level-1].groupby(strata)['FIN'].agg({
            'KNOWN_FINAL_WEIGHTS': 'sum'})
        temp2_sty = temp2[level-1].groupby(strata)['STY'].agg({
            'KNOWN_STAY': 'sum'})
        temp2_exp = temp2[level-1].groupby(strata)['EXP'].agg({
            'KNOWN_EXPEND': 'sum'})       
        
        temp2_count = temp2_count.reset_index()
        temp2_fin = temp2_fin.reset_index()
        temp2_sty = temp2_sty.reset_index()
        temp2_exp = temp2_exp.reset_index()
        
        temp2[level-1] = pd.merge(temp2_count,temp2_fin,on = strata, how = 'inner')
        temp2[level-1] = pd.merge(temp2[level-1],temp2_sty,on = strata, how = 'inner')
        temp2[level-1] = pd.merge(temp2[level-1],temp2_exp,on = strata, how = 'inner')
        
        temp2[level-1][strata] = temp2[level-1][strata].replace(-1, np.NaN)
        
        compare_dfs('05_temp2_'+str(level), 'temp2_'+str(level)+'.sas7bdat', temp2[level-1])              # Match Loop : 1
        
        
        
        # Compute weight totals over bad records
        temp3[level-1] = impute_towns_ext_mod[impute_towns_ext_mod['INFO_PRESENT_MKR'] == 0]
        
        # Replace blank values with -1 as python drops blanks during the aggregation process.  
        temp3[level-1][strata] = temp3[level-1][strata].fillna(-1)
        
        temp3_count = temp3[level-1].groupby(strata)['FIN'].agg({
            'NO_TOWN_COUNT' : 'count'})
        temp3_fin = temp3[level-1].groupby(strata)['FIN'].agg({
            'UNKNOWN_FINAL_WEIGHT': 'sum'})
        temp3_sty = temp3[level-1].groupby(strata)['STY'].agg({
            'UNKNOWN_STAY': 'sum'})
        temp3_exp = temp3[level-1].groupby(strata)['EXP'].agg({
            'UNKNOWN_EXPEND': 'sum'})
        
        temp3_count = temp3_count.reset_index()
        temp3_fin = temp3_fin.reset_index()
        temp3_sty = temp3_sty.reset_index()
        temp3_exp = temp3_exp.reset_index()
        
        temp3[level-1] = pd.merge(temp3_count,temp3_fin,on = strata, how = 'inner')
        temp3[level-1] = pd.merge(temp3[level-1],temp3_sty,on = strata, how = 'inner')
        temp3[level-1] = pd.merge(temp3[level-1],temp3_exp,on = strata, how = 'inner')
        
        temp3[level-1][strata] = temp3[level-1][strata].replace(-1, np.NaN)
        
        compare_dfs('06_temp3_'+str(level), 'temp3_'+str(level)+'.sas7bdat', temp3[level-1])              # Match Loop : 1
        
        
        # Sort the generated data by strata before merging them together
        seg[level-1] = seg[level-1].sort_values(by = strata)
        temp2[level-1] = temp2[level-1].sort_values(by = strata)
        temp3[level-1] = temp3[level-1].sort_values(by = strata)
         
        # Merge good and bad totals onto data
        segment[level-1] = pd.merge(seg[level-1],temp2[level-1],on = strata, how = 'left')
        segment[level-1] = pd.merge(segment[level-1],temp3[level-1],on = strata, how = 'left')
        
        # Account for missing values by setting weights to zero
        segment[level-1].loc[segment[level-1]['UNKNOWN_FINAL_WEIGHT'].isnull() , 'UNKNOWN_FINAL_WEIGHT'] = 0
        segment[level-1].loc[segment[level-1]['UNKNOWN_STAY'].isnull() , 'UNKNOWN_STAY'] = 0
        segment[level-1].loc[segment[level-1]['UNKNOWN_EXPEND'].isnull() , 'UNKNOWN_EXPEND'] = 0
        segment[level-1].loc[segment[level-1]['TOTAL_COUNT'].isnull() , 'TOTAL_COUNT'] = 0
        segment[level-1].loc[segment[level-1]['TOWN_COUNT'].isnull() , 'TOWN_COUNT'] = 0
        segment[level-1].loc[segment[level-1]['NO_TOWN_COUNT'].isnull() , 'NO_TOWN_COUNT'] = 0
        
        segment[level-1][strata] = segment[level-1][strata].fillna(-1)
        segment[level-1] = segment[level-1].sort_values(by = strata)
        segment[level-1][strata] = segment[level-1][strata].replace(-1, np.NaN)
        
        compare_dfs('07_segment_'+str(level), 'segment_'+str(level)+'_merge.sas7bdat', segment[level-1])                                    # Match Loop : 1
        
        
        
        #ALL working up to here (loop1)

        # Look for records that still need to be uplifted
        cseg[level-1] = segment[level-1].loc[segment[level-1]['TOWN_COUNT'] != segment[level-1]['VISIT_WT_COUNT']]
        
        compare_dfs('08_cseg_'+str(level), 'cseg_'+str(level)+'.sas7bdat', cseg[level-1])                                                   # Match Loop : 1
        
        
        
        record_count = len(cseg[level-1].index)
        
        if record_count > 0:
            
            # Remove invalid groups from the imputation set
            
            # Check current level, as level 4 thresholds are different to 1-3
            if level < 4:
                
                trunc_segment[level-1] = segment[level-1].loc[(segment[level-1]['VISIT_WT_COUNT'] < segment[level-1]['TOTAL_COUNT'])]
                       
                condition = ((trunc_segment[level-1]['TOWN_COUNT'] >= 20)
                             & (trunc_segment[level-1]['NO_TOWN_COUNT'] < trunc_segment[level-1]['TOWN_COUNT'])
                             & (trunc_segment[level-1]['KNOWN_EXPEND'] != 0)
                             & (trunc_segment[level-1]['KNOWN_STAY'] != 0)
                             & (trunc_segment[level-1]['KNOWN_FINAL_WEIGHTS'] != 0)
                             & (trunc_segment[level-1]['KNOWN_FINAL_WEIGHTS'].notnull())
                             & (((trunc_segment[level-1]['KNOWN_FINAL_WEIGHTS'] + trunc_segment[level-1]['UNKNOWN_FINAL_WEIGHT']) / trunc_segment[level-1]['KNOWN_FINAL_WEIGHTS']) <= 2)
                             & (((trunc_segment[level-1]['KNOWN_STAY'] + trunc_segment[level-1]['UNKNOWN_STAY']) / trunc_segment[level-1]['KNOWN_STAY']) <= 2)
                             & (((trunc_segment[level-1]['KNOWN_EXPEND'] + trunc_segment[level-1]['UNKNOWN_EXPEND']) / trunc_segment[level-1]['KNOWN_EXPEND']) <= 2))
                
                trunc_segment[level-1] = trunc_segment[level-1].loc[condition]
                
                compare_dfs('09_trunc_segment_'+str(level), 'trunc_segment_'+str(level)+'_lt4.sas7bdat', trunc_segment[level-1])           # Match Loop : 1
                pass
                
            if level > 3:
                trunc_segment[level-1] = segment[level-1].loc[(segment[level-1]['VISIT_WT_COUNT'] < segment[level-1]['TOTAL_COUNT'])]
                
                condition = ((trunc_segment[level-1]['KNOWN_EXPEND'] != 0)
                             & (trunc_segment[level-1]['KNOWN_STAY'] != 0)
                             & (trunc_segment[level-1]['KNOWN_FINAL_WEIGHTS'] != 0))
                
                
                trunc_segment[level-1] = trunc_segment[level-1].loc[condition]
                
                compare_dfs('09_trunc_segment_'+str(level), 'trunc_segment_'+str(level)+'_gt3.sas7bdat', trunc_segment[level-1])           # Match Loop : 1
                pass
            
            # Sort trunc_segment before merge
            trunc_segment[level-1] = trunc_segment[level-1].sort_values(by = strata)
            trunc_segment[level-1] = trunc_segment[level-1][strata + ['VISIT_WT_COUNT', 'TOTAL_COUNT', 'KNOWN_FINAL_WEIGHTS', 'KNOWN_STAY', 'KNOWN_EXPEND',
                                                           'UNKNOWN_FINAL_WEIGHT', 'UNKNOWN_STAY', 'UNKNOWN_EXPEND']]
            
            # Sort impute_towns_ext before merge
            impute_towns_ext = impute_towns_ext.sort_values(by = strata)
            
            # Join the known and unknown weights onto the original data
            #if(level == 1):
            impute_towns_ext = pd.merge(impute_towns_ext,trunc_segment[level-1], on=strata, how = 'left')
            #else:
            #    trunc_segment_slice = trunc_segment[level-1].drop(columns=['VISIT_WT_COUNT', 'TOTAL_COUNT'])
            #    impute_towns_ext = pd.merge(impute_towns_ext,trunc_segment_slice, on=strata, how = 'left')
            #    impute_towns_ext.update(trunc_segment[level-1])
                        
            compare_dfs('10_impute_towns_ext_trunc_seg_' + str(level), 'impute_towns_ext_trunc_seg_' + str(level) +'.sas7bdat', impute_towns_ext)              # CANT CHECK MATCH DUE TO LOOP
            
            # Calculate the revised weights
            def calculate_revised_weights(row):
                
                if row['SERIAL'] == 410049406155.00:
                    print('at record')
                
                if row['KNOWN_FINAL_WEIGHTS'] != 0 and not math.isnan(row['KNOWN_FINAL_WEIGHTS']):
                    
                    if row[var_visit_wtk] == '':
                        row[var_visit_wtk] = str(level)
                        row[var_stay_wtk] = str(level)
                        row[var_expenditure_wtk] = str(level)
                
                    
                    if row['INFO_PRESENT_MKR'] == 1 and row[var_visit_wtk] == str(level):
                        row[var_visit_wt] = row[var_visit_wt] * (row['KNOWN_FINAL_WEIGHTS'] + row['UNKNOWN_FINAL_WEIGHT']) / row['KNOWN_FINAL_WEIGHTS']
                        row[var_stay_wt] = row[var_stay_wt] * (row['KNOWN_STAY'] + row['UNKNOWN_STAY']) / row['KNOWN_STAY']
                        row[var_expenditure_wt] = row[var_expenditure_wt] * (row['KNOWN_EXPEND'] + row['UNKNOWN_EXPEND']) / row['KNOWN_EXPEND']
                
                elif row['INFO_PRESENT_MKR'] == 0:
                    row[var_visit_wt] = 0
                    row[var_stay_wt] = 0
                    row[var_expenditure_wt] = 0
                    pass
                
                return row
            
            impute_towns_ext = impute_towns_ext.apply(calculate_revised_weights, axis = 1)
            impute_towns_ext = impute_towns_ext.drop(columns=['KNOWN_FINAL_WEIGHTS', 'KNOWN_STAY', 'KNOWN_EXPEND',
                                                              'UNKNOWN_FINAL_WEIGHT', 'UNKNOWN_STAY', 'UNKNOWN_EXPEND'])
            
            
            compare_dfs('11_impute_ext_revised_weights_' + str(level), 'impute_ext_revised_weights_' + str(level)+'.sas7bdat', impute_towns_ext)      # Match Loop : 1
            pass
        # if record_count > 0
    # Loop end
    
    outputData = impute_towns_ext[[var_serial,
                                   var_visit_wt, var_stay_wt, var_expenditure_wt,
                                   var_visit_wtk, var_stay_wtk, var_expenditure_wtk,
                                   'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8',
                                   'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']]
    
    
    def round_wts(row):
        row[var_visit_wt] = round(row[var_visit_wt], 3)
        row[var_stay_wt] = round(row[var_stay_wt], 3)
        row[var_expenditure_wt] = round(row[var_expenditure_wt], 3)
        return row
    
    outputData = outputData.apply(round_wts,axis = 1)

    outputData = outputData.sort_values(by = var_serial)
    
    compare_dfs('12_output_final', 'output_final.sas7bdat', outputData)
    sys.exit()
    
    # Return the generated data frame to be appended to oracle
    return (outputData)


def calculate(intabname, outtabname, responseTable, var_serial, maxLevel,
              strataBase, var_stay, var_spend, var_final_wt, var_stay_wt,
              var_visit_wt,  var_expenditure_wt, var_stay_wtk, var_visit_wtk,
              var_expenditure_wtk, var_eligible_flag,
              strata1, strata2, strata3, strata4):
    """
    Author       : Thomas Mahoney
    Date         : 16 / 02 / 2018
    Purpose      : Calculates regional weights for IPS system.
    Parameters   : intabname - the IPS survey records for the period.
                   outtabname - dataset to hold output data
                   responseTable - SAS response table
                   var_serial - Variable holding the record serial number
                   maxLevel - the number of the last imputation level (1-up)
                   strataBase - the stem for the strata parameters
                   var_stay - the name of the stay variable
                   var_spend - the name of the spend variable
                   var_final_wt - the name of the final weight variable
                   var_stay_wt - the name of the stay weight output variable
                   var_visit_wt - the name of the visit weight output variable
                   var_expenditure_wt - the name of the spend weight output variable
                   var_stay_wtk - the name of the stay weight key output variable
                   var_visit_wtk - the name of the visit weight key output variable
                   var_expenditure_wtk - the name of the spend weight key output variable
                   var_eligible_flag - the name of the eligibility flag variable                     
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
        
    # Import data via SAS
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Regional Weights\surveydata.sas7bdat"
    df_surveydata = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)
    
    # Set all of the columns imported to uppercase
    df_surveydata.columns = df_surveydata.columns.str.upper()

    # Set up strata lists
    strata_levels = [strata1, strata2, strata3, strata4]
    
    # Calculate the unsampled weights of the imported dataset.
    print("Start - Calculate Regional Weights.")     
    output_dataframe = do_ips_regional_weight_calculation(df_surveydata, 'output', var_serial, maxLevel, strataBase, 
                                                          var_stay, var_spend, var_final_wt, var_stay_wt, 
                                                          var_visit_wt, var_expenditure_wt, var_stay_wtk, 
                                                          var_visit_wtk, var_expenditure_wtk, var_eligible_flag,
                                                          strata_levels)
    
    # Append the generated data to output tables
    cf.insert_into_table_many(outtabname, output_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Regional Weights calculation: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Regional Weights calculation.")
    cf.commit_to_audit_log("Create", "Regional", audit_message)
    print("Completed - Calculate Regional Weights.")
    

if __name__ == '__main__':
    calculate(intabname = 'SAS_SURVEY_SUBSAMPLE',
              outtabname = 'SAS_REGIONAL_IMP', 
              responseTable = 'SAS_RESPONSE', 
              var_serial = 'SERIAL', 
              maxLevel = 4,
              strataBase = 'STRATA',
              var_stay = 'STAY',
              var_spend = 'SPEND',
              var_final_wt = 'FINAL_WT',
              var_stay_wt = 'STAY_WT',
              var_visit_wt = 'VISIT_WT',
              var_expenditure_wt = 'EXPENDITURE_WT',
              var_stay_wtk = 'STAY_WTK',
              var_visit_wtk = 'VISIT_WTK',
              var_expenditure_wtk = 'EXPENDITURE_WTK',
              var_eligible_flag = 'REG_IMP_ELIGIBLE_PV',
              strata1  = ['FLOW', 
                          'PURPOSE_PV', 
                          'STAYIMPCTRYLEVEL1_PV'],
              strata2  = ['FLOW', 
                          'PURPOSE_PV', 
                          'STAYIMPCTRYLEVEL2_PV'],
              strata3  = ['FLOW', 
                          'PURPOSE_PV', 
                          'STAYIMPCTRYLEVEL3_PV'],
              strata4  = ['FLOW', 
                          'PURPOSE_PV', 
                          'STAYIMPCTRYLEVEL4_PV'])
    
    
    