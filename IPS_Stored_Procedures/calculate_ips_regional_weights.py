    
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


 
def compare_dfs(test_name, sas_file, df, col_list = False, save_index = True):
    
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
    
    
    # Correct nights information so that it matches stay
    temp1 = ips_correct_regional_nights(impute_towns, var_stay)
    
    temp1 = temp1[['FLOW', 'SERIAL', 
              'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 
              'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8',
              'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 
              'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']].sort_values(by = var_serial)
    
    
    temp1.index = range(0, len(temp1))
    
    # Update towns info
    impute_towns_ext = impute_towns.sort_values(by = var_serial)
    impute_towns_ext.index = range(0, len(impute_towns_ext))
    
    impute_towns_ext.update(temp1)

    compare_dfs('impute_towns_ext', 'impute_towns_ext.sas7bdat', impute_towns_ext)
    
    
    # Loop over imputation levels
    for level in range(1,maxLevel+1):
        strata = strata_levels[level-1]
        
        # Look for records that have already been uplifted
        impute_towns_ext['VISIT_WTK_ALL'] = 1
        impute_towns_ext.loc[impute_towns_ext[var_visit_wtk].isnull() , 'VISIT_WTK_NONMISS'] = 1
        
        compare_dfs('impute_towns_ext_update1', 'impute_towns_ext_update1.sas7bdat', impute_towns_ext)
           
               
        impute_towns_ext = impute_towns_ext.sort_values(by = strata)
        
        # Calculate the number of records in each segment that have previously
        # been uplifted and the total number of records in each segment */ 
        
        # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
        impute_towns_ext[strata] = impute_towns_ext[strata].fillna('NOTHING')
        
        impute_towns_ext1 = impute_towns_ext.groupby(strata)['VISIT_WTK_NONMISS'].agg({
            'VISIT_WT_COUNT' : 'count'})
        # Flattens the column structure after adding the new 'CASES' column
        impute_towns_ext1 = impute_towns_ext1.reset_index()
                
        impute_towns_ext2 = impute_towns_ext.groupby(strata)['VISIT_WTK_ALL'].agg({
            'TOTAL_COUNT' : 'count'})
        # Flattens the column structure after adding the new OOHDesignWeight and POSTWEIGHT columns
        impute_towns_ext2 = impute_towns_ext2.reset_index()
        
        seg_level = impute_towns_ext1
        
        seg_level = pd.merge(impute_towns_ext1,impute_towns_ext2 ,on = strata, how = 'inner')
        
        # Replace the previously added 'NOTHING' values with their original blank values  
        impute_towns_ext[strata] = impute_towns_ext[strata].replace('NOTHING', np.NaN)
        
        compare_dfs('seg_'+str(level), 'seg_'+str(level)+'.sas7bdat', seg_level)
        
        # Calculate visit, stay and expenditure weights
        impute_towns_ext_mod = impute_towns_ext
        
        impute_towns_ext_mod['FIN'] = impute_towns_ext_mod[var_final_wt] * impute_towns_ext_mod[var_visit_wt]
        impute_towns_ext_mod['STY'] = impute_towns_ext_mod[var_stay] * impute_towns_ext_mod[var_final_wt] * impute_towns_ext_mod[var_stay_wt]
        impute_towns_ext_mod['EXP'] = impute_towns_ext_mod[var_spend] * impute_towns_ext_mod[var_final_wt] * impute_towns_ext_mod[var_expenditure_wt]
        
        compare_dfs('impute_towns_ext_mod', 'impute_towns_ext_mod.sas7bdat', impute_towns_ext_mod)
        
        # Compute weight totals over good records
        temp2 = impute_towns_ext_mod[impute_towns_ext_mod['INFO_PRESENT_MKR'] == 1]
        
        
        # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
        temp2[strata] = impute_towns_ext[strata].fillna('NOTHING')
        
        temp2_fin = temp2.groupby(strata)['FIN'].agg({
            'TOWN_COUNT' : 'count',
            'KNOWN_FINAL_WEIGHTS': 'sum'})
        temp2_sty = temp2.groupby(strata)['STY'].agg({
            'KNOWN_STAY': 'sum'})
        temp2_exp = temp2.groupby(strata)['EXP'].agg({
            'KNOWN_EXPEND': 'sum'})       
        
        temp2_fin = temp2_fin.reset_index()
        temp2_sty = temp2_sty.reset_index()
        temp2_exp = temp2_exp.reset_index()
        
        
        temp2_merged = pd.merge(temp2_fin,temp2_sty,on = strata, how = 'inner')
        temp2_merged = pd.merge(temp2_merged,temp2_exp,on = strata, how = 'inner')
        
        compare_dfs('temp2_merged', 'temp2_1.sas7bdat', temp2_merged)
        
        
        # Compute weight totals over good records
        temp3 = impute_towns_ext_mod[impute_towns_ext_mod['INFO_PRESENT_MKR'] == 0]
        
        # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
        temp3[strata] = impute_towns_ext[strata].fillna('NOTHING')
        
        temp3_fin = temp3.groupby(strata)['FIN'].agg({
            'NO_TOWN_COUNT' : 'count',
            'UNKNOWN_FINAL_WEIGHT': 'sum'})
        temp3_sty = temp3.groupby(strata)['STY'].agg({
            'UNKNOWN_STAY': 'sum'})
        temp3_exp = temp3.groupby(strata)['EXP'].agg({
            'UNKNOWN_EXPEND': 'sum'})
        
        temp3_fin = temp3_fin.reset_index()
        temp3_sty = temp3_sty.reset_index()
        temp3_exp = temp3_exp.reset_index()
        
        temp3_merged = pd.merge(temp3_fin,temp3_sty,on = strata, how = 'inner')
        temp3_merged = pd.merge(temp3_merged,temp3_exp,on = strata, how = 'inner')
        
        compare_dfs('temp3_'+str(level), 'temp3_'+str(level)+'.sas7bdat', temp3_merged)
        
        # Replace the previously added 'NOTHING' values with their original blank values  
        seg_level[strata] = seg_level[strata].replace('NOTHING', np.NaN) 
        temp2_merged[strata] = temp2_merged[strata].replace('NOTHING', np.NaN)
        temp3_merged[strata] = temp3_merged[strata].replace('NOTHING', np.NaN)
                
        # Merge good and bad totals onto data
        seg_level = seg_level.sort_values(by = strata)
        temp2_merged = temp2_merged.sort_values(by = strata)
        temp3_merged = temp3_merged.sort_values(by = strata)
         
        segment_level = pd.merge(seg_level,temp2_merged,on = strata, how = 'left')
        segment_level = pd.merge(segment_level,temp3_merged,on = strata, how = 'left')
        
        # Account for missing values by setting weights to zero
        segment_level.loc[segment_level['UNKNOWN_FINAL_WEIGHT'].isnull() , 'UNKNOWN_FINAL_WEIGHT'] = 0
        segment_level.loc[segment_level['UNKNOWN_STAY'].isnull() , 'UNKNOWN_STAY'] = 0
        segment_level.loc[segment_level['UNKNOWN_EXPEND'].isnull() , 'UNKNOWN_EXPEND'] = 0
        segment_level.loc[segment_level['TOTAL_COUNT'].isnull() , 'TOTAL_COUNT'] = 0
        segment_level.loc[segment_level['TOWN_COUNT'].isnull() , 'TOWN_COUNT'] = 0
        segment_level.loc[segment_level['NO_TOWN_COUNT'].isnull() , 'NO_TOWN_COUNT'] = 0
        
        compare_dfs('segment_'+str(level)+'_merge', 'segment_'+str(level)+'_merge'+'.sas7bdat', segment_level)
        
        #ALL working up to here
        
        # Look for records that still need to be uplifted
        cseg_level = segment_level.loc[segment_level['TOWN_COUNT'] != segment_level['VISIT_WT_COUNT']]  #    <<<<<<<<<<<<<<<<< NOT TESTED - Home time
        
        
        
        
        sys.exit()
        
    sys.exit()






    ##END OF CALCULATION LOGGING


    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_unsampled_thresholds_check.iterrows():
        threshold_string += "___||___" \
                         + str(df_unsampled_thresholds_check.columns[0]) + " : " + str(record[0]) + " | "\
                         + str(df_unsampled_thresholds_check.columns[1]) + " : " + str(record[1]) + " | "\
                         + str(df_unsampled_thresholds_check.columns[2]) + " : " + str(record[2]) + " | "\
                         + str(df_unsampled_thresholds_check.columns[3]) + " : " + str(record[3])
                         
    # Output the values outside of the threshold to the logger - COMMENTED OUT DUE TO SIZE ISSUE?
    if len(df_unsampled_thresholds_check) > 0:
        cf.database_logger().warning('WARNING: Respondent count below minimum threshold for: ')# + str(threshold_string))
    
    
    # Return the generated data frames to be appended to oracle
    return (df_output, df_summary)


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


    print("4")
    sys.exit()
    # This was in unsampled, leaving here just incase I need it - @TM
    def num_to_string(row):
        row['UNSAMP_REGION_GRP_PV'] = str(row['UNSAMP_REGION_GRP_PV'])
        if(row['UNSAMP_REGION_GRP_PV'] == 'nan'):
            row['UNSAMP_REGION_GRP_PV'] = ' '
        return row
    
    
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
              outtabname = 'SAS_UNSAMPLED_OOH_WT', 
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
    
    
    