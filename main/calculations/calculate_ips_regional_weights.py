import inspect
import math
import numpy as np
import pandas as pd
import survey_support
from main.io import CommonFunctions as cf


def ips_correct_regional_nights(df_input, stay):
    """
    Author       : Thomas Mahoney
    Date         : 12 / 03 / 2018
    Purpose      : Corrects the regional nights data.
    Parameters   : df_input - the IPS survey records for the period.
                   stay - the stay column name
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    # Adjust regional night figures so that they match overall stay
    def compute(row):
        if row['INFO_PRESENT_MKR'] == 1:
            known_town_nk_nights = 0
            nights_sum = 0
            stay_sum = 0
            
            # Compute nights_sum and known_town_nk_nights for this record
            for x in range(1, 9):
                if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]):
                    if not math.isnan(row['NIGHTS' + str(x)]):
                        nights_sum = nights_sum + row['NIGHTS' + str(x)]
                    else:
                        known_town_nk_nights = known_town_nk_nights + 1

            if known_town_nk_nights == 0:
                # Check if sum of nights is not equal to stay
                if nights_sum != row[stay]:
                    stay_sum = (row[stay]/nights_sum)
                    
                    for x in range(1, 9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]):
                            row['NIGHTS' + str(x)] = row['NIGHTS' + str(x)] * stay_sum
                            row['STAY' + str(x) + 'K'] = 'K'
            else:
                # If town has known code add stay to total nights_sum
                # if town is null adds 1 to unknown
                if nights_sum >= row[stay]:
                    for x in range(1, 9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]) and math.isnan(row['NIGHTS' + str(x)]):
                            row['NIGHTS' + str(x)] = 1
                            nights_sum = nights_sum + row['NIGHTS' + str(x)]
                    
                    # Calculate nights uplift factor
                    stay_sum = (row[stay]/nights_sum)
            
                    for x in range(1, 9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]):
                            row['NIGHTS' + str(x)] = row['NIGHTS' + str(x)] * stay_sum
                            row['STAY' + str(x) + 'K'] = 'L'
                            
                else:
                    for x in range(1, 9):
                        if row['TOWNCODE' + str(x)] != 99999 and not math.isnan(row['TOWNCODE' + str(x)]) and math.isnan(row['NIGHTS' + str(x)]):
                            row['NIGHTS' + str(x)] = (row[stay] - nights_sum) / known_town_nk_nights
                            row['STAY' + str(x) + 'K'] = 'M'

        return row
    
    df_output = df_input.apply(compute, axis=1)
    
    return df_output
    

def do_ips_regional_weight_calculation(df_input_data, var_serial, max_level,
                                       var_stay, var_spend, var_final_wt, var_stay_wt, var_visit_wt,
                                       var_expenditure_wt, var_stay_wtk, var_visit_wtk,
                                       var_expenditure_wtk, var_eligible_flag, strata_levels):
    """
    Author       : Thomas Mahoney
    Date         : 12 / 03 / 2018
    Purpose      : Calculates regional weights for IPS.
    Parameters   : df_input_data - the IPS survey records for the period.
                   summary - dataset to hold summary output
                   var_serial - Variable holding the record serial number
                   maxLevel - the number of the last imputation level (1-up)
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

    # Extract only eligible rows
    df_impute_towns = df_input_data[df_input_data[var_eligible_flag] == 1]

    # Set initial values for wt and wkt columns
    df_impute_towns.loc[:, var_stay_wt] = 1
    df_impute_towns.loc[:, var_stay_wtk] = ''
    
    df_impute_towns.loc[:, var_visit_wt] = 1
    df_impute_towns.loc[:, var_visit_wtk] = ''
    
    df_impute_towns.loc[:, var_expenditure_wt] = 1
    df_impute_towns.loc[:, var_expenditure_wtk] = ''

    # Check if towncode information is present for the input data
    def check_info(row):
        
        if row['TOWNCODE1'] == 99999 or math.isnan(row['TOWNCODE1']):
            row['INFO_PRESENT_MKR'] = 0
        else:
            row['INFO_PRESENT_MKR'] = 1
            
        return row

    df_impute_towns = df_impute_towns.apply(check_info, axis=1)

    # Correct nights information so that it matches stay
    df_temp1 = ips_correct_regional_nights(df_impute_towns, var_stay)
    
    # Extract the corrected data and sort
    df_temp1 = df_temp1[['FLOW', 'SERIAL', 
                         'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4',
                         'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8',
                         'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K',
                         'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']].sort_values(by=var_serial)
    
    df_impute_towns_ext = df_impute_towns.sort_values(by=var_serial)
    
    # Update df_impute_towns_ext info with the corrected data.
    df_impute_towns_ext.update(df_temp1)

    # Generate lists to hold the loop data frames
    seg = [0] * 4
    df_temp2 = [0] * 4
    df_temp3 = [0] * 4
    segment = [0] * 4
    cseg = [0] * 4
    trunc_segment = [0] * 4
    
    # Loop over imputation levels
    for level in range(1, max_level + 1):
        strata = strata_levels[level-1]
        
        # Look for records that have already been uplifted and mark appropriately 
        def check_if_uplifted(row):
            if row[var_visit_wtk] != '':
                row['VISIT_WTK_NONMISS'] = 1
            else:
                row['VISIT_WTK_NONMISS'] = np.NaN
            row['VISIT_WTK_ALL'] = 1
            return row
        
        df_impute_towns_ext = df_impute_towns_ext.apply(check_if_uplifted, axis=1)

        # Sort df_impute_towns_ext by strata
        df_impute_towns_ext = df_impute_towns_ext.sort_values(by=strata)
        
        # Replace blank values with -1 as python drops blanks during the aggregation process.  
        df_impute_towns_ext[strata] = df_impute_towns_ext[strata].fillna(-1)
        
        # Calculate the number of records in each segment that have previously
        # been uplifted and the total number of records in each segment  
        df_impute_towns_ext1 = df_impute_towns_ext.groupby(strata)['VISIT_WTK_NONMISS'].agg({
            'VISIT_WT_COUNT': 'count'})
        df_impute_towns_ext2 = df_impute_towns_ext.groupby(strata)['VISIT_WTK_ALL'].agg({
            'TOTAL_COUNT': 'count'})
        
        # Flatten the column structure after adding the new columns above
        df_impute_towns_ext1 = df_impute_towns_ext1.reset_index()
        df_impute_towns_ext2 = df_impute_towns_ext2.reset_index()
        
        # Merge the two data sets generated
        seg[level-1] = pd.merge(df_impute_towns_ext1, df_impute_towns_ext2, on=strata, how='inner')
                
        # Replace the previously added -1 values with their original blank values  
        df_impute_towns_ext[strata] = df_impute_towns_ext[strata].replace(-1, np.NaN)
        seg[level-1][strata] = seg[level-1][strata].replace(-1, np.NaN) 

        # Copy the data and calculate the visit, stay and expenditure weights
        df_impute_towns_ext_mod = df_impute_towns_ext.copy()
        
        df_impute_towns_ext_mod['FIN'] = df_impute_towns_ext_mod[var_final_wt] * df_impute_towns_ext_mod[var_visit_wt]
        df_impute_towns_ext_mod['STY'] = df_impute_towns_ext_mod[var_stay] * df_impute_towns_ext_mod[var_final_wt] * df_impute_towns_ext_mod[var_stay_wt]
        df_impute_towns_ext_mod['EXP'] = df_impute_towns_ext_mod[var_spend] * df_impute_towns_ext_mod[var_final_wt] * df_impute_towns_ext_mod[var_expenditure_wt]

        # Compute weight totals over good records
        df_temp2[level-1] = df_impute_towns_ext_mod.loc[df_impute_towns_ext_mod['INFO_PRESENT_MKR'] == 1]
        
        # Replace blank values with -1 as python drops blanks during the aggregation process.  
        df_temp2[level-1][strata] = df_temp2[level-1][strata].fillna(-1)
        
        df_temp2_count = df_temp2[level-1].groupby(strata)['FIN'].agg({
            'TOWN_COUNT': 'count'})
        df_temp2_fin = df_temp2[level-1].groupby(strata)['FIN'].agg({
            'KNOWN_FINAL_WEIGHTS': 'sum'})
        df_temp2_sty = df_temp2[level-1].groupby(strata)['STY'].agg({
            'KNOWN_STAY': 'sum'})
        df_temp2_exp = df_temp2[level-1].groupby(strata)['EXP'].agg({
            'KNOWN_EXPEND': 'sum'})       
        
        # Flatten the column structure after generating the new columns above
        df_temp2_count = df_temp2_count.reset_index()
        df_temp2_fin = df_temp2_fin.reset_index()
        df_temp2_sty = df_temp2_sty.reset_index()
        df_temp2_exp = df_temp2_exp.reset_index()
        
        # Merge the generated values into one data frame
        df_temp2[level-1] = pd.merge(df_temp2_count, df_temp2_fin, on=strata, how='inner')
        df_temp2[level-1] = pd.merge(df_temp2[level-1], df_temp2_sty, on=strata, how='inner')
        df_temp2[level-1] = pd.merge(df_temp2[level-1], df_temp2_exp, on=strata, how='inner')
        
        # Replace the previously added -1 values with their original blank values  
        df_temp2[level-1][strata] = df_temp2[level-1][strata].replace(-1, np.NaN)

        # Compute weight totals over bad records
        df_temp3[level-1] = df_impute_towns_ext_mod[df_impute_towns_ext_mod['INFO_PRESENT_MKR'] == 0]
        
        # Replace blank values with -1 as python drops blanks during the aggregation process.  
        df_temp3[level-1][strata] = df_temp3[level-1][strata].fillna(-1)
        
        df_temp3_count = df_temp3[level-1].groupby(strata)['FIN'].agg({
            'NO_TOWN_COUNT': 'count'})
        df_temp3_fin = df_temp3[level-1].groupby(strata)['FIN'].agg({
            'UNKNOWN_FINAL_WEIGHT': 'sum'})
        df_temp3_sty = df_temp3[level-1].groupby(strata)['STY'].agg({
            'UNKNOWN_STAY': 'sum'})
        df_temp3_exp = df_temp3[level-1].groupby(strata)['EXP'].agg({
            'UNKNOWN_EXPEND': 'sum'})
        
        # Flatten the column structure after generating the new columns above
        df_temp3_count = df_temp3_count.reset_index()
        df_temp3_fin = df_temp3_fin.reset_index()
        df_temp3_sty = df_temp3_sty.reset_index()
        df_temp3_exp = df_temp3_exp.reset_index()
        
        # Merge the generated values into one data frame
        df_temp3[level-1] = pd.merge(df_temp3_count, df_temp3_fin, on=strata, how='inner')
        df_temp3[level-1] = pd.merge(df_temp3[level-1], df_temp3_sty, on=strata, how='inner')
        df_temp3[level-1] = pd.merge(df_temp3[level-1], df_temp3_exp, on=strata, how='inner')
        
        # Replace the previously added -1 values with their original blank values  
        df_temp3[level-1][strata] = df_temp3[level-1][strata].replace(-1, np.NaN)

        # Sort the generated data frames by strata before merging them together
        seg[level-1] = seg[level-1].sort_values(by=strata)
        df_temp2[level-1] = df_temp2[level-1].sort_values(by=strata)
        df_temp3[level-1] = df_temp3[level-1].sort_values(by=strata)
         
        # Merge good and bad totals into the data
        segment[level-1] = pd.merge(seg[level-1], df_temp2[level-1], on=strata, how='left')
        segment[level-1] = pd.merge(segment[level-1], df_temp3[level-1], on=strata, how='left')
        
        # Account for missing values by setting weights to zero
        segment[level-1].loc[segment[level-1]['UNKNOWN_FINAL_WEIGHT'].isnull(), 'UNKNOWN_FINAL_WEIGHT'] = 0
        segment[level-1].loc[segment[level-1]['UNKNOWN_STAY'].isnull(), 'UNKNOWN_STAY'] = 0
        segment[level-1].loc[segment[level-1]['UNKNOWN_EXPEND'].isnull(), 'UNKNOWN_EXPEND'] = 0
        segment[level-1].loc[segment[level-1]['TOTAL_COUNT'].isnull(), 'TOTAL_COUNT'] = 0
        segment[level-1].loc[segment[level-1]['TOWN_COUNT'].isnull(), 'TOWN_COUNT'] = 0
        segment[level-1].loc[segment[level-1]['NO_TOWN_COUNT'].isnull(), 'NO_TOWN_COUNT'] = 0
        
        # Replace blank values with -1 and sort the data by strata
        segment[level-1][strata] = segment[level-1][strata].fillna(-1)
        segment[level-1] = segment[level-1].sort_values(by=strata)
        
        # Replace the previously added -1 values with their original blank values  
        segment[level-1][strata] = segment[level-1][strata].replace(-1, np.NaN)

        # Look for records that still need to be uplifted
        cseg[level-1] = segment[level-1].loc[segment[level-1]['TOWN_COUNT'] != segment[level-1]['VISIT_WT_COUNT']]
        
        # Count the number of records found that still need uplifting
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

            if level > 3:
                trunc_segment[level-1] = segment[level-1].loc[(segment[level-1]['VISIT_WT_COUNT'] < segment[level-1]['TOTAL_COUNT'])]
                
                condition = ((trunc_segment[level-1]['KNOWN_EXPEND'] != 0)
                             & (trunc_segment[level-1]['KNOWN_STAY'] != 0)
                             & (trunc_segment[level-1]['KNOWN_FINAL_WEIGHTS'] != 0))

                trunc_segment[level-1] = trunc_segment[level-1].loc[condition]

            # Sort trunc_segment before merging into the df_impute_towns_ext data frame
            trunc_segment[level-1] = trunc_segment[level-1].sort_values(by=strata)
            
            # Select data to be merged
            trunc_segment[level-1] = trunc_segment[level-1][strata + ['VISIT_WT_COUNT', 'TOTAL_COUNT',
                                                                      'KNOWN_FINAL_WEIGHTS', 'KNOWN_STAY',
                                                                      'KNOWN_EXPEND', 'UNKNOWN_FINAL_WEIGHT',
                                                                      'UNKNOWN_STAY', 'UNKNOWN_EXPEND']]
            
            # Sort df_impute_towns_ext before merge
            df_impute_towns_ext = df_impute_towns_ext.sort_values(by=strata)
            
            # Join the known and unknown weights onto the original data
            df_impute_towns_ext = pd.merge(df_impute_towns_ext, trunc_segment[level-1], on=strata, how='left')

            # Calculate the revised weights
            def calculate_revised_weights(row):
                                
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
            
            df_impute_towns_ext = df_impute_towns_ext.apply(calculate_revised_weights, axis=1)
            
            # Drop the no longer needed columns
            df_impute_towns_ext = df_impute_towns_ext.drop(columns=['KNOWN_FINAL_WEIGHTS', 'KNOWN_STAY',
                                                                    'KNOWN_EXPEND', 'UNKNOWN_FINAL_WEIGHT',
                                                                    'UNKNOWN_STAY', 'UNKNOWN_EXPEND'])
            
        # if record_count > 0
        
    # Loop end
    
    # Extract the required data from the looped dataset
    df_output_data = df_impute_towns_ext[[var_serial,
                                          var_visit_wt, var_stay_wt, var_expenditure_wt,
                                          var_visit_wtk, var_stay_wtk, var_expenditure_wtk,
                                          'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4',
                                          'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8',
                                          'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K',
                                          'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']]
    
    # Round the generated weights
    def round_wts(row):
        row[var_visit_wt] = round(row[var_visit_wt], 3)
        row[var_stay_wt] = round(row[var_stay_wt], 3)
        row[var_expenditure_wt] = round(row[var_expenditure_wt], 3)
        return row
    
    df_output_data = df_output_data.apply(round_wts, axis=1)
    
    # Fills blanks in the generated columns to be of type float (NIGHTS#) or string (STAY#K)
    df_output_data[['NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8']] = \
               df_output_data[['NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8']].fillna(np.NaN)
    df_output_data[['STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']] = \
               df_output_data[['STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K']].fillna('')
    
    # Sort the output data frame
    df_output_data = df_output_data.sort_values(by=var_serial)
    
    # Return the generated data frame to be appended to oracle
    return df_output_data


def calculate(in_table_name, out_table_name, response_table, var_serial, max_level,
              var_stay, var_spend, var_final_wt, var_stay_wt,
              var_visit_wt, var_expenditure_wt, var_stay_wtk, var_visit_wtk,
              var_expenditure_wtk, var_eligible_flag,
              strata1, strata2, strata3, strata4):
    """
    Author       : Thomas Mahoney
    Date         : 16 / 02 / 2018
    Purpose      : Calculates regional weights for IPS system.
    Parameters   : in_table_name - the IPS survey records for the period.
                   out_table_name - dataset to hold output data
                   response_table - SAS response table
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
    df_survey_data = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    # df_survey_data = cf.get_table_values(in_table_name)
    
    # Set all of the columns imported to uppercase
    df_survey_data.columns = df_survey_data.columns.str.upper()

    # Set up strata lists
    strata_levels = [strata1, strata2, strata3, strata4]
    
    # Calculate the unsampled weights of the imported dataset.
    print("Start - Calculate Regional Weights.")     
    output_dataframe = do_ips_regional_weight_calculation(df_survey_data, var_serial, max_level,
                                                          var_stay, var_spend, var_final_wt, var_stay_wt,
                                                          var_visit_wt, var_expenditure_wt, var_stay_wtk,
                                                          var_visit_wtk, var_expenditure_wtk, var_eligible_flag,
                                                          strata_levels)

    cf.insert_dataframe_into_table(out_table_name, output_dataframe)

    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Regional Weights calculation: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Regional Weights calculation.")
    cf.commit_to_audit_log("Create", "Regional", audit_message)
    print("Completed - Calculate Regional Weights.")
    

if __name__ == '__main__':
    calculate(in_table_name='SAS_SURVEY_SUBSAMPLE',
              out_table_name='SAS_REGIONAL_IMP',
              response_table='SAS_RESPONSE',
              var_serial='SERIAL',
              max_level=4,
              var_stay='STAY',
              var_spend='SPEND',
              var_final_wt='FINAL_WT',
              var_stay_wt='STAY_WT',
              var_visit_wt='VISIT_WT',
              var_expenditure_wt='EXPENDITURE_WT',
              var_stay_wtk='STAY_WTK',
              var_visit_wtk='VISIT_WTK',
              var_expenditure_wtk='EXPENDITURE_WTK',
              var_eligible_flag='REG_IMP_ELIGIBLE_PV',
              strata1=['FLOW',
                        'PURPOSE_PV',
                        'STAYIMPCTRYLEVEL1_PV'],
              strata2=['FLOW',
                       'PURPOSE_PV',
                       'STAYIMPCTRYLEVEL2_PV'],
              strata3=['FLOW',
                       'PURPOSE_PV',
                       'STAYIMPCTRYLEVEL3_PV'],
              strata4=['FLOW',
                       'PURPOSE_PV',
                       'STAYIMPCTRYLEVEL4_PV'])
