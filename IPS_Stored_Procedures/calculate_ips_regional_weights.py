    
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
        if(row['info_present_mkr'] == 1):
            known_town_nk_nights=0
            nights_sum=0;
            stay_sum=0;
            
            if row['towncode1'] != 99999 and not math.isnan(row['TOWNCODE1']):
                if not math.isnan(row['NIGHTS1']):
                    nights_sum = nights_sum + row['NIGHTS1']
                else:
                    known_town_nk_nights = known_town_nk_nights + 1;
            
            
        
        
        
        return row
    
    
    output = input.apply(compute,axis = 1,args = (stay,))
    
     
    
    
    
    
    pass
    
    

def do_ips_regional_weight_calculation(inputData, outputData, var_serial, maxLevel, strataBase, 
                               var_stay, var_spend, var_final_wt, var_stay_wt, var_visit_wt, 
                               var_expenditure_wt, var_stay_wtk, var_visit_wtk, 
                               var_expenditure_wtk, var_eligible_flag):  
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
    
    impute_towns[var_stay_wt] = 1
    impute_towns[var_stay_wtk] = ''
    
    impute_towns[var_visit_wt] = 1
    impute_towns[var_visit_wtk] = ''
    
    impute_towns[var_expenditure_wt] = 1
    impute_towns[var_expenditure_wtk] = ''

    def check_info(row):
        
        if row['TOWNCODE1'] == 99999  or math.isnan(row['TOWNCODE1']): 
            row['INFO_PRESENT_MKR'] = 0
        else:
            row['INFO_PRESENT_MKR'] = 1
        return row

    impute_towns = impute_towns.apply(check_info,axis = 1)
    
    
    # Correct nights information so that it matches stay
    
    temp1 = ips_correct_regional_nights(impute_towns, var_stay)




    compare_dfs('impute_towns', 'impute_towns.sas7bdat', impute_towns)
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
              var_expenditure_wtk, var_eligible_flag):
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


    # Calculate the unsampled weights of the imported dataset.
    print("Start - Calculate Regional Weights.")     
    output_dataframe = do_ips_regional_weight_calculation(df_surveydata, 'output', var_serial, maxLevel, strataBase, 
                                                          var_stay, var_spend, var_final_wt, var_stay_wt, 
                                                          var_visit_wt, var_expenditure_wt, var_stay_wtk, 
                                                          var_visit_wtk, var_expenditure_wtk, var_eligible_flag)
     
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
              var_eligible_flag = 'REG_IMP_ELIGIBLE_PV')
    
    
    