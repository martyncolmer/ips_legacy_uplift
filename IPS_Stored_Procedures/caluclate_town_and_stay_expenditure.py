'''
Created on 13 Mar 2018

@author: thorne1
'''
'''
Created on 7 Mar 2018

@author: thorne1
'''
import inspect
import numpy as np
import pandas as pd
import survey_support

from IPSTransformation import CommonFunctions as cf
from IPS_Unallocated_Modules import ips_impute as imp

import sys
from pprint import pprint

def do_ips_spend_imputation(df_survey_data, output, var_serialNum, var_flow
                            , var_purpose_grp, var_country_grp, var_residence
                            , var_stay, var_spend, var_final_wt, var_eligible_flag):
    """
    Author        : thorne1
    Date          : 13 Mar 2018
    Purpose       :   
    Parameters    : 
    Returns       : Dataframe  
    """

    # Do some initial setup and selection
    df_output_data = df_survey_data.copy()
    df_output_data.drop(df_output_data[df_output_data[var_eligible_flag] 
                                       != 1.0].index, inplace=True)
    df_output_data["KNOWN_LONDON_NOT_VISIT"] = 0
    df_output_data["KNOWN_LONDON_VISIT"] = 0
    df_output_data["ADE1"] = 0
    df_output_data["ADE2"] = 0
    df_output_data["RATION_L_NL_ADES"] = 0
    
#    #===========================================================================
#    #===========================================================================
##    stay_towns = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\stay_towns.sas7bdat")
##    stay_towns.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\in.csv")
##    seg1 = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\seg1.sas7bdat")
##    seg1.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\out.csv")
##    cf.beep()
##    sys.exit()
#    cf.compare_dfs("stay_towns", "stay_towns.sas7bdat", df_output_data, ["SERIAL",var_eligible_flag,"KNOWN_LONDON_NOT_VISIT", "KNOWN_LONDON_VISIT", "ADE1", "ADE2", "RATION_L_NL_ADES"])    
##    cf.beep()
##    sys.exit()
#    #===========================================================================
#    #===========================================================================
    
    # Calculate the value ade1
    # Create df where condition
    condition = (((df_output_data["TOWNCODE1"] >= 70000) & (df_output_data["TOWNCODE1"] <= 79999))
                 | ((df_output_data["TOWNCODE2"] >= 70000) & (df_output_data["TOWNCODE2"] <= 79999))
                 | ((df_output_data["TOWNCODE3"] >= 70000) & (df_output_data["TOWNCODE3"] <= 79999))
                 | ((df_output_data["TOWNCODE4"] >= 70000) & (df_output_data["TOWNCODE4"] <= 79999))
                 | ((df_output_data["TOWNCODE5"] >= 70000) & (df_output_data["TOWNCODE5"] <= 79999))
                 | ((df_output_data["TOWNCODE6"] >= 70000) & (df_output_data["TOWNCODE6"] <= 79999))
                 | ((df_output_data["TOWNCODE7"] >= 70000) & (df_output_data["TOWNCODE7"] <= 79999))
                 | ((df_output_data["TOWNCODE8"] >= 70000) & (df_output_data["TOWNCODE8"] <= 79999)))
    df_segment1 = df_output_data[[var_flow
                                  , var_purpose_grp
                                  , var_country_grp
                                  , "KNOWN_LONDON_VISIT"]].ix[(condition)]
    
    # Calculate
    df_segment1["ADE1_TEMP1"] = pd.Series(df_output_data[var_final_wt] * (df_output_data[var_spend] / df_output_data[var_stay]))
    df_segment1["ADE1_TEMP2"] = df_output_data[var_final_wt]
    
    # Group by and aggregate
    df_segment1 = df_segment1.groupby([var_flow, var_purpose_grp, var_country_grp])\
                                                 .agg({"KNOWN_LONDON_VISIT" : 'count'
                                                       ,"ADE1_TEMP1" : 'sum'
                                                       , "ADE1_TEMP2" : 'sum' })
    df_segment1["ADE1"] = df_segment1["ADE1_TEMP1"] / df_segment1["ADE1_TEMP2"]
    
    # Cleanse
    df_segment1 = df_segment1.reset_index()
    df_segment1.drop(["ADE1_TEMP1"], axis=1, inplace=True)
    df_segment1.drop(["ADE1_TEMP2"], axis=1, inplace=True)
    
    #===========================================================================
    #===========================================================================
#    sas = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\seg1.sas7bdat")
#    sas.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\seg1.csv")
#    cf.compare_dfs("seg1", "seg1.sas7bdat", df_segment1)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate the vale ade2
    # Create df where condition
    condition = (((df_output_data["TOWNCODE1"] >= 70000) & (df_output_data["TOWNCODE1"] <= 79999))
                 | ((df_output_data["TOWNCODE2"] >= 70000) & (df_output_data["TOWNCODE2"] <= 79999))
                 | ((df_output_data["TOWNCODE3"] >= 70000) & (df_output_data["TOWNCODE3"] <= 79999))
                 | ((df_output_data["TOWNCODE4"] >= 70000) & (df_output_data["TOWNCODE4"] <= 79999))
                 | ((df_output_data["TOWNCODE5"] >= 70000) & (df_output_data["TOWNCODE5"] <= 79999))
                 | ((df_output_data["TOWNCODE6"] >= 70000) & (df_output_data["TOWNCODE6"] <= 79999))
                 | ((df_output_data["TOWNCODE7"] >= 70000) & (df_output_data["TOWNCODE7"] <= 79999))
                 | ((df_output_data["TOWNCODE8"] >= 70000) & (df_output_data["TOWNCODE8"] <= 79999)))
    df_segment2 = df_output_data[[var_flow
                                  , var_purpose_grp
                                  , var_country_grp
                                  , "KNOWN_LONDON_NOT_VISIT"]].ix[(condition)]
    
    # Calculate
    df_segment2["ADE2_TEMP1"] = pd.Series(df_output_data[var_final_wt] * (df_output_data[var_spend] / df_output_data[var_stay]))
    df_segment2["ADE2_TEMP2"] = df_output_data[var_final_wt]
    
    # Group by and aggregate
    df_segment2 = df_segment2.groupby([var_flow, var_purpose_grp, var_country_grp])\
                                                 .agg({"KNOWN_LONDON_NOT_VISIT" : 'count'
                                                       ,"ADE2_TEMP1" : 'sum'
                                                       , "ADE2_TEMP2" : 'sum' })
    df_segment2["ADE2"] = df_segment2["ADE2_TEMP1"] / df_segment2["ADE2_TEMP2"]
    
    # Cleanse
    df_segment2 = df_segment2.reset_index()
    df_segment2.drop(["ADE2_TEMP1"], axis=1, inplace=True)
    df_segment2.drop(["ADE2_TEMP2"], axis=1, inplace=True)
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("seg2", "seg2.sas7bdat", df_segment2)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Merge the files containing ade1 and ade2
    df_segment_merge = pd.merge(df_segment1, df_segment2, on=[var_flow
                                                              , var_purpose_grp 
                                                              ,var_country_grp]
                                , how = 'left')

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("segment", "segment.sas7bdat", df_segment_merge)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
#    
#    
#    sas = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\stay_towns_update1.sas7bdat")
#    sas.columns = sas.columns.str.upper()
#    sas.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\stay_towns_update1.csv")
#    cf.beep()
#    sys.exit()
    
    # Update the extract with ade1, ade2 and counts
    df_extract_update = pd.merge(df_output_data, df_segment_merge, on=[var_flow
                                                              , var_purpose_grp 
                                                              ,var_country_grp], how = 'left')
    df_extract_update.rename(columns={"KNOWN_LONDON_VISIT_y": "KNOWN_LONDON_VISIT"
                                   ,"KNOWN_LONDON_NOT_VISIT_y": "KNOWN_LONDON_NOT_VISIT"
                                   ,"ADE1_y": "ADE1"
                                   ,"ADE2_y" : "ADE2"}, inplace=True)
    df_extract_update.drop(["KNOWN_LONDON_VISIT_x"
                            ,"KNOWN_LONDON_NOT_VISIT_x"
                            ,"ADE1_x"
                            ,"ADE2_x"], axis=1, inplace=True)
    
    #===========================================================================
    #===========================================================================
    # cf.compare_dfs("stay_towns_update1", "stay_towns_update1.sas7bdat", df_extract_update, ["SERIAL", "KNOWN_LONDON_VISIT", "KNOWN_LONDON_NOT_VISIT", "ADE1", "ADE2"])    
    # cf.beep()
    # sys.exit()
    #===========================================================================
    #===========================================================================
      
    # Calculate ade1 with out flow
    # MEDIUM NOPE
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("temp_london", "temp_london.sas7bdat", df_temp_london)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate ade2 without flow
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("temp_london_other", "temp_london_other.sas7bdat", df_temp_london2)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Merge files containing ade1 ade2
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("london", "london.sas7bdat", df_london)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Update extract with ade1 ade2 where not already set
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update2", "stay_towns_update2.sas7bdat", df_stay_towns2)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================    
    
    # SAS HAS NO COMMENT - ET
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update3", "stay_towns_update3.sas7bdat", df_stay_towns3)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #=========================================================================== 
    
    # Calculate ratio london to not london
    # EASY-PEASY
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update4", "stay_towns_update4.sas7bdat", df_stay_towns4)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #=========================================================================== 
    
    # Calculate number of nights in london and number of nights outside london
    # BIG IF STATEMENT - THINK ABOUT LOOPING?
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update5", "stay_towns_update5.sas7bdat", df_stay_towns5)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================

    # Calculate spends
    # AWFUL IF STATEMENT
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update6", "stay_towns_update6.sas7bdat", df_stay_towns6)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Finish calculating spends
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update7", "stay_towns_update7.sas7bdat", df_stay_towns7)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Set markers to indicate london visits or outside
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns99", "stay_towns99.sas7bdat", df_stay_towns99)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Create new variables for expenditure and stay
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns98", "stay_towns98.sas7bdat", df_stay_towns98)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Sum spend and stay
    pass

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_tot", "stay_tot.sas7bdat", df_stay_totals)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    



    




    
def calculate(processname, gparam_table_name, input, output, responseTable
              , var_serialNum, var_flow, var_purpose_grp, var_country_grp
              , var_residence, var_stay, var_spend, var_final_wt, var_eligible_flag):
    """
    Author        : thorne1
    Date          : 13 Mar 2018
    Purpose       :   
    Parameters    : 
    Returns       : N/A  
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    logger = cf.database_logger()
    
    # Import data via SAS
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\input_townspend.sas7bdat"
    df_survey_data = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(input_table_name)
    
    # Set all of the columns imported to uppercase
    df_survey_data.columns = df_survey_data.columns.str.upper()

    # Calculate the Air miles values of the imported data set.
    output_dataframe = do_ips_spend_imputation(df_survey_data, output, var_serialNum
                                               , var_flow, var_purpose_grp
                                               , var_country_grp, var_residence
                                               , var_stay, var_spend, var_final_wt
                                               , var_eligible_flag) 
    
    # Append the generated data to output tables
    cf.insert_into_table_many(output, output_dataframe)
    
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Town and Stay Imputation: %s()" % function_name
    
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    logger.info("SUCCESS - Completed Town and Stay Imputation.")
    cf.commit_to_audit_log("Create", "Town and Stay Imputation", audit_message)
    

if __name__ == '__main__':
    calculate(processname = "Foundation/ips/town_and_stay_imp_sp"
              , gparam_table_name = "ORA_DATA.SAS_PARAMETERS"
              , input = "SAS_SURVEY_SUBSAMPLE"
              , output = "SAS_TOWN_STAY_IMP"
              , responseTable = "SAS_RESPONSE"
              , var_serialNum = "SERIAL"
              , var_flow = "FLOW"
              , var_purpose_grp = "PURPOSE_PV"
              , var_country_grp = "STAYIMPCTRYLEVEL4_PV"
              , var_residence = "RESIDENCE"
              , var_stay = "STAY"
              , var_spend = "SPEND"
              , var_final_wt = "FINAL_WT"
              , var_eligible_flag = "TOWN_IMP_ELIGIBLE_PV") 