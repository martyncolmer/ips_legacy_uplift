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
#
#    cf.compare_dfs("stay_towns", "stay_towns.sas7bdat", df_output_data, ["SERIAL",var_eligible_flag,"KNOWN_LONDON_NOT_VISIT", "KNOWN_LONDON_VISIT", "ADE1", "ADE2", "RATION_L_NL_ADES"])    
##    cf.beep()
##    sys.exit()
#    #===========================================================================
#    #===========================================================================
    
    # Calculate the value ade1
    # Create df where condition
    towncode_condition = (((df_output_data["TOWNCODE1"] >= 70000) & (df_output_data["TOWNCODE1"] <= 79999))
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
                                  , "KNOWN_LONDON_VISIT"]].ix[(towncode_condition)]
    
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
    df_segment2 = df_output_data[[var_flow
                                  , var_purpose_grp
                                  , var_country_grp
                                  , "KNOWN_LONDON_NOT_VISIT"]].ix[(towncode_condition)]
    
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
#    cf.compare_dfs("stay_towns_update1", "stay_towns_update1.sas7bdat", df_extract_update, ["SERIAL", "KNOWN_LONDON_VISIT", "KNOWN_LONDON_NOT_VISIT", "ADE1", "ADE2"])    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
      
    # Calculate ade1 with out flow
    df_temp_london = df_output_data[[var_purpose_grp
                                  , var_country_grp]].ix[(towncode_condition)]
    
    # Calculate
    df_temp_london["ADE1_TEMP1"] = pd.Series((df_output_data[var_final_wt] * df_output_data[var_spend]) / (df_output_data[var_stay]))
    df_temp_london["ADE1_TEMP2"] = df_output_data[var_final_wt]
    
    # Group by and aggregate
    df_temp_london = df_temp_london.groupby([var_purpose_grp, var_country_grp])\
                                                 .agg({"ADE1_TEMP1" : 'sum'
                                                       , "ADE1_TEMP2" : 'sum' })
    df_temp_london["ADE1"] = df_temp_london["ADE1_TEMP1"] / df_temp_london["ADE1_TEMP2"]
    
    # Cleanse
    df_temp_london = df_temp_london.reset_index()
    df_temp_london.drop(["ADE1_TEMP1"], axis=1, inplace=True)
    df_temp_london.drop(["ADE1_TEMP2"], axis=1, inplace=True)
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("temp_london", "temp_london.sas7bdat", df_temp_london)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate ade2 without flow
    df_temp_london2 = df_output_data[[var_purpose_grp
                                  , var_country_grp]].ix[(towncode_condition)]
    
    # Calculate
    df_temp_london2["ADE2_TEMP1"] = (pd.Series((df_output_data[var_final_wt] 
                                                * df_output_data[var_spend]) 
                                               / (df_output_data[var_stay])))
    df_temp_london2["ADE2_TEMP2"] = df_output_data[var_final_wt]
    
    # Group by and aggregate
    df_temp_london2 = df_temp_london2.groupby([var_purpose_grp, var_country_grp])\
                                                 .agg({"ADE2_TEMP1" : 'sum'
                                                       , "ADE2_TEMP2" : 'sum' })
    df_temp_london2["ADE2"] = df_temp_london2["ADE2_TEMP1"] / df_temp_london2["ADE2_TEMP2"]
    
    # Cleanse
    df_temp_london2 = df_temp_london2.reset_index()
    df_temp_london2.drop(["ADE2_TEMP1"], axis=1, inplace=True)
    df_temp_london2.drop(["ADE2_TEMP2"], axis=1, inplace=True)

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("temp_london_other", "temp_london_other.sas7bdat", df_temp_london2)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Merge files containing ade1 ade2
    df_london = pd.merge(df_temp_london, df_temp_london2, on=[var_purpose_grp 
                                                              ,var_country_grp]
                                , how = 'left')
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("london", "london.sas7bdat", df_london)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Update extract with ade1 ade2 where not already set
    df_stay_towns2 = pd.merge(df_extract_update, df_london, on=[var_purpose_grp
                                                                ,var_country_grp], how = 'left')
    
    df_stay_towns2.rename(columns={"ADE1_x": "ADE1"
                                   ,"ADE2_x" : "ADE2"}, inplace=True)
    df_stay_towns2.drop(["ADE1_y"
                            ,"ADE2_y"], axis=1, inplace=True)

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns_update2", "stay_towns_update2.sas7bdat", df_stay_towns2, ["SERIAL", "ADE1"]) 
#    cf.compare_dfs("stay_towns_update3", "stay_towns_update3.sas7bdat", df_stay_towns2, ["SERIAL", "ADE2"])   
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate ratio london to not london
    df_stay_towns4 = df_stay_towns2.copy()
    
    df_stay_towns4.loc[((df_stay_towns4["ADE1"] != 0) & (df_stay_towns4["ADE2"] != 0)), 
                       "RATION_L_NL_ADES"] = (df_stay_towns4["ADE1"]/df_stay_towns4["ADE2"])

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns_update4", "stay_towns_update4.sas7bdat", df_stay_towns4, ["SERIAL", "RATION_L_NL_ADES"])    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #=========================================================================== 
    
    # Calculate number of nights in london and number of nights outside london
#    df_stay_towns4.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\df_stay_towns4_py.csv")
    df_stay_towns5 = df_stay_towns4.copy()
    df_stay_towns5["NIGHTS_IN_LONDON"] = 0
    df_stay_towns5["NIGHTS_NOT_LONDON"] = 0
    
    for count in range(1,9):  
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        
        condition1 = ((df_stay_towns5[towncode+str(count)] >= 70000) & (df_stay_towns5[towncode+str(count)] <= 79999))
        condition2 = ((df_stay_towns5[towncode+str(count)] <= 69999) | (df_stay_towns5[towncode+str(count)] >= 80000))

        df_stay_towns5.loc[(condition1), "NIGHTS_IN_LONDON"] = (df_stay_towns5["NIGHTS_IN_LONDON"] 
                                               + df_stay_towns5[nights+str(count)])
        df_stay_towns5.loc[(condition2), "NIGHTS_NOT_LONDON"] = (df_stay_towns5["NIGHTS_NOT_LONDON"] 
                                               + df_stay_towns5[nights+str(count)])
        
#        df_stay_towns5["NOTCOUNT"] = np.where(df_stay_towns5["NIGHTS_NOT_LONDON"] != np.NaN
#                                          , df_stay_towns5["NIGHTS_NOT_LONDON"], np.NaN)
        

#    df_stay_towns5["INCOUNT"] = 0
#    df_stay_towns5["NOTCOUNT"] = 0
#    nights = "NIGHTS"
#    for count in range(1,9):
#        df_stay_towns5[nights+str(count)].fillna(0, inplace=True)
#    
#    def another_selection(row):
#        
#        if (row["NIGHTS1"] != np.NaN):
#            if ((row["TOWNCODE1"] >= 70000) & (row["TOWNCODE1"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS1"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS1"]
#            
#        if (row["NIGHTS2"] != np.NaN):
#            if ((row["TOWNCODE2"] >= 70000) & (row["TOWNCODE2"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS2"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS2"]
#        
#        if (row["NIGHTS3"] != np.NaN):
#            if ((row["TOWNCODE3"] >= 70000) & (row["TOWNCODE3"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS3"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS3"]
#        
#        if (row["NIGHTS4"] != np.NaN):
#            if ((row["TOWNCODE4"] >= 70000) & (row["TOWNCODE4"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS4"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS4"]
#        
#        if (row["NIGHTS5"] != np.NaN):
#            if ((row["TOWNCODE5"] >= 70000) & (row["TOWNCODE5"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS5"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS5"]
#        
#        if (row["NIGHTS6"] != np.NaN):
#            if ((row["TOWNCODE6"] >= 70000) & (row["TOWNCODE6"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS6"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS6"]
#        
#        if (row["NIGHTS7"] != np.NaN):
#            if ((row["TOWNCODE7"] >= 70000) & (row["TOWNCODE7"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS7"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS7"]
#        
#        if (row["NIGHTS8"] != np.NaN):
#            if ((row["TOWNCODE8"] >= 70000) & (row["TOWNCODE8"] <= 79999)):
#                row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row["NIGHTS8"]
#            else:
#                row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row["NIGHTS8"]
#             
#        return row
#
#    def another_selection(row):
#        nights = "NIGHTS"
#        towncode = "TOWNCODE"
#        
#        for count in range(1,9):
#            if row[nights+str(count)] != np.NaN:
#                if ((row[towncode+str(count)] >= 70000) & (row[towncode+str(count)] <= 79999)):
#                    row["NIGHTS_IN_LONDON"] = row["INCOUNT"] + row[nights+str(count)]
#                else:
#                    row["NIGHTS_NOT_LONDON"] = row["NOTCOUNT"] + row[nights+str(count)]
#        
#        return row
#    
#    df_stay_towns5 = df_stay_towns5.apply(another_selection, axis=1)

    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update5", "stay_towns_update5.sas7bdat", df_stay_towns5
                   ,["SERIAL"
                     , "TOWNCODE1"
                     , "TOWNCODE2"
                     , "TOWNCODE3"
                     , "TOWNCODE4"
                     , "TOWNCODE5"
                     , "TOWNCODE6"
                     , "TOWNCODE7"
                     , "TOWNCODE8"
                     , "NIGHTS1"
                     , "NIGHTS2"
                     , "NIGHTS3"
                     , "NIGHTS4"
                     , "NIGHTS5"
                     , "NIGHTS6"
                     , "NIGHTS7"
                     , "NIGHTS8"
                   ,"NIGHTS_IN_LONDON"
                   ,"NIGHTS_NOT_LONDON"
#                   , "NOTCOUNT"
                   ,"RATION_L_NL_ADES"])    
    cf.beep()
    sys.exit()
#    grp_nights_in_london    grp_nights_not_london
    #===========================================================================
    #===========================================================================
    
#    # Calculate spends
#    df_stay_towns6 = df_stay_towns5.copy()
##    print df_stay_towns6["RATION_L_NL_ADES"]
##    cf.beep()
##    sys.exit()
##    df_stay_towns6[]
#    
#    def selection(row):
#        nights = "NIGHTS"
#        towncode = "TOWNCODE"
#        var_spend = "SPEND"
#        
#        for count in range(1,9):
#            if (row["NIGHTS_IN_LONDON"] == 0) | (row["NIGHTS_IN_LONDON"] == np.NaN):
#                if row[towncode+str(count)] == np.NaN:
#                    row[var_spend+str(count)] = 1
#                    if (((row["NIGHTS_NOT_LONDON"] != 0) 
#                      | (row["NIGHTS_NOT_LONDON"] != np.NaN)) 
#                      & (row[nights+str(count)] != np.NaN)):
#                        row[var_spend+str(count)] = ((row[var_spend] 
#                                                      * row[nights+str(count)])
#                                                     /row["NIGHTS_NOT_LONDON"])
#                    else:
#                        row[var_spend+str(count)] = 0
#                else:
#                    row[var_spend+str(count)] = 9999
#            elif ((row["NIGHTS_NOT_LONDON"] == 0) | (row["NIGHTS_NOT_LONDON"] == np.NaN)):
#                if row[towncode+str(count)] == np.NaN:
#                    if (((row["NIGHTS_IN_LONDON"] != 0) 
#                      | (row["NIGHTS_IN_LONDON"] != np.NaN))
#                        & (row[nights+str(count)] != np.NaN)):
#                        row[var_spend+str(count)] = ((row[var_spend] 
#                                                      * row[nights+str(count)])
#                                                     /row["NIGHTS_IN_LONDON"])
#                    else:
#                        row[var_spend+str(count)] = 0
#                else:
#                    row[var_spend+str(count)] = 9999
#            else:
#                row["HERE"] = 3
#                if (((row["KNOWN_LONDON_VISIT"] != 0) 
#                   | (row["KNOWN_LONDON_VISIT"] != np.NaN))
#                    & ((row["KNOWN_LONDON_NOT_VISIT"] != 0) 
#                   | (row["KNOWN_LONDON_NOT_VISIT"] != np.NaN)) 
#                     & ((row["RATION_L_NL_ADES"] != 0) 
#                   | (row["RATION_L_NL_ADES"] != np.NaN))):
#                    row["H_K"] = ((row["NIGHTS_IN_LONDON"] 
#                                   / row["NIGHTS_NOT_LONDON"]) 
#                                  * row["RATION_L_NL_ADES"])
#                else:
#                    row["H_K"] = 0
#                    row["LONDON_SPEND"] = 0
#                    if ((row[towncode+str(count)] >= 70000)
#                      & (row[towncode+str(count)] <= 79999)):
#                        if (((row["NIGHTS_IN_LONDON"] != 0) 
#                          | (row["NIGHTS_IN_LONDON"] != np.NaN))
#                          & (row[nights+str(count)] != np.NaN) 
#                          & ((row["H_K"] != 0) | (row["H_K"] != np.NaN)) 
#                          & (row[var_spend] != 0)):
#                            row[var_spend+str(count)] = (((row[var_spend] * row["H_K"]) / ( 1 + row["H_K"])) * (row[nights+str(count)]/row["NIGHTS_IN_LONDON"]))
#                        else:
#                            row[var_spend+str(count)] = 0
#                            row["LONDON_SPEND"] = (row["LONDON_SPEND"] 
#                                                   + row[var_spend+str(count)])  
#                    else:
#                        row[var_spend+str(count)] = 9999
#        return row
#    
#    
##    df_stay_towns6["H_K"] = np.NaN
#    df_stay_towns6["LONDON_SPEND"] = np.NaN
#    df_stay_towns6["HERE"] = np.NaN
#    df_stay_towns6 = df_stay_towns6.apply(selection, axis = 1)


    # Calculate spends
    df_stay_towns6 = df_stay_towns5.copy()
#    df_stay_towns6.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\df_stay_towns6.csv")
#    cf.beep()
#    sys.exit()
        
    def selection(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        var_spend = "SPEND"
        
        #FIRST LOOP
        if (row["NIGHTS_IN_LONDON"] == 0):
            for count in range(1,9):
                if row[towncode+str(count)] == np.NaN:
                    if (((row["NIGHTS_NOT_LONDON"] != 0) & (row["NIGHTS_NOT_LONDON"] != np.NaN)) & row[nights+str(count)] != np.NaN):
                        row[var_spend+str(count)] = (row[var_spend] * row[nights+str(count)]) / row["NIGHTS_NOT_LONDON"]
                    else:
                        row[var_spend+str(count)] = 0
                                                                                                
        #SECOND LOOP
        elif (row["NIGHTS_NOT_LONDON"] != 0) & (row["NIGHTS_NOT_LONDON"] != np.NaN):
            for count in range(1,9):
                if row[towncode+str(count)] != np.NaN:
                    if (((row["NIGHTS_IN_LONDON"] != 0) & (row["NIGHTS_IN_LONDON"] != np.NaN)) & (row[nights+str(count)] != np.NaN)):
                        row[var_spend+str(count)] = (row[var_spend] * row[nights+str(count)]) / row["NIGHTS_IN_LONDON"]
                    else:
                        row[var_spend+str(count)] = 0
        
        #THIRD LOOP
        else:
            row["HERE"] = 3
            if (((row["KNOWN_LONDON_VISIT"] != 0) & (row["KNOWN_LONDON_VISIT"] != np.NaN)) 
                & ((row["KNOWN_LONDON_NOT_VISIT"] != 0) & (row["KNOWN_LONDON_NOT_VISIT"] != np.NaN)) 
                & ((row["RATION_L_NL_ADES"] != 0) & (row["RATION_L_NL_ADES"] != np.NaN))):
                row["H_K"] = (row["NIGHTS_IN_LONDON"] / row["NIGHTS_NOT_LONDON"]) * row["RATION_L_NL_ADES"]
            else:
                row["H_K"] = 0
                row["LONDON_SPEND"] = 0
                for count in range(1,9):
                    if ((row[towncode+str(count)] >= 70000) & (row[towncode+str(count)] <= 79999)):
                        if (((row["NIGHTS_IN_LONDON"] != 0) & (row["NIGHTS_IN_LONDON"] != np.NaN)) 
                            & (row[nights+str(count)] != np.NaN) 
                            & ((row["H_K"] != 0) & (row["H_K"] != np.NaN)) 
                            & (row[var_spend] != 0)):
                            row[var_spend+str(count)] = ((row[var_spend] * row["H_K"]) / ( 1 + row["H_K"])) * (row[nights+str(count)]/row["NIGHTS_IN_LONDON"])
                        else:
                            row[var_spend+str(count)] = 0
                            row["LONDON_SPEND"] = (row["LONDON_SPEND"] + row[var_spend+str(count)])  
        return row
    
#    df_stay_towns6["H_K"] = np.NaN
#    df_stay_towns6["LONDON_SPEND"] = np.NaN
#    df_stay_towns6["HERE"] = np.NaN
    df_stay_towns6 = df_stay_towns6.apply(selection, axis = 1)

    
    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns_update6", "stay_towns_update6.sas7bdat", df_stay_towns6, ["SERIAL", "SPEND1", "SPEND2", "SPEND3", "SPEND4", "SPEND5", "SPEND6", "SPEND7", "SPEND8", "H_K", "LONDON_SPEND"])
#    cf.compare_dfs("stay_towns_update6_full", "stay_towns_update6.sas7bdat", df_stay_towns6)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Finish calculating spends
#    df_stay_towns7 = df_stay_towns6.copy()
    df_stay_towns7 = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\stay_towns_update6.sas7bdat")
    df_stay_towns7.columns = df_stay_towns7.columns.str.upper()
#    df_stay_towns7.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\df_stay_towns7.csv")
#    cf.beep()
#    sys.exit()
    
    def calculate_spends(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        var_spend = "SPEND"
        
        for count in range(1,9):
            if ((row["NIGHTS_IN_LONDON"] != 0) & ((row["NIGHTS_NOT_LONDON"] != 0) | (row["NIGHTS_NOT_LONDON"] != np.NaN))):
                if row[towncode+str(count)] == np.NaN:
                    row[var_spend+str(count)] = 0
                elif (row[towncode+str(count)] <= 70000) | (row[towncode+str(count)] >= 79999):
                    if ((row["NIGHTS_NOT_LONDON"] != 0) & (row["NIGHTS_NOT_LONDON"] != np.NaN)):
                        row[var_spend+str(count)] = row[nights+str(count)] * (( row[var_spend] - row["LONDON_SPEND"])/ row["NIGHTS_NOT_LONDON"])
                    else:
                        row[var_spend+str(count)] = 0
        
        return row
    
    df_stay_towns7 = df_stay_towns7.apply(calculate_spends, axis=1) 
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns_update7", "stay_towns_update7.sas7bdat", df_stay_towns7, ["SERIAL", "SPEND1", "SPEND2", "SPEND3", "SPEND4", "SPEND5", "SPEND6", "SPEND7", "SPEND8"])    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Set markers to indicate london visits or outside
    df_stay_towns99 = df_stay_towns7.ix[df_stay_towns7["TOWNCODE1"] != 99999]
#    df_stay_towns99.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\df_stay_towns99_py.csv")
#    sas = pd.read_sas(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Town and Stay Imputation\stay_towns99.sas7bdat")
#    sas.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\df_stay_towns99_sas.csv")
#    cf.beep()
#    sys.exit()
    
    def set_markers(row):
        towncode = "TOWNCODE"
        town = "TOWN"
        
        for count in range(1,9):
            if (row[towncode+str(count)] >= 70000) & (row[towncode+str(count)] <= 79999):
                row[town+str(count)] = 1
            elif (row[towncode+str(count)] >= 0) & (row[towncode+str(count)] <= 59999):
                row[town+str(count)] = 0
            elif (row[towncode+str(count)] >= 90000) & (row[towncode+str(count)] <= 99999):
                row[town+str(count)] = 0
            else:
                row[town+str(count)] = 9
            
        for count in range(1,9):
            if (row[town+str(count)] == 1):
                row["LONDON"] = 1
            else:
                row["LONDON"] = 0
                 
        return row
    
    df_stay_towns99 = df_stay_towns99.apply(set_markers, axis=1)
    

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns99", "stay_towns99.sas7bdat", df_stay_towns99, ["SERIAL", "TOWN1", "TOWN2", "TOWN3", "TOWN4", "TOWN5", "TOWN6", "TOWN7", "TOWN8"])    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Create new variables for expenditure and stay
    df_stay_towns98 = df_stay_towns99.copy()
    df_stay_towns98["EXPLON"] = df_stay_towns98["LONDON_SPEND"]
    df_stay_towns98["STYLON"] = df_stay_towns98["NIGHTS_IN_LONDON"]
    df_stay_towns98["EXPOTH"] = df_stay_towns98[var_spend] - df_stay_towns98["LONDON_SPEND"] 
    df_stay_towns98["STYOTH"] = df_stay_towns98["NIGHTS_NOT_LONDON"] 
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns98", "stay_towns98.sas7bdat", df_stay_towns98, ["EXPLON", "STYLON", "EXPOTH", "STYOTH"])    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Sum spend and stay
    df_stay_totals = df_stay_towns98[[var_flow, var_residence, "EXPLON"
                                      , "EXPOTH", "STYLON", "STYOTH"]].copy()
    
    df_stay_totals = df_stay_totals.groupby([var_flow, var_residence])\
                                                 .agg({"EXPLON" : 'sum'
                                                       ,"EXPOTH" : 'sum'
                                                       , "STYLON" : 'sum'
                                                       , "STYOTH" : 'sum'})
    df_stay_totals = df_stay_totals.reset_index()
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_tot", "stay_tot.sas7bdat", df_stay_totals)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate uplift
    
    df_stay_totals1 = df_stay_totals.copy()
    df_stay_totals1["UPLIFT"] = 1
    df_stay_totals1.loc[(df_stay_totals1["EXPLON"] != 0) 
                        & (df_stay_totals1["STYLON"] != 0) 
                        & (df_stay_totals1["EXPOTH"] != 0) 
                        & (df_stay_totals1["STYOTH"] != 0)
                        , "UPLIFT"] = ((df_stay_totals1["EXPLON"] / df_stay_totals1["STYLON"]) 
                                       / (df_stay_totals1["EXPOTH"] / df_stay_totals1["STYOTH"]))
    
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_tot1", "stay_tot_update1.sas7bdat", df_stay_totals1)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Mark records where uplift needed
    
    df_stay_downs98b = df_stay_towns98.copy()
#    df_stay_towns98.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\LegacyUplift\Compare\df_stay_towns98.csv")
#    cf.beep()
#    sys.exit()
    
    df_stay_downs98b["SPENDT"] = np.where(df_stay_downs98b["SERIAL"] == 410148777027
                                          , (df_stay_downs98b["SPEND1"]
                                             + df_stay_downs98b["SPEND2"]
                                             + df_stay_downs98b["SPEND3"]
                                             + df_stay_downs98b["SPEND4"]
                                             + df_stay_downs98b["SPEND5"]), np.NaN)
    
#    df_stay_downs98b.loc[df_stay_downs98b["LONDON"] == 1, df_stay_downs98b["SPENDT"]] = pd.Series("HELLO?") #pd.Series(df_stay_downs98b["SPEND1"]
#                                                                       + df_stay_downs98b["SPEND2"]
#                                                                       + df_stay_downs98b["SPEND3"]
#                                                                       + df_stay_downs98b["SPEND4"]
#                                                                       + df_stay_downs98b["SPEND5"])
    cf.compare_dfs("stay_towns98b", "stay_towns98b.sas7bdat", df_stay_downs98b, ["SERIAL", "SPENDT"])    
    cf.beep()
    sys.exit()
    
    
    df_stay_downs98b.loc[df_stay_downs98b["LONDON"] == 1, "SPENDD"] = pd.Series(df_stay_downs98b[var_spend] 
                                                                       - df_stay_downs98b["SPENDT"])
    df_stay_downs98b.loc[(df_stay_downs98b["SPENDD"] > 0.01) | (df_stay_downs98b["SPENDD"] < -0.01)
                                                                                , "MARK"] = 1
    
    #===========================================================================
    #===========================================================================
    cf.compare_dfs("stay_towns98b", "stay_towns98b.sas7bdat", df_stay_downs98b, ["SERIAL", "SPENDT", "SPENDD", "MARK"])    
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
#    survey_support.setup_logging('IPS_logging_config_debug.json')
#    logger = cf.database_logger()
    
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
#    cf.insert_into_table_many(output, output_dataframe)
    
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
#    function_name = str(inspect.stack()[0][3])
#    audit_message = "Load Town and Stay Imputation: %s()" % function_name
#    
#    # Log success message in SAS_RESPONSE and AUDIT_LOG
#    logger.info("SUCCESS - Completed Town and Stay Imputation.")
#    cf.commit_to_audit_log("Create", "Town and Stay Imputation", audit_message)
    

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