'''
Created on 7 Mar 2018

@author: thorne1
'''
import inspect
import numpy as np
import pandas as pd
import survey_support
import math
from IPSTransformation import CommonFunctions as cf
import sys

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
#    cf.compare_dfs("stay_towns", "stay_towns.sas7bdat", df_output_data)    
#    cf.beep()
#    sys.exit()
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
    df_segment1["ADE1_TEMP1"] = pd.Series(df_output_data[var_final_wt] 
                                          * (df_output_data[var_spend] 
                                             / df_output_data[var_stay]))
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
    df_segment2["ADE2_TEMP1"] = pd.Series(df_output_data[var_final_wt] 
                                          * (df_output_data[var_spend] 
                                             / df_output_data[var_stay]))
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
                                                              ,var_country_grp], how = 'left')

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
#    cf.compare_dfs("stay_towns_update1", "stay_towns_update1.sas7bdat", df_extract_update)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
      
    # Calculate ade1 with out flow
    df_temp_london = df_output_data[[var_purpose_grp
                                  , var_country_grp]].ix[(towncode_condition)]
    
    # Calculate
    df_temp_london["ADE1_TEMP1"] = pd.Series((df_output_data[var_final_wt] 
                                              * df_output_data[var_spend]) 
                                             / (df_output_data[var_stay]))
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
                                                              ,var_country_grp], how = 'left')
    
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
#    cf.compare_dfs("stay_towns_update2", "stay_towns_update2.sas7bdat", df_stay_towns2) 
#    cf.compare_dfs("stay_towns_update3", "stay_towns_update3.sas7bdat", df_stay_towns2)
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate ratio london to not london
    df_stay_towns4 = df_stay_towns2.copy()
    df_stay_towns4.loc[((df_stay_towns4["ADE1"] != 0) & (df_stay_towns4["ADE2"] != 0)), 
                       "RATION_L_NL_ADES"] = (df_stay_towns4["ADE1"]/df_stay_towns4["ADE2"])
    df_stay_towns4["RATION_L_NL_ADES"].fillna(0, inplace=True) 

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns_update4", "stay_towns_update4.sas7bdat", df_stay_towns4)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #=========================================================================== 
    
    # Calculate number of nights in london and number of nights outside london
    df_stay_towns5 = df_stay_towns4.copy()
    df_stay_towns5["NIGHTS_IN_LONDON"] = 0
    df_stay_towns5["NIGHTS_NOT_LONDON"] = 0
    
    def another_selection(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        
        for count in range(1,9):
            if not math.isnan(row[nights+str(count)]):
                if ((row[towncode+str(count)] >= 70000) & (row[towncode+str(count)] <= 79999)):
                    row["NIGHTS_IN_LONDON"] = row["NIGHTS_IN_LONDON"] + row[nights+str(count)]
                else:
                    row["NIGHTS_NOT_LONDON"] = row["NIGHTS_NOT_LONDON"] + row[nights+str(count)]
        
        return row
    
    df_stay_towns5 = df_stay_towns5.apply(another_selection, axis=1)

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns_update5", "stay_towns_update5.sas7bdat", df_stay_towns5)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate spends
    df_stay_towns6 = df_stay_towns5.copy()
    
    def selection(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        var_spend = "SPEND"
        
        #FIRST LOOP
        if (row["NIGHTS_IN_LONDON"] == 0):
            for count in range(1,9):
                if math.isnan(row[towncode+str(count)]):
                    if (((row["NIGHTS_NOT_LONDON"] != 0) & (not math.isnan(row["NIGHTS_NOT_LONDON"])))
                        & (not math.isnan(row[nights+str(count)]))):
                        row[var_spend+str(count)] = (row[var_spend] 
                                                     * row[nights+str(count)]) / row["NIGHTS_NOT_LONDON"]
                    else:
                        row[var_spend+str(count)] = 0
                                                                                                
        #SECOND LOOP
        elif (row["NIGHTS_NOT_LONDON"] == 0) | (math.isnan(row["NIGHTS_NOT_LONDON"])):
            for count in range(1,9):
                if not math.isnan(row[towncode+str(count)]):
                    
                    if (((row["NIGHTS_IN_LONDON"] != 0) 
                         & (not math.isnan(row["NIGHTS_IN_LONDON"])))
                        & (not math.isnan(row[nights+str(count)]))):
                        row[var_spend+str(count)] = (row[var_spend] 
                                                     * row[nights+str(count)]) / row["NIGHTS_IN_LONDON"]
                    else:
                        row[var_spend+str(count)] = 0
        
        #THIRD LOOP
        else:
            row["HERE"] = 3
            if (((row["KNOWN_LONDON_VISIT"] != 0) & (not math.isnan(row["KNOWN_LONDON_VISIT"]))) 
                & ((row["KNOWN_LONDON_NOT_VISIT"] != 0) & (not math.isnan(row["KNOWN_LONDON_NOT_VISIT"]))) 
                & ((row["RATION_L_NL_ADES"] != 0) & (not math.isnan(row["RATION_L_NL_ADES"])))):
                row["H_K"] = (row["NIGHTS_IN_LONDON"] / row["NIGHTS_NOT_LONDON"]) * row["RATION_L_NL_ADES"]
            else:
                row["H_K"] = 0
            row["LONDON_SPEND"] = 0
        
            for count in range(1,9):
                if ((row[towncode+str(count)] >= 70000) & (row[towncode+str(count)] <= 79999)):
                    if (((row["NIGHTS_IN_LONDON"] != 0) & (not math.isnan(row["NIGHTS_IN_LONDON"]))) 
                        & (not math.isnan(row[nights+str(count)])) 
                        & ((row["H_K"] != 0) & (not math.isnan(row["H_K"]))) 
                        & (row[var_spend] != 0)):
                        row[var_spend+str(count)] = (((row[var_spend] 
                                                      * row["H_K"]) 
                                                     / ( 1 + row["H_K"])) 
                                                     * (row[nights+str(count)]
                                                        /row["NIGHTS_IN_LONDON"]))
                    else:
                        row[var_spend+str(count)] = 0
                    row["LONDON_SPEND"] = (row["LONDON_SPEND"] 
                                           + row[var_spend+str(count)])  
        
        return row
    
    df_stay_towns6["H_K"] = np.NaN
    df_stay_towns6["LONDON_SPEND"] = 0
    df_stay_towns6 = df_stay_towns6.apply(selection, axis = 1)
    
    #===========================================================================
    #===========================================================================
    column_order = list(df_survey_data.columns.values)
    new_columns = ["KNOWN_LONDON_NOT_VISIT", "KNOWN_LONDON_VISIT", "ADE1", "ADE2"
                   , "RATION_L_NL_ADES", "NIGHTS_IN_LONDON", "NIGHTS_NOT_LONDON"
                   , "LONDON_SPEND", "GRP_NIGHTS_IN_LONDON", "GRP_NIGHTS_NOT_LONDON"
                   , "HERE", "H_K"]
    column_order.extend(new_columns)
#    df_stay_towns6 = df_stay_towns6.reindex(columns=column_order)
#    cf.compare_dfs("stay_towns_update6", "stay_towns_update6.sas7bdat", df_stay_towns6)
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Finish calculating spends
    df_stay_towns7 = df_stay_towns6.copy()
    
    def calculate_spends(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        var_spend = "SPEND"
        
        if ((row["NIGHTS_IN_LONDON"] != 0) 
            & ((row["NIGHTS_NOT_LONDON"] != 0) 
               & (not math.isnan(row["NIGHTS_NOT_LONDON"])))):
            for count in range(1,9):
                
                if (math.isnan(row[towncode+str(count)])):
                    row[var_spend+str(count)] = 0
                elif ((row[towncode+str(count)] < 70000) 
                      | (row[towncode+str(count)] > 79999)):
                    if (((row["NIGHTS_NOT_LONDON"] != 0) 
                         & (not math.isnan(row["NIGHTS_NOT_LONDON"])))
                         & (not math.isnan(row[nights+str(count)]))):
                        row[var_spend+str(count)] = (row[nights+str(count)] 
                                                     * ((row[var_spend] 
                                                         - row["LONDON_SPEND"]) 
                                                        / row["NIGHTS_NOT_LONDON"]))
                    else:
                        row[var_spend+str(count)] = 0
            
        return row
    
    df_stay_towns7 = df_stay_towns7.apply(calculate_spends, axis=1) 
    
    #===========================================================================
    #===========================================================================
#    df_stay_towns7 = df_stay_towns7.reindex(columns=column_order)
#    cf.compare_dfs("stay_towns_update7", "stay_towns_update7.sas7bdat", df_stay_towns7)
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Set markers to indicate london visits or outside
    df_stay_towns99 = df_stay_towns7.ix[df_stay_towns7["TOWNCODE1"] != 99999]
    
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
            
        if ((row["TOWN1"] == 1) | (row["TOWN2"] == 1) 
            | (row["TOWN3"] == 1) | (row["TOWN4"] == 1) 
            | (row["TOWN5"] == 1) | (row["TOWN6"] == 1) 
            | (row["TOWN7"] == 1) | (row["TOWN8"] == 1)):
            row["LONDON"] = 1
        else:
            row["LONDON"] = 0
                 
        return row
    
    df_stay_towns99 = df_stay_towns99.apply(set_markers, axis=1)

    #===========================================================================
    #===========================================================================
#    df_stay_towns99 = df_stay_towns99.reindex(columns=column_order)
#    cf.compare_dfs("stay_towns99", "stay_towns99.sas7bdat", df_stay_towns99)    
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
    new_cols = ["TOWN1", "TOWN2", "TOWN3", "TOWN4", "TOWN5", "TOWN6", "TOWN7", "TOWN8", "LONDON", "EXPLON", "STYLON", "EXPOTH", "STYOTH"]
    column_order.extend(new_cols)
#    df_stay_towns98 = df_stay_towns98.reindex(columns=column_order)
#    cf.compare_dfs("stay_towns98", "stay_towns98.sas7bdat", df_stay_towns98)    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Sum spend and stay
    df_stay_totals = df_stay_towns98[[var_flow, var_residence, "EXPLON"
                                      , "EXPOTH", "STYLON", "STYOTH"]].copy()
    
    df_stay_totals[[var_flow, var_residence]] = df_stay_totals[[var_flow
                                                                , var_residence]].fillna(-1)
    
    df_stay_totals = df_stay_totals.groupby([var_flow, var_residence])\
                                                 .agg({"EXPLON" : 'sum'
                                                       ,"EXPOTH" : 'sum'
                                                       , "STYLON" : 'sum'
                                                       , "STYOTH" : 'sum'})
    df_stay_totals = df_stay_totals.reset_index()
    
    df_stay_totals[[var_flow, var_residence]] = df_stay_totals[[var_flow, var_residence]].replace(-1,np.NaN)
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_tot", "stay_tot.sas7bdat", df_stay_totals)
#    cf.compare_dfs("stay_tot", "stay_tot.sas7bdat", df_stay_totals)        
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Calculate uplift
    df_stay_totals1 = df_stay_totals.copy()
    df_stay_totals1["UPLIFT"] = np.NaN

    def uplift(row):
        
        if ((row["EXPLON"] != 0) 
            & (row["STYLON"] != 0) 
            & (row["EXPOTH"] != 0) 
            & (row["STYOTH"] != 0)):
            row["UPLIFT"] = float((row["EXPLON"]
                                   /row["STYLON"])
                                  /(row["EXPOTH"]
                                    /row["STYOTH"]))
        else:
            row["UPLIFT"] = 1
            
        return row
    
    df_stay_totals1 = df_stay_totals1.apply(uplift, axis=1)
    
    #===========================================================================
    #===========================================================================
    # Two False values within data set for EXPOTH and UPLIFT
    stay_tot_cols = ["FLOW", "RESIDENCE", "EXPLON", "EXPOTH", "STYLON", "STYOTH", "UPLIFT"]
    df_stay_totals1 = df_stay_totals1.reindex(columns=stay_tot_cols)
    cf.compare_dfs("stay_tot_update1", "stay_tot_update1.sas7bdat", df_stay_totals1)    
    cf.beep()
    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Mark records where uplift needed
    df_stay_towns98b = df_stay_towns98.ix[df_stay_towns98["LONDON"] == 1]

    df_stay_towns98b["SPENDD"] = np.NaN
    df_stay_towns98b["SPENDT"] = np.NaN
    df_stay_towns98b["MARK"] = np.NaN
    
    def mark_records(row):
        
        if ((not math.isnan(row["SPEND1"])) 
            | (not math.isnan(row["SPEND2"])) 
            | (not math.isnan(row["SPEND3"])) 
            | (not math.isnan(row["SPEND4"])) 
            | (not math.isnan(row["SPEND5"]))):
            row["SPENDT"] = float(row["SPEND1"] 
                                 + row["SPEND2"] 
                                 + row["SPEND3"] 
                                 + row["SPEND4"] 
                                 + row["SPEND5"])
        
        if (not math.isnan(row["SPENDT"])):
            row["SPENDD"] = float(row[var_spend] - row["SPENDT"])
        
        if (row["SPENDD"] > 0.01) | (row["SPENDD"] < -0.1) | (math.isnan(row["SPENDD"])):
            row["MARK"] = 1  
        return row

    df_stay_towns98b = df_stay_towns98b.apply(mark_records, axis=1)
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("stay_towns98b", "stay_towns98b.sas7bdat", df_stay_towns98b, ["SERIAL"
#                                                                                 , "SPENDT"
#                                                                                 , "SPEND1"  
#                                                                                 , "SPEND2" 
#                                                                                 , "SPEND3" 
#                                                                                 , "SPEND4" 
#                                                                                 , "SPEND5"
#                                                                                 , "SPENDD"
#                                                                                 , "MARK"
#                                                                                 , "SPEND"])    
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
     
    # Merge totals onto file with markers
    df_stay_towns97 = pd.merge(df_stay_towns98, df_stay_totals1, on=[var_flow
                                                                ,var_residence], how = "left")   
    
    
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("df_stay_towns97", "stay_towns97.sas7bdat", df_stay_towns97)
#    df_stay_towns97[["SERIAL", "EXPLON_x", "EXPLON_y", "EXPOTH_x", "EXPOTH_y", "STYLON_x", "STYLON_y", "STYOTH_x", "STYOTH_y"]].to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\Legacy Uplift\Compare\df_stay_towns97.csv")  
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Apply uplift
    df_stay_towns96 = df_stay_towns97.copy()
    
    def apply_uplift(row):
        towncode = "TOWNCODE"
        stay = "STAY"
        nights = "NIGHTS"
        var_spend = "SPEND"
        
        if row["MARK"] == 1:
            for count in range(1,9):
                if (row[towncode+str(count)] > 70000) & (row[towncode+str(count)] < 79999):
                    row[stay+str(count)+"X"] = row[nights+str(count)] * row["UPLIFT"]
                else:
                    row[stay+str(count)+"X"] = row[nights+str(count)]
            
            row["STAY0X"] = row["STAY1X"] + row["STAY2X"] + row["STAY3X"] + row["STAY4X"] + row["STAY5X"]
            row[var_spend+str(count)] = row[var_spend] / row["STAY0X"] * row[stay+str(count)+"X"]
        
        return row
    
    df_stay_towns96.apply(apply_uplift, axis=1)

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("df_stay_towns96", "df_stay_towns96.sas7bdat", df_stay_towns96)
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Merge uplifted data
    df_output_stay1 = pd.merge(df_survey_data, df_stay_towns96, on=[var_serialNum], how = "left")
    
    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("output_stay1", "output_stay1.sas7bdat", df_output_stay1)
#    cf.beep()
#    sys.exit()
    #===========================================================================
    #===========================================================================
    
    # Extract those records that need uplifting
    condition = (((df_output_stay1["SPEND1"] != 0) & (df_output_stay1["SPEND1"] != np.NaN))
                 &
                 ((df_output_stay1["NIGHTS_IN_LONDON"] != 0) & (df_output_stay1["NIGHTS_IN_LONDON"] != np.NaN)))
     
    df_output_stay2 = df_output_stay1.ix[(condition)]

    #===========================================================================
    #===========================================================================
#    cf.compare_dfs("output_stay2", "output_stay1_update1.sas7bdat", df_output_stay2)
#    cf.beep()
#    sys.exit()
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