'''
Created on 7 Mar 2018

@author: thorne1
'''
import inspect
import numpy as np
import pandas as pd
import survey_support
import math
from main.io import CommonFunctions as cf

import sys
from pprint import pprint

def do_ips_spend_imputation(df_survey_data, output, var_serialNum, var_flow
                            , var_purpose_grp, var_country_grp, var_residence
                            , var_stay, var_spend, var_final_wt, var_eligible_flag):
    """
    Author        : thorne1
    Date          : 13 Mar 2018
    Purpose       : Calculate the town and stay expenditure
    Parameters    : df_survey_data = "SAS_SURVEY_SUBSAMPLE"
                    output = "SAS_TOWN_STAY_IMP"
                    var_serialNum = "SERIAL"
                    var_flow = "FLOW"
                    var_purpose_grp = "PURPOSE_PV"
                    var_country_grp = "STAYIMPCTRYLEVEL4_PV"
                    var_residence = "RESIDENCE"
                    var_stay = "STAY"
                    var_spend = "SPEND"
                    var_final_wt = "FINAL_WT"
                    var_eligible_flag = "TOWN_IMP_ELIGIBLE_PV"
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
    cf.compare_dfs("stay_towns", "stay_towns.sas7bdat", df_output_data)
    # cf.beep()
    # sys.exit()
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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("seg1", "seg1.sas7bdat", df_segment1)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("seg2", "seg2.sas7bdat", df_segment2)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Merge the files containing ade1 and ade2
    df_segment_merge = pd.merge(df_segment1
                                , df_segment2
                                , on=[var_flow, var_purpose_grp,var_country_grp]
                                , how = 'left')

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("segment", "segment.sas7bdat", df_segment_merge)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns_update1", "stay_towns_update1.sas7bdat", df_extract_update)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

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
    df_temp_london["ADE1"] = (df_temp_london["ADE1_TEMP1"]
                              / df_temp_london["ADE1_TEMP2"])

    # Cleanse
    df_temp_london = df_temp_london.reset_index()
    df_temp_london.drop(["ADE1_TEMP1"], axis=1, inplace=True)
    df_temp_london.drop(["ADE1_TEMP2"], axis=1, inplace=True)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("temp_london", "temp_london.sas7bdat", df_temp_london)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

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
    df_temp_london2["ADE2"] = (df_temp_london2["ADE2_TEMP1"]
                               / df_temp_london2["ADE2_TEMP2"])

    # Cleanse
    df_temp_london2 = df_temp_london2.reset_index()
    df_temp_london2.drop(["ADE2_TEMP1"], axis=1, inplace=True)
    df_temp_london2.drop(["ADE2_TEMP2"], axis=1, inplace=True)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("temp_london_other", "temp_london_other.sas7bdat", df_temp_london2)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Merge files containing ade1 ade2
    df_london = pd.merge(df_temp_london
                         , df_temp_london2
                         , on=[var_purpose_grp, var_country_grp]
                         , how = 'left')

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("london", "london.sas7bdat", df_london)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Update extract with ade1 ade2 where not already set
    df_stay_towns2 = pd.merge(df_extract_update
                              , df_london
                              , on=[var_purpose_grp,var_country_grp]
                              , how = 'left')

    df_stay_towns2.rename(columns={"ADE1_x": "ADE1"
                                   ,"ADE2_x" : "ADE2"}, inplace=True)
    df_stay_towns2.drop(["ADE1_y"
                            ,"ADE2_y"], axis=1, inplace=True)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns_update2", "stay_towns_update2.sas7bdat", df_stay_towns2)
    cf.compare_dfs("stay_towns_update3", "stay_towns_update3.sas7bdat", df_stay_towns2)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Calculate ratio london to not london
    df_stay_towns4 = df_stay_towns2.copy()
    df_stay_towns4.loc[((df_stay_towns4["ADE1"] != 0) & (df_stay_towns4["ADE2"] != 0)),
                       "RATION_L_NL_ADES"] = (df_stay_towns4["ADE1"]/df_stay_towns4["ADE2"])
    df_stay_towns4["RATION_L_NL_ADES"].fillna(0, inplace=True)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns_update4", "stay_towns_update4.sas7bdat", df_stay_towns4)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Calculate number of nights in london and number of nights outside london
    df_stay_towns5 = df_stay_towns4.copy()
    df_stay_towns5["NIGHTS_IN_LONDON"] = 0
    df_stay_towns5["NIGHTS_NOT_LONDON"] = 0

    def another_selection(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"

        for count in range(1, 9):
            if not math.isnan(row[nights+str(count)]):
                if ((row[towncode+str(count)] >= 70000) & (row[towncode+str(count)] <= 79999)):
                    row["NIGHTS_IN_LONDON"] =(row["NIGHTS_IN_LONDON"]
                                              + row[nights+str(count)])
                else:
                    row["NIGHTS_NOT_LONDON"] = (row["NIGHTS_NOT_LONDON"]
                                                + row[nights+str(count)])

        return row

    df_stay_towns5 = df_stay_towns5.apply(another_selection, axis=1)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns_update5", "stay_towns_update5.sas7bdat", df_stay_towns5)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Calculate spends
    df_stay_towns6 = df_stay_towns5.copy()

    def selection(row):
        nights = "NIGHTS"
        towncode = "TOWNCODE"
        var_spend = "SPEND"

        #FIRST LOOP
        if row["NIGHTS_IN_LONDON"] == 0:
            for count in range(1,9):
                if math.isnan(row[towncode+str(count)]):
                    if (((row["NIGHTS_NOT_LONDON"] != 0) & (not math.isnan(row["NIGHTS_NOT_LONDON"])))
                        & (not math.isnan(row[nights+str(count)]))):
                        row[var_spend+str(count)] = ((row[var_spend]
                                                     * row[nights+str(count)])
                                                     / row["NIGHTS_NOT_LONDON"])
                    else:
                        row[var_spend+str(count)] = 0

        #SECOND LOOP
        elif (row["NIGHTS_NOT_LONDON"] == 0) | (math.isnan(row["NIGHTS_NOT_LONDON"])):
            for count in range(1,9):
                if not math.isnan(row[towncode+str(count)]):

                    if (((row["NIGHTS_IN_LONDON"] != 0)
                         & (not math.isnan(row["NIGHTS_IN_LONDON"])))
                        & (not math.isnan(row[nights+str(count)]))):
                        row[var_spend+str(count)] = ((row[var_spend]
                                                     * row[nights+str(count)])
                                                     / row["NIGHTS_IN_LONDON"])
                    else:
                        row[var_spend+str(count)] = 0

        #THIRD LOOP
        else:
            row["HERE"] = 3
            if (((row["KNOWN_LONDON_VISIT"] != 0) & (not math.isnan(row["KNOWN_LONDON_VISIT"])))
                &
                ((row["KNOWN_LONDON_NOT_VISIT"] != 0) & (not math.isnan(row["KNOWN_LONDON_NOT_VISIT"])))
                &
                ((row["RATION_L_NL_ADES"] != 0) & (not math.isnan(row["RATION_L_NL_ADES"])))):
                row["H_K"] = ((row["NIGHTS_IN_LONDON"]
                               / row["NIGHTS_NOT_LONDON"])
                              * row["RATION_L_NL_ADES"])
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
    df_stay_towns6["GRP_NIGHTS_IN_LONDON"] = 0
    df_stay_towns6["GRP_NIGHTS_NOT_LONDON"] = 0
    df_stay_towns6 = df_stay_towns6.apply(selection, axis = 1)

    # ===========================================================================
    # ===========================================================================
    column_order = list(df_survey_data.columns.values)
    new_columns = ["KNOWN_LONDON_NOT_VISIT", "KNOWN_LONDON_VISIT", "ADE1", "ADE2"
                   , "RATION_L_NL_ADES", "NIGHTS_IN_LONDON", "NIGHTS_NOT_LONDON"
                   , "LONDON_SPEND", "GRP_NIGHTS_IN_LONDON", "GRP_NIGHTS_NOT_LONDON"
                   , "HERE", "H_K"]
    column_order.extend(new_columns)
    df_stay_towns6 = df_stay_towns6.reindex(columns=column_order)
    cf.compare_dfs("stay_towns_update6", "stay_towns_update6.sas7bdat", df_stay_towns6)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns_update7", "stay_towns_update7.sas7bdat", df_stay_towns7)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Set markers to indicate london visits or outside
    df_stay_towns99 = df_stay_towns7.ix[df_stay_towns7["TOWNCODE1"] != 99999]

    def set_markers(row):
        towncode = "TOWNCODE"
        town = "TOWN"

        for count in range(1, 9):
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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns99", "stay_towns99.sas7bdat", df_stay_towns99)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Create new variables for expenditure and stay
    df_stay_towns98 = df_stay_towns99.copy()
    df_stay_towns98["EXPLON"] = df_stay_towns98["LONDON_SPEND"]
    df_stay_towns98["STYLON"] = df_stay_towns98["NIGHTS_IN_LONDON"]
    df_stay_towns98["EXPOTH"] = (df_stay_towns98[var_spend]
                                 - df_stay_towns98["LONDON_SPEND"])
    df_stay_towns98["STYOTH"] = df_stay_towns98["NIGHTS_NOT_LONDON"]

    # ===========================================================================
    # ===========================================================================
    new_cols = ["TOWN1", "TOWN2", "TOWN3", "TOWN4", "TOWN5", "TOWN6", "TOWN7", "TOWN8", "LONDON", "EXPLON", "STYLON", "EXPOTH", "STYOTH"]
    column_order.extend(new_cols)
    df_stay_towns98 = df_stay_towns98.reindex(columns=column_order)
    cf.compare_dfs("stay_towns98", "stay_towns98.sas7bdat", df_stay_towns98)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

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

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_tot", "stay_tot.sas7bdat", df_stay_totals)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Calculate uplift
    df_stay_totals1 = df_stay_totals.copy()
    df_stay_totals1["UPLIFT"] = np.NaN

    def uplift(row):

        if ((row["EXPLON"] != 0)
            & (row["STYLON"] != 0)
            & (row["EXPOTH"] != 0)
            & (row["STYOTH"] != 0)):
            row["UPLIFT"] = float((row["EXPLON"]
                                   / row["STYLON"])
                                  / (row["EXPOTH"]
                                    / row["STYOTH"]))
        else:
            row["UPLIFT"] = 1

        return row

    df_stay_totals1 = df_stay_totals1.apply(uplift, axis=1)

    # Merge totals onto file with markers
    df_stay_towns97 = pd.merge(df_stay_towns98, df_stay_totals1, on=[var_flow, var_residence], how="left")

    # Cleanse
    df_stay_towns97.drop(["EXPLON_x"], axis=1, inplace=True)
    df_stay_towns97.drop(["STYLON_x"], axis=1, inplace=True)
    df_stay_towns97.drop(["EXPOTH_x"], axis=1, inplace=True)
    df_stay_towns97.drop(["STYOTH_x"], axis=1, inplace=True)

    df_stay_towns97.rename(index=str, columns={"EXPLON_y": "EXPLON", "STYLON_y": "STYLON", "EXPOTH_y": "EXPOTH",
                                               "STYOTH_y": "STYOTH"}, inplace=True)

    #===========================================================================
    #===========================================================================
    column_order = list(df_stay_towns6.columns.values)
    new_columns = ["TOWN1",	"TOWN2", "TOWN3", "TOWN4", "TOWN5",	"TOWN6", "TOWN7", "TOWN8", "LONDON", "EXPLON", "STYLON",
                   "EXPOTH", "STYOTH", "UPLIFT"]
    column_order.extend(new_columns)
    df_stay_towns97 = df_stay_towns97.reindex(columns=column_order)
    cf.compare_dfs("df_stay_towns97a", "stay_towns97.sas7bdat", df_stay_towns97)
    # cf.beep()
    # sys.exit()
    #===========================================================================
    #===========================================================================

    # Apply uplift
    df_stay_towns96 = df_stay_towns97.copy()

    # ===========================================================================
    # ===========================================================================
    # cf.compare_dfs("df_stay_towns96_new", "stay_towns96_new.sas7bdat", df_stay_towns96)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Merge uplifted data
    df_stay_towns97_right = df_stay_towns97[["SERIAL", "EXPLON", "EXPOTH", "LONDON", "STYLON", "STYOTH", "TOWN1",
                                             "TOWN2", "TOWN3", "TOWN4", "TOWN5", "TOWN6", "TOWN7", "TOWN8", "UPLIFT"]]
    df_output_stay1 = pd.merge(df_stay_towns7
                               , df_stay_towns97_right
                               , on=[var_serialNum]
                               , how="outer")

    # ===========================================================================
    # ===========================================================================
    column_order.extend(new_cols)
    df_output_stay1 = df_output_stay1.reindex(columns=column_order)
    cf.compare_dfs("output_stay1", "output_stay1.sas7bdat", df_output_stay1)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Extract those records that need uplifting
    df_output_stay2 = df_output_stay1.dropna(subset=[["SPEND1", "NIGHTS_IN_LONDON"]])
    df_output_stay2 = df_output_stay2[(df_output_stay2["SPEND1"] != 0)]
    df_output_stay2 = df_output_stay2[(df_output_stay2["NIGHTS_IN_LONDON"] != 0)]

    # ===========================================================================
    # ===========================================================================
    df_output_stay2 = df_output_stay2.reindex(columns=column_order)
    cf.compare_dfs("output_stay2", "output_stay1_update1.sas7bdat", df_output_stay2, ["SERIAL", "SPEND1", "SPEND2",
                                                                                      "SPEND3", "SPEND4", "SPEND5",
                                                                                      "SPEND6", "SPEND7", "SPEND8"])
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Update main file with uplifted data
    # df_stay_towns7.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\Legacy Uplift\Compare\df_stay_towns7.csv")
    # df_output_stay2.to_csv(r"H:\My Documents\Documents\Git Repo\Misc and Admin\Legacy Uplift\Compare\df_output_stay2.csv")
    # cf.beep()
    # sys.exit()

    # df_stay_towns8 = df_output_stay2.drop(["EXPLON", "EXPOTH", "LONDON", "STYLON", "STYOTH", "TOWN1", "TOWN2", "TOWN3",
    #                                        "TOWN4", "TOWN5", "TOWN6", "TOWN7", "TOWN8", "UPLIFT"])

    df_output_stay3 = df_output_stay2[["SERIAL", "SPEND1", "SPEND2", "SPEND3", "SPEND4", "SPEND5", "SPEND6", "SPEND7",
                                       "SPEND8"]]
    df_stay_towns8 = pd.merge(df_stay_towns7
                              , df_output_stay3
                              , on="SERIAL"
                              , how="left")

    df_stay_towns8 = df_stay_towns8.drop(["SPEND1_y", "SPEND2_y", "SPEND3_y", "SPEND4_y", "SPEND5_y", "SPEND6_y", "SPEND7_y",
                         "SPEND8_y"], axis=1)
    df_stay_towns8 = df_stay_towns8.rename(columns={"SPEND1_x": "SPEND1",  "SPEND2_x": "SPEND2",  "SPEND3_x": "SPEND3",
                                              "SPEND4_x": "SPEND4",  "SPEND5_x": "SPEND5",  "SPEND6_x": "SPEND6",
                                              "SPEND7_x": "SPEND7",  "SPEND8_x": "SPEND8"})
    # pprint(list(df_stay_towns8.columns.values))

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("stay_towns_update8_stay_towns_8", "stay_towns_update8.sas7bdat", df_stay_towns8)
    # cf.beep()
    # sys.exit()
    # ===========================================================================
    # ===========================================================================

    # Create output file ready for appending to Oracle file
    df_output = df_stay_towns8[["SERIAL", "SPEND1", "SPEND2", "SPEND3", "SPEND4", "SPEND5", "SPEND6", "SPEND7", "SPEND8"]]
    df_output.round(1)

    # ===========================================================================
    # ===========================================================================
    cf.compare_dfs("output", "output_townspend_final.sas7bdat", df_output)
    cf.beep()
    sys.exit()
    # ===========================================================================
    # ===========================================================================


def calculate(processname, gparam_table_name, input, output, responseTable
              , var_serialNum, var_flow, var_purpose_grp, var_country_grp
              , var_residence, var_stay, var_spend, var_final_wt, var_eligible_flag):
    """
    Author        : thorne1
    Date          : 13 Mar 2018
    Purpose       : Sets up calculation for the town and stay expenditure and
                  : commits results to SQL database
    Parameters    : processname = "Foundation/ips/town_and_stay_imp_sp"
                    gparam_table_name = "ORA_DATA.SAS_PARAMETERS"
                    input = "SAS_SURVEY_SUBSAMPLE"
                    output = "SAS_TOWN_STAY_IMP"
                    responseTable = "SAS_RESPONSE"
                    var_serialNum = "SERIAL"
                    var_flow = "FLOW"
                    var_purpose_grp = "PURPOSE_PV"
                    var_country_grp = "STAYIMPCTRYLEVEL4_PV"
                    var_residence = "RESIDENCE"
                    var_stay = "STAY"
                    var_spend = "SPEND"
                    var_final_wt = "FINAL_WT"
                    var_eligible_flag = "TOWN_IMP_ELIGIBLE_PV"
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
    output_dataframe = do_ips_spend_imputation(df_survey_data, output, var_serialNum, var_flow, var_purpose_grp, var_country_grp, var_residence, var_stay, var_spend, var_final_wt, var_eligible_flag)

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
    calculate(processname="Foundation/ips/town_and_stay_imp_sp"
              , gparam_table_name="ORA_DATA.SAS_PARAMETERS"
              , input="SAS_SURVEY_SUBSAMPLE"
              , output="SAS_TOWN_STAY_IMP"
              , responseTable="SAS_RESPONSE"
              , var_serialNum="SERIAL"
              , var_flow="FLOW"
              , var_purpose_grp="PURPOSE_PV"
              , var_country_grp="STAYIMPCTRYLEVEL4_PV"
              , var_residence="RESIDENCE"
              , var_stay="STAY"
              , var_spend="SPEND"
              , var_final_wt="FINAL_WT"
              , var_eligible_flag="TOWN_IMP_ELIGIBLE_PV")