import numpy as np
import pandas as pd
import sys

def compare_dfs(test_name, sas_file, df, col_list = False):
    
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Fares Imputation"
    print sas_root + "\\" + sas_file
    csv = pd.read_sas(sas_root + "\\" + sas_file)
    
    fdir = r"H:\\My Documents\Git Repositories\ILU_Fares_impute_testing"
    sas = "_sas.csv"
    py = "_py.csv"
    
    print("TESTING " + test_name)
    
    if col_list == False:
        csv.to_csv(fdir+"\\"+test_name+sas)
        df.to_csv(fdir+"\\"+test_name+py)
    else:
        csv[col_list].to_csv(fdir+"\\"+test_name+sas)
        df[col_list].to_csv(fdir+"\\"+test_name+py)
    
    print(test_name + " COMPLETE")
    print("")


def ips_impute(input,output,var_serialNum,strata_base_list,thresh_base_list,num_levels,
               impute_var,var_value,impute_function,var_impute_flag,var_impute_level):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Performs imputations based on input parameters
    Parameters   : input - dataframe holding both donors and recipients
                   output - dataframe holding imputed records
                   var_serialNum - variable holding the serial number
                   strata_base - stem for the strata variables
                   thresh_base - stem for the threshold variables
                   num_levels - number of imputation levels (run from 0 to num-1)
                   impute_var - variable to be imputed
                   var_value - variable holding the name of the output value field
                   impute_function - imputation cluster measure function (e.g. mean)
                   var_impute_flag - name of the imputation flag variable
                   var_impute_level - name of the imputation level output field
    Returns      : Dataframe containing the imputed records
    Requirements : NA
    Dependencies : NA
    """
    
    # Create the donor set, in which the impute flag is false
    df_input = input
    
    df_output = df_input
    
    df_to_impute = df_input.loc[df_input[var_impute_flag] == 1.0]
    
    # Create recipient set, in which the inpute flag is true
    df_impute_from = df_input.loc[df_input[var_impute_flag] == 0.0]
    
    level = 0
    
    # dictionary_of_dataframes will contain a copy of the output dataframe at
    # each iteration of the while loop, accessed through the key which uses
    # the iteration number to define it. SAS creates intermediate datasets in 
    # this style, but may not use them.
    dictionary_of_dataframes = {}
    
    count = 'COUNT'
    
    while((level < num_levels) & (df_to_impute.empty == False)):
        
        key_name = 'df_output_match_' + str(level)
        
        # Thresh and strata base lists are each a list containing other lists
        # These lists need to be hard coded and passed in from the calling procedure
        df_segment_output = ips_impute_segment(df_impute_from, level
                                               , strata_base_list[level], impute_var
                                               , impute_function, var_value
                                               , count, thresh_base_list[level])
        
        df_output_frames = ips_impute_match(df_to_impute, df_segment_output, df_output
                                           , strata_base_list[level]
                                           , var_value, impute_var, level
                                           , var_impute_level, var_impute_flag)
        
        df_output = df_output_frames[0]
        df_to_impute = df_output_frames[1]
        
        dictionary_of_dataframes[key_name] = df_output.copy()
        
        level += 1
    
    compare_dfs('final_output_test', 'output_final.sas7bdat', df_output)
    sys.exit()
    
    return df_output


def ips_impute_segment(input,level,strata,impute_var,function,var_value,
                       var_count,thresh):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Generates segments for use within IPS imputation.
    Parameters   : input - dataframe holding the records to be segmented
                   output - dataframe holding the produced segments
                   strata - the list of classification variables
                   impute_var - variable to impute
                   function - measurement function to use e.g. mean
                   var_value - variable holding the name of the output value field
                   var_count - variable holding the name of the segment count field
                   thresh - the segment count threshold
    Returns      : Dataframe containing the produced segments
    Requirements : NA
    Dependencies : NA
    """

    df_input = input
    
    df_input = df_input.sort_values(strata)

    # SAS code is unclear here. Summary being performed using input as the input
    # dataset but the output is the resolved version of the output dataset with
    # a condition applied. Unsure if the input and output data is supposed to be
    # merged in this instance.
    df_input.fillna(value = 'Nothing', inplace = True)
    
    df_output = df_input.groupby(strata)[impute_var].agg({\
            var_value + str(level) : str(function), var_count : 'count'})
    
    df_output.reset_index(inplace = True)
    
    df_output = df_output.replace('Nothing', np.NaN)
    
    df_output = df_output.where(df_output[var_count] > thresh)
    
    df_output = df_output.dropna(thresh = 2)
    
    return df_output
    
    
def ips_impute_match(remainder,input, output,strata,var_value,impute_var,level,
                     var_level,var_impute_flag):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Produces and returns imputed records.
    Parameters   : remainder - dataframe of records left to impute
                   input - donor dataframe
                   output - current latest output set
                   impute_from - dataframe containing current overall output
                   level - current imputation level
                   strata - list of classification variables
                   var_value - variable holding the name of the output value field
                   imputeVar - variable to be imputed
                   level - current imputation level
                   var_level - variable holding the name of the imputation level
                               output field
                   var_imputeFlag - name of imputation flag variable
    Returns      : File(CHECKTHIS: is it a file?) containing imputed records.
    Requirements : 
    Dependencies : 
    """
    
    df_input = input
    
    df_remainder = remainder
    
    df_remainder = df_remainder.sort_values(strata)
    
    df_input = df_input.sort_values(strata)
    
    # Merge all data and indicate where the data is found. Keep rows that are
    # not found in both datasets.
    df_remainder = pd.merge(df_remainder, df_input, how = "outer"
                            , indicator = True).query("_merge == 'left_only'")
    
    df_remainder = df_remainder.drop('_merge', axis = 1)
    df_remainder = df_remainder.reset_index(drop = True)
    
    # Update output with imputed values
    df_to_impute_merge_input = df_remainder
    
    df_to_impute_merge_input.sort_values(strata, inplace = True)
    
    # Merge current output data with donor dataframe
    df_output = output
    df_output = df_output.merge(df_input, how = "left"
                            , on = strata, indicator = True)
    
    df_output.sort_values(strata, inplace = True)
    
    # If conditions are met, set df_input[var_level] to level and
    # set df_input[var_value] to df_input[var_value + iteration number/level]
    # i.e. the value of the column FARE3 if on the third iteration, for example
    value_level = str(var_value) + str(level)
    
    df_output[var_value] = np.where((df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] == 'both'), df_output[value_level], df_output[var_value])
    df_output[var_level] = np.where((df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] == 'both'), level, df_output[var_level])
    
    #df_output = df_output.loc[(df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] != 'left_only'), [var_value]] = df_output[value_level]
   # df_output = df_output.loc[(df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] != 'left_only'), [var_level]] = level
    
    df_output = df_output.drop('_merge', axis = 1)
    df_output = df_output.reset_index(drop = True)
    
    df_output = df_output.sort_values(strata)
    
    print("Output match 1 doneso.")

    return (df_output, df_to_impute_merge_input)