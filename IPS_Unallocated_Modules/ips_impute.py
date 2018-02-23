import numpy as np
import pandas as pd
import sys

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
    
    df_impute_from = df_input[df_input[var_impute_flag] == False]
    
    # Create recipient set, in which the inpute flag is true
    df_to_impute = df_input[df_input[var_impute_flag] == True]
    
    level = 0
    
    # dictionary_of_dataframes will contain a copy of the output dataframe at
    # each iteration of the while loop, accessed through the key which uses
    # the iteration number to define it. SAS creates intermediate datasets in 
    # this style, but may not use them.
    dictionary_of_dataframes = {}
    
    count = 'COUNT'
    
    while((level < num_levels) & (df_to_impute.empty == False)):
        
        key_name = 'df_output_' + str(level)
        
        # 'Imputed' and 'count' may be redudant as they are 'return variables' in SAS
        # Thresh and strata base lists are each a list containing other lists
        # These lists need to be hard coded and passed in from the calling procedure
        df_segment_output = ips_impute_segment(df_impute_from, level
                                               , strata_base_list[level], impute_var
                                               , impute_function, var_value
                                               , count, thresh_base_list[level])
        
        df_output = ips_impute_match(df_to_impute, df_segment_output
                                           , strata_base_list[level]
                                           , var_value, impute_var, level
                                           , var_impute_level, var_impute_flag)
        
        dictionary_of_dataframes[key_name] = df_output.copy()
        
        level += 1
    
    print(df_output)
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
            str(var_value) + str(level) : str(function), str(var_count) : 'count'})
    
    df_output.reset_index(inplace = True)
    
    df_output = df_output.replace('Nothing', np.NaN)
    
    df_output = df_output.where(df_output[var_count] > thresh)
    
    df_output = df_output.dropna(thresh = 2)

    return df_output
    
    
def ips_impute_match(remainder,input,strata,var_value,impute_var,level,
                     var_level,var_impute_flag):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Produces and returns imputed records.
    Parameters   : remainder - dataframe of records left to impute
                   input - donor dataframe
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
    
    # Ensure remainder contains observations that require imputing
    df_input = df_input.where(df_input[strata] == np.NaN)
    
    df_remainder = df_remainder.merge(df_input, how = "outer")
    
    print(df_remainder)
    sys.exit()
    
    # Update output with imputed values
    df_output.sort_values(strata, inplace = True)
    
    df_input[var_level] = np.where(df_input[var_level] == np.NaN & df_input[var_impute_flag] == 1
                                   , level, df_input[var_level])
    
    df_input[var_value] = np.where(df_input[var_level] == np.NaN & df_input[var_impute_flag] == 1
                                   , var_value + str(level)
                                   , df_input[var_level])
        
    df_output = df_output.merge(df_input, on = strata, how = 'outer')
    
    return df_output