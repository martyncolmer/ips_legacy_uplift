import numpy as np
import pandas as pd



def ips_impute(input,output,var_serial_num,strata_base_list,thresh_base_list,num_levels,
               impute_var,var_value,impute_function,var_impute_flag,var_impute_level):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Performs imputations based on input parameters
    Parameters   : input - dataframe holding both donors and recipients
                   output - dataframe holding imputed records
                   var_serial_num - variable holding the serial number
                   strata_base_list - list containing lists of strata variable names
                   thresh_base_list - list containing threshold values
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
    
    df_output[count] = 0
    
    # Loop until no more records can be imputed or max number of iterations is reached
    while((level < num_levels) & (df_to_impute.empty == False)):
        
        key_name = 'df_output_match_' + str(level)
        
        # strata_base_list is a list containing other lists, which control sorting
        # at each iteration level.
        # These lists need to be hard coded and passed in from the calling procedure.
        # Only the list for the current iteration is passed from strata_base_list
        
        # Calculate the imputed values to be used
        df_segment_output = ips_impute_segment(df_impute_from, level
                                               , strata_base_list[level], impute_var
                                               , impute_function, var_value
                                               , count, thresh_base_list[level])
        
        # Use values from segment output to impute missing values 
        df_output_frames = ips_impute_match(df_to_impute, df_segment_output, df_output
                                           , strata_base_list[level]
                                           , var_value, impute_var, level
                                           , var_impute_level, var_impute_flag
                                           , count)
        
        # Store output from each iteration, which is fed into next iteration
        df_output = df_output_frames[0]
        
        # Stores the remaining values needing imputing
        df_to_impute = df_output_frames[1]
        
        # Stores a copy of the current output dataset (may be redundant)
        dictionary_of_dataframes[key_name] = df_output.copy()
        
        level += 1
    
    # Tidy up the output by keeping only non-missing data and a subset of columns
    columns_to_keep = [var_serial_num, var_value, var_impute_level]
    df_output = df_output[columns_to_keep]
    df_output = df_output.dropna()
    
    return df_output


def ips_impute_segment(input,level,strata,impute_var,function,var_value,
                       var_count,thresh):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Generates segments for use within IPS imputation.
    Parameters   : input - dataframe holding the records to be segmented
                   level - current iteration value
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

    # Ensure rows with missing data aren't excluded indiscriminately
    df_input.fillna(value = 'Nothing', inplace = True)
    
    # Impute values from donor variable
    df_output = df_input.groupby(strata)[impute_var].agg({\
            var_value + str(level) : str(function), var_count + str(level) : 'count'})
    
    # Flatten column structure back into one index
    df_output.reset_index(inplace = True)
    
    # Change dataset missing values back to missing instead of a string
    df_output = df_output.replace('Nothing', np.NaN)
    
    # Keep only rows where count is above threshold. New count column created for
    # each iteration to allow easier aggregation of count data later on
    df_output = df_output.where(df_output[var_count + str(level)] > thresh)
    
    # Remove rows still containing missing data
    df_output = df_output.dropna(thresh = 2)
    
    return df_output
    
    
def ips_impute_match(remainder,input,output,strata,var_value,impute_var,level,
                     var_level,var_impute_flag, var_count):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Produces and returns imputed records.
    Parameters   : remainder - dataframe of records left to impute
                   input - donor dataframe
                   output - current latest output set
                   strata - list of classification variables
                   var_value - variable holding the name of the output value field
                   imputeVar - variable to be imputed
                   level - current imputation level
                   var_level - variable holding the name of the imputation level
                               output field
                   var_imputeFlag - name of imputation flag variable
                   var_count - name of count variable
    Returns      : File(CHECKTHIS: is it a file?) containing imputed records.
    Requirements : 
    Dependencies : 
    """
    
    # Create sorted dataframes from passed-in data
    df_input = input
    
    df_remainder = remainder
    
    df_remainder = df_remainder.sort_values(strata)
    
    df_input = df_input.sort_values(strata)
    
    # Merge all data and indicate where the data is found. Keep only rows that
    # are found in df_remainder
    df_remainder = pd.merge(df_remainder, df_input, how = "outer"
                            , indicator = True).query("_merge == 'left_only'")
    
    df_remainder = df_remainder.drop('_merge', axis = 1)
    df_remainder = df_remainder.reset_index(drop = True)
    
    df_remainder.sort_values(strata, inplace = True)
    
    # Merge current output data with donor dataframe
    df_output = output
    df_output = df_output.merge(df_input, how = "left"
                            , on = strata, indicator = True)
    
    df_output.sort_values(strata, inplace = True)
   
    # Setup iteration specific column name variables
    value_level = str(var_value) + str(level)
    count_level = str(var_count) + str(level)
    
    # Update output with imputed values
    # If conditions are met, set value column to be imputed to value of the 
    # calculate imputed column, collect the count value and record which 
    # iteration level this row was imputed in.
    df_output[var_value] = np.where((df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] == 'both'), df_output[value_level], df_output[var_value])
    df_output[var_count] = np.where((df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] == 'both'), df_output[count_level], df_output[var_count])
    df_output[var_level] = np.where((df_output[var_level].isnull() == True) & (df_output[var_impute_flag] == 1.0) & (df_output['_merge'] == 'both'), level, df_output[var_level])
    
    # Remove merge origin tracking column from output
    df_output = df_output.drop('_merge', axis = 1)
    df_output = df_output.reset_index(drop = True)
    
    df_output = df_output.sort_values(strata)

    return (df_output, df_remainder)