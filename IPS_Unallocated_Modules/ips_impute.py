import numpy as np

def ips_impute(input,output,var_serialNum,stratsbase,threshbase,numLevels,
               imputeVar,var_value,imputeFunction,var_imputeFlag,var_impLevel):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Generates segments for use within IPS imputation.
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
    
    df_impute_from = df_input[df_input['STAY_IMP_FLAG_PV'] == False]
    
    df_to_impute = df_input[df_input['STAY_IMP_FLAG_PV'] == True]


def ips_impute_segment(input,output,strata,imputeVar,function,var_value,
                       var_count,thresh):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Generates segments for use within IPS imputation.
    Parameters   : input - dataframe holding the records to be segmented
                   output - dataframe holding the produced segments
                   strata - the list of classification variables
                   imputeVar - variable to impute
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
    
    df_output = output.where(output['var_count'] > output['thresh'])
    
    
    df_output = df_input.groupby(imputeVar).agg({\
            'var_value' : function, 'var_count' : 'count'})
    
    # Might be temporary. When level is 0, performs the same function as 
    # concatenating var_value with the value of level
    df_output['var_value'] = df_output['var_value'] * 10
    
    return df_output
    
def ips_impute_match(remainder,input,output,strata,var_value,imputeVar,level,
                     var_level,var_imputeFlag):
    """
    Author       : James Burr
    Date         : 09 Feb 2018
    Purpose      : Produces and returns imputed records.
    Parameters   : remainder - dataframe of records left to impute
                   input - donor dataframe
                   output - file with imputed records
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
    
    df_input = df_input.where(df_input[strata] == np.NaN)
    
    df_remainder = df_remainder.merge(df_input, how = "outer")
    
    df_output = output
    
    df_output.sort_values(strata, inplace = True)
    