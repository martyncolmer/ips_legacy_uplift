def ips_impute(input,output,var_serialNum,stratsbase,threshbase,numLevels,
               imputeVar,var_value,imputeFunction,var_imputeFlag,var_impLevel):
    
    
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
    