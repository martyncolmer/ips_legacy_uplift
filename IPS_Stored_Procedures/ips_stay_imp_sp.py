from IPS_Unallocated_Modules import ips_impute


def do_ips_stay_imputation(input, output, var_serialNum, varStem, threshStem,
                            numLevels, donorVar, outputVar, measure,
                            var_eligibleFlag, var_impFlag,var_impLevel):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
    Purpose      : Imputes stay for the IPS system.
    Parameters   : input - the IPS survey dataset.
                   output - dataframe holding imputed records
                   var_serialNum - the serial number field name
                   var_stem - stem of the imputation variables parameters
                   num_levels - number of imputation levels
                   donor_var - name of the donor variable
                   output_var - name of the output variable
                   measure - measure function, such as mean
                   var_eligible_flag - the imputation eligibility field name for
                   both donor and recipient
                   var_imp_flag - the imputation required trigger/flag field name
                   var_imp_level - the imputation level field name
    Returns      : 
    Requirements : NA
    Dependencies : NA
    """
    df_input = input
    
    df_eligible = df_input.where(df_input['var_eligibleFlag'] == True)
    
    ips_impute.ips_impute(df_eligible, output, var_serialNum, varStem, threshStem
                          , numLevels, donorVar,outputVar, measure, var_impFlag
                          , var_impLevel)


def ips_stay_imp(SurveyData = 'sas_survey_subsample',OutputData = 'sas_stay_imp'
                 ,ResponseTable = 'sas_response',var_serialNum = 'serial'
                 ,varStem = 'vars',threshStem = 'thresh',numLevels = 1
                 ,donorVar = 'numNights',outputVar = 'stay',measure = 'mean'
                 ,var_eligibleFlag
                 ,var_impFlag = 'stay_imp_flag_pv',var_impLevel = 'stayk'):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
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
    do_ips_stay_imputation(SurveyData, OutputData, var_serialNum, varStem
                           , threshStem, numLevels, donorVar, outputVar, measure
                           , var_eligibleFlag, var_impFlag, var_impLevel)
    