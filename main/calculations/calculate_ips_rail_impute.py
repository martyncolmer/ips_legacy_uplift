import inspect
import math

import numpy as np
import pandas as pd
import survey_support

from main.io import CommonFunctions as cf

OUTPUT_TABLE_NAME = 'SAS_RAIL_IMP'
ELIGIBLE_VARIABLE = 'FLOW'  # direction of travel (use 5 out uk , 8 in )
COUNT_VARIABLE = 'COUNT'
STRATA = ['FLOW', 'RAIL_CNTRY_GRP_PV']
RAIl_FARE_VARIABLE = 'RAIL_EXERCISE_PV'
SPEND_VARIABLE = 'SPEND'
PRESPEND_VARIABLE = 'PRESPEND'
GROSS_PRESPEND_VARIABLE = 'GROSSPRESPEND'
RAIL_EXPENSE_VARIABLE = 'RAILEXP'
RAIL_FACTOR_VARIABLE = 'RAIL_FACTOR'


def do_ips_railex_imp(df_input, var_serial, var_final_weight, minCountThresh):
    """
    Author       : Thomas Mahoney
    Date         : 28 / 02 / 2018
    Purpose      : Calculates the imputed values for rail expenditure for the IPS system.
    Parameters   : df_input - the IPS survey dataset         
                   output - the output dataset                                         
                   var_serial - the serial number field name
                   var_final_weight - previously estimated final weight
                   minCountThresh - threshold for respondent count warning msg
    Returns      : df_output(dataframe containing serial number and calculated spend value)
    Requirements : NA
    Dependencies : NA
    """
    
    # Sort the df_input data by flow and rail country
    df_input = df_input.sort_values(by = STRATA)  
        
    # Create second data set containing records where flow is not null
    input2 = df_input[np.isfinite(df_input[ELIGIBLE_VARIABLE])]

    # Calculate the PRESPEND_VARIABLE column value using the SPEND_VARIABLE and var_fweight column values.
    input2[PRESPEND_VARIABLE] = input2[SPEND_VARIABLE] * input2[var_final_weight]
    
    input2 = input2.sort_values(by = STRATA)  
    
    # Replace blank values with zero as python drops blanks during the aggregation process.  
    input2[STRATA] = input2[STRATA].fillna(0)
    
    
    # Generate the aggregated data 
    gp_summin = input2.groupby(STRATA)[PRESPEND_VARIABLE].agg({GROSS_PRESPEND_VARIABLE : 'sum',
                                                               COUNT_VARIABLE : 'count'})
    railexp_summin = input2.groupby(STRATA)[RAIl_FARE_VARIABLE].agg({RAIL_EXPENSE_VARIABLE : 'mean'})
    
    # Reset the data frames index to include the new columns generated
    gp_summin = gp_summin.reset_index()
    railexp_summin = railexp_summin.reset_index()
    
    # Merge the generated data sets into one
    df_summin = pd.merge(gp_summin,railexp_summin,how = 'inner')
        
    # Replace the previously filled blanks with their original values
    df_summin[STRATA] = df_summin[STRATA].replace(0,np.NaN)
    
    # Report any cells with respondent counts below the minCountThreshold
    
    # Create data set for rows below the threshold
    df_summin_thresholds_check = \
        df_summin[(df_summin[COUNT_VARIABLE] < minCountThresh)]
    
    # Collect data below of specified threshold
    threshold_string = ""
    for index, record in df_summin_thresholds_check.iterrows():
        threshold_string += "___||___" \
                         + df_summin_thresholds_check.columns[0] + " : " + str(record[0]) + " | "\
                         + df_summin_thresholds_check.columns[1] + " : " + str(record[1])
                         
    # Output the values below of the threshold to the logger
    if len(df_summin_thresholds_check) > 0:
        cf.database_logger().warning('WARNING: Respondent count below minimum threshold for: ' + threshold_string)
    
    
    # Calculate each row's rail factor
    def calculate_rail_factor(row):
        if row[GROSS_PRESPEND_VARIABLE] == 0:
            row[RAIL_FACTOR_VARIABLE] = np.NaN
        else:
            row[RAIL_FACTOR_VARIABLE] = (row[GROSS_PRESPEND_VARIABLE]
                                  + row[RAIL_EXPENSE_VARIABLE]) / row[GROSS_PRESPEND_VARIABLE]
        return row

    df_summinsum = df_summin.apply(calculate_rail_factor,axis = 1)
    
    
    # Sort the calculated data frame by the STRATA ready to be merged
    df_summinsum = df_summinsum.sort_values(by = STRATA)
    
    # Append the calculated values to the input data set (generating our output)
    df_output = pd.merge(df_input,df_summinsum, on = STRATA, how = 'left')
    
    # Calculate the spend of the output data set
    def calculate_spend(row):
        if not math.isnan(row[RAIL_FACTOR_VARIABLE]):
            if not math.isnan(row[SPEND_VARIABLE]):
                row[SPEND_VARIABLE] = round(row[SPEND_VARIABLE] * row[RAIL_FACTOR_VARIABLE])
        return row

    df_output = df_output.apply(calculate_spend, axis=1)
        
    # Keep only the 'SERIAL' and 'SPEND' columns
    df_output = df_output[[var_serial,SPEND_VARIABLE]]
    
    # Return the generated data frame to be appended to oracle
    return (df_output)
    

def calculate(survey_data, var_serial, var_final_weight, minCountThresh = 30):
    """
    Author       : Thomas Mahoney
    Date         : 28 / 02 / 2018
    Purpose      : Calculates the imputed values for rail expenditure for the IPS system.
    Parameters   : SurveyData - the IPS survey dataset             
                   var_serialNum - the serial number field name
                   var_final_weight - previously estimated final weight
                   minCountThresh - threshold for respondent count warning msg                     
    Returns      : NA 
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
        
    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    #df_nonresponsedata = SAS7BDAT(path_to_nonresponse_data).to_data_frame()
    
    # This method is untested with a range of data sets but is faster
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Rail Imputation\surveydata.pkl"
    df_surveydata = pd.read_pickle(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)
    #df_ustotals = cf.get_table_values(PopTotals)
    
    df_surveydata.columns = df_surveydata.columns.str.upper()
    
    # Start the Calculate IPS Rail Impute function.
    print("Start - Calculate IPS Rail Impute.")     
    output_dataframe = do_ips_railex_imp(df_surveydata, var_serial, var_final_weight, minCountThresh)
    
    # Append the generated data to output table
    #cf.insert_into_table_many(OutputData, output_dataframe)
    cf.insert_dataframe_into_table(OUTPUT_TABLE_NAME, output_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Rail Imputationn: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Rail Imputation.")
    cf.commit_to_audit_log("Create", "RailImputation", audit_message)
    print("Completed - Calculate IPS Rail Impute.")
