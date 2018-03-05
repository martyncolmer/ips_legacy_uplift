import sys
import os
import logging
import inspect
import numpy as np
import pandas as pd
from sas7bdat import SAS7BDAT
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict
import survey_support
from IPSTransformation import CommonFunctions as cf



def compare_dfs(test_name, sas_file, df, col_list = False):
    
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Weight"
    print sas_root + "\\" + sas_file
    csv = pd.read_sas(sas_root + "\\" + sas_file)
    
    fdir = r"\\NDATA12\mahont1$\My Documents\GIT_Repositories\Test_Drop"
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

	
	

def do_ips_railex_imp(input, output, var_serial, var_eligible, var_fweight,
						var_count, strata, var_railfare, var_Spend,  minCountThresh):
    """
    Author       : Thomas Mahoney
    Date         : 28 / 02 / 2018
    Purpose      : Calculates the imputed values for rail expenditure for the IPS system.
    Parameters   : input - the IPS survey dataset         
                   output - the output dataset                                         
                   var_serial - the serial number field name           
                   var_eligible - flag for imputation eligibility (flow 5 or 8)  
				   var_fweight - previously estimated final weight
				   var_count - number in indvidual strata 
				   strata - List of classificatory variables 
				   var_railfare - value of total rail expenditure per country group
				   var_spend - previously imputed spend variable
				   minCountThresh - threshold for respondent count warning msg
    Returns      : df_output(dataframe containing serial number and calculated spend value)
    Requirements : NA
    Dependencies : NA
    """
	
    # Sort the input data by flow and railcountry
    input = input.sort_values(by = strata)    
	
	# Create second dataset containing records where flow is 5 or 8
	input2 = input[input[var_eligible].isin([5,8])]
	
	# Calculate the 'PRESPEND' column value using the var_spend and var_fweight column values.
	input2['PRESPEND'] = input2[var_spend] * input2[var_fweight]
	
	# Need to merge these three summaries (I THINK)
    gp_summin = input2.groupby(strata)[PreSpend].agg({'GROSSPRESPEND' : 'sum'})
    railexp_summin = input2.groupby(strata)[var_railfare].agg({'RAILEXP' : 'mean'})
    count_summin = input2.groupby(strata).agg({var_count : 'count'})
	
	df_summin = gp_summin # all 3 of the above?
	
	
	# Report any cells with respondent counts below the minCountThreshold
	
    # Create data set for rows below the threshold
    df_summin_thresholds_check = \
        df_summin[(summin[var_Count] < minCountThresh)]
    
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
		if(row['GROSSPRESPEND'] == 0:
			row['RAIL_FACTOR'] = np.NaN
		else:
			row['RAIL_FACTOR'] = (row['GROSSPRESPEND'] + row['RAILEXP']) / row['GROSSPRESPEND']
		return row
		
	df_summinsum = df_summin.apply(calculate_rail_factor,axis = 1)
	
	
	# Sort the calculated data frame by the strata ready to be merged
	df_summinsum = df_summinsum.sort_values(by = strata)
	
	# Generate output data set
	df_output = pd.merge(input,df_summinsum, on = strata, how = 'left')
	
	# Calculate the spend of the output data set
	def calculate_spend(row):
		if not row['RAIL_FACTOR'].isnan():
			row[var_spend] = round(row[var_spend]* row['RAIL_FACTOR'], 1)
		return row
		
	df_output = df_output.apply(calculate_spend,axis = 1)
	
	# Keep only the 'SERIAL' and 'SPEND' columns
	df_output = df_output[[var_serial,var_spend]]
	
	
    # Return the generated data frame to be appended to oracle
    return (df_output)
	
	
	
	
def calculate(SurveyData, OutputData, ResponseTable, var_serial, var_flow,
			  var_fweight, var_count, strata, var_railexercise, var_spend,
			  minCountThresh):			  
    """
    Author       : Thomas Mahoney
    Date         : 28 / 02 / 2018
    Purpose      : Calculates the imputed values for rail expenditure for the IPS system.
    Parameters   : SurveyData - the IPS survey dataset         
                   OutputData - the output dataset                                         
                   responseTable - Oracle table to hold response information (status etc.)    
                   var_serialNum - the serial number field name           
                   var_flow - irection of travel (use 5 out uk , 8 in )  
				   var_fweight - previously estimated final weight
				   var_count - number in indvidual strata 
				   strata - List of classificatory variables 
				   var_railfare - value of total rail expenditure per country group
				   var_spend - previously imputed spend variable
				   minCountThresh - threshold for respondent count warning msg                     
    Returns      : NA 
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    global df_surveydata
    
    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    #df_nonresponsedata = SAS7BDAT(path_to_nonresponse_data).to_data_frame()
    
    # This method is untested with a range of data sets but is faster
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Weight\survey_input.sas7bdat"
    path_to_unsampled_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Weight\ustotals.sas7bdat"
    df_surveydata = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)
    #df_ustotals = cf.get_table_values(PopTotals)
    
    df_surveydata.columns = df_surveydata.columns.str.upper()


    # Start the Calculate IPS Rail Impute function.
    print("Start - Calculate IPS Rail Impute.")     
	output_dataframe = do_ips_railex_imp(df_surveydata, 'output', var_serial, var_flow, var_fweight,
											var_count, strata , var_railexercise, var_spend, minCountThresh)


    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData, output_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Rail Imputationn: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Rail Imputation.")
    cf.commit_to_audit_log("Create", "RailImputation", audit_message)
    print("Completed - Calculate IPS Rail Impute.")
    

	
	
if __name__ == '__main__':
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE',
              OutputData = 'SAS_RAIL_IMP', 
              ResponseTable = 'SAS_RESPONSE', 
              var_serial = 'SERIAL', 
              var_flow = 'FLOW', 
              var_fweight = 'FINAL_WT', 
              var_count = 'COUNT',
              strata = ['FLOW', 
                        'RAIL_CNTRY_GRP_PV'],
              var_railexercise = 'RAIL_EXERCISE_PV',
              var_spend = 'SPEND',
              minCountThresh = '30')