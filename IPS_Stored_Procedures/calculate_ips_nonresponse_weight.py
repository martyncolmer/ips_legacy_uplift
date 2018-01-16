import sys
import os
import logging
import inspect
import numpy as np
import pandas as pd
import survey_support
from IPSTransformation import CommonFunctions as cf

def do_ips_nrweight_calculation():
    pass


# Call JSON configuration file for error logger setup
survey_support.setup_logging('IPS_logging_config_debug.json')

# Connect to Oracle
conn = cf.get_oracle_connection()
parameters = cf.unload_parameters(59) # Not likely to be 59 here, do not run code yet.

# Setup path to the base directory containing data file
root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_Non_Response_Weight"

# Retrieve SAS data and load into dataframes
path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
path_to_nonresponse_data = root_data_path + r"\nonresponsedata_1.sas7bdat"

df_surveydata = pd.read_sas(path_to_survey_data)
df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)

print("Start - Calculate NonResponse Weight.")
weight_calculated_dataframes = do_ips_nrweight_calculation()

# Extract the two data sets returned from do_ips_nrweight_calculation
surveydata_dataframe = weight_calculated_dataframes[0]
summary_dataframe = weight_calculated_dataframes[1]

# Append the generated data to output tables
cf.insert_into_table_many(parameters['OutputData'], surveydata_dataframe)
cf.insert_into_table_many(parameters['SummaryData'], summary_dataframe)

# Retrieve current function name using inspect:
# 0 = frame object, 3 = function name. 
# See 28.13.4. in https://docs.python.org/2/library/inspect.html
function_name = str(inspect.stack()[0][3])
audit_message = "Load NonResponse Weight calculation: %s()" %function_name

# Log success message in SAS_RESPONSE and AUDIT_LOG
cf.database_logger().info("SUCCESS - Completed NonResponse weight calculation.")
cf.commit_to_audit_log("Create", "NonReponse", audit_message)
print("Completed - Calculate NonResponse Weight")