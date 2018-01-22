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


def do_ips_nrweight_calculation():
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs calculations to find the nonresponse weight.
    Parameters   : NA
    Returns      : 
    Requirements : 
    Dependencies : 
    """
    
    # Add the shift weight
    df_nonresponsedata_sorted = df_nonresponsedata.sort_values(colset1)
    df_surveydata_sorted = df_surveydata.sort_values(colset1)
    
    df_summary = df_surveydata_sorted.groupby(parameters['ShiftsStratumDef'])\
            [parameters['var_PSW']].agg({'SHIFT_WT' : 'mean'})
    
    # Only keep rows that exist in df_nonresponsedata_sorted 
    df_grossmignonresp = pd.merge(df_nonresponsedata_sorted, df_psw, on=\
            colset1, how='left')
    
    # Add gross values using the primary sampling weight
    df_grossmignonresp['grossmignonresp'] = df_grossmignonresp['SHIFT_WT'] * \
            df_grossmignonresp['MIGTOTAL']
            
    df_grossmignonresp['grossordnonresp'] = df_grossmignonresp['SHIFT_WT'] * \
            df_grossmignonresp['ORDTOTAL']
    
    # Summarise over non-response strata
    df_grossmignonresp = df_grossmignonresp.sort_values(parameters['NRStratumDef'])
    
    df_summignonresp = df_grossmignonresp.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART']).agg({\
            'grossmignonresp' : 'sum', 'grossordnonresp' : 'sum'})
    
    df_summignonresp = df_summignonresp.rename(columns = {'grossordnonresp' : 'grossinelresp'})
    
    # Calculate the grossed number of respondents over the non-response strata
    df_surveydata_sliced = df_surveydata.loc[df_surveydata['NR_FLAG_PV'] == 0]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_sumresp = df_surveydata_sorted.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART'])\
            [parameters['var_PSW']].agg({\
            'gross_resp' : 'sum',
            'count_resps' : 'count'})
    
    # Calculate the grossed number of T&T non-respondents of the non-response strata        
    df_surveydata_sliced = df_surveydata.loc[df_surveydata['NR_FLAG_PV'] == 1]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])     
    
    df_sumordnonresp = df_surveydata_sorted.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART'])\
            [parameters['var_PSW']].agg({\
            'grossordnonresp' : 'sum'})
    
    df_sumordnonresp = df_sumordnonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_sumresp = df_sumresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_summignonresp = df_summignonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    # Use the calculated data frames to calculate the non-response weight
    
    print(df_sumordnonresp)
    
    sys.exit()
    
# Call JSON configuration file for error logger setup
survey_support.setup_logging('IPS_logging_config_debug.json')

# Connect to Oracle
conn = cf.get_oracle_connection()
parameters = cf.unload_parameters(136)


# Setup path to the base directory containing data files
root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_Non_Response_Weight"

# Retrieve SAS data and load into dataframes
path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
path_to_nonresponse_data = root_data_path + r"\nonresponsedata_1.sas7bdat"
path_to_psw_data = root_data_path + r"\psw.sas7bdat"
# path_to_new_data2 = root_data_path + r"\"

df_surveydata = pd.read_sas(path_to_survey_data)
df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)
df_psw = pd.read_sas(path_to_psw_data)

# Setup the columns sets used for the calculation steps
colset1 = parameters['ShiftsStratumDef']
     
colset2 = parameters['ShiftsStratumDef'] \
+ [parameters['var_PSW']]

df_psw.columns = df_psw.columns.str.upper()
df_surveydata.columns = df_surveydata.columns.str.upper()
df_nonresponsedata.columns = df_nonresponsedata.columns.str.upper()

print("Start - Calculate NonResponse Weight.")
do_ips_nrweight_calculation()

# weight_calculated_dataframes = do_ips_nrweight_calculation()
# 
# # Extract the two data sets returned from do_ips_nrweight_calculation
# surveydata_dataframe = weight_calculated_dataframes[0]
# summary_dataframe = weight_calculated_dataframes[1]
# 
# # Append the generated data to output tables
# cf.insert_into_table_many(parameters['OutputData'], surveydata_dataframe)
# cf.insert_into_table_many(parameters['SummaryData'], summary_dataframe)
# 
# # Retrieve current function name using inspect:
# # 0 = frame object, 3 = function name. 
# # See 28.13.4. in https://docs.python.org/2/library/inspect.html
# function_name = str(inspect.stack()[0][3])
# audit_message = "Load NonResponse Weight calculation: %s()" % function_name
# 
# # Log success message in SAS_RESPONSE and AUDIT_LOG
# cf.database_logger().info("SUCCESS - Completed NonResponse weight calculation.")
# cf.commit_to_audit_log("Create", "NonReponse", audit_message)
print("Completed - Calculate NonResponse Weight")
