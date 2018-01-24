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
    
    df_summary = df_surveydata_sorted.groupby(colset2).agg({'SHIFT_WT' : 'mean'})
    
    # Only keep rows that exist in df_nonresponsedata_sorted 
    df_grossmignonresp = pd.merge(df_nonresponsedata_sorted, df_psw, on=\
            colset1, how='left')
    
    # Add gross values using the primary sampling weight
    df_grossmignonresp['grossmignonresp'] = df_grossmignonresp['SHIFT_WT'] * \
            df_grossmignonresp['MIGTOTAL']
            
    df_grossmignonresp['grossordnonresp'] = df_grossmignonresp['SHIFT_WT'] * \
            df_grossmignonresp['ORDTOTAL']
    
    # Summarise over non-response strata
    df_grossmignonresp = df_grossmignonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_summignonresp = df_grossmignonresp.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART']).agg({\
            'grossmignonresp' : 'sum', 'grossordnonresp' : 'sum'})
    
    # Flattens the column structure after adding the new grossmignonresp and grossordnonresp columns
    df_summignonresp = df_summignonresp.reset_index()
    
    df_summignonresp = df_summignonresp.rename(columns = {'grossordnonresp' : 'grossinelresp'})
    
    # Calculate the grossed number of respondents over the non-response strata
    df_surveydata_sliced = df_surveydata.loc[df_surveydata['NR_FLAG_PV'] == 0]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_sumresp = df_surveydata_sorted.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART'])\
            ['SHIFT_WT'].agg({\
            'gross_resp' : 'sum',
            'count_resps' : 'count'})
    
    # Flattens the column structure after adding the new gross_resp and count_resps columns
    df_sumresp = df_sumresp.reset_index()
    
    # Calculate the grossed number of T&T non-respondents of the non-response strata    
    
    # Use onely records from the survey dataset where the NR_FLAG_PV is 1    
    df_surveydata_sliced = df_surveydata.loc[df_surveydata['NR_FLAG_PV'] == 1]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])     
    
    # Create new column using the sum of ShiftWt (evaluates from var_PSW)
    df_sumordnonresp = df_surveydata_sorted.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART'])\
            ['SHIFT_WT'].agg({\
            'grossordnonresp' : 'sum'})
    
    # Flattens the column structure after adding the new grossordnonresp column
    df_sumordnonresp = df_sumordnonresp.reset_index()
    
    # Sort values in the three dataframes
    df_sumordnonresp = df_sumordnonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_sumresp = df_sumresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_summignonresp = df_summignonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
     
    # Use the calculated data frames to calculate the non-response weight
    #df_gnr_with_drops = df_gnr.drop('j', 'x', 'z', 'errorStr') may be required to drop these values from unprepared table
    
    # Merge previously sorted dataframes into one, ensuring all rows from summignonresp are kept
    df_gnr = df_summignonresp.merge(df_sumresp, on=['NR_PORT_GRP_PV', 'ARRIVEDEPART'], how = 'outer')
    
    df_gnr = df_gnr.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])

    df_gnr = df_gnr.merge(df_sumordnonresp, on=['NR_PORT_GRP_PV', 'ARRIVEDEPART'], how = 'left')
    
    # Replace all NaN values in columns with zero's
    df_gnr['grossmignonresp'].fillna(0, inplace =True)
    df_gnr['grossinelresp'].fillna(0, inplace =True)
    df_gnr['grossordnonresp'].fillna(0, inplace =True)
    
    # Add in two new columns with checks to prevent division by 0 
    df_gnr['gnr'] = np.where(df_gnr['gross_resp'] != 0, df_gnr['grossordnonresp'] + df_gnr['grossmignonresp'] + df_gnr['grossinelresp'], 0)
    df_gnr['non_response_wt'] = np.where(df_gnr['gross_resp'] != 0, (df_gnr['gnr'] + df_gnr['gross_resp']) / df_gnr['gross_resp'], None)
    
    # Sort df_gnr and df_surveydata ready for producing summary
    df_surveydata_sorted = df_surveydata.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    df_gnr = df_gnr.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])

    # Ensure only complete or partial responses are kept
    df_surveydata_sorted = df_surveydata_sorted.loc[df_surveydata_sorted['NR_FLAG_PV'] == 0]
    
    # Produce summary by merging survey data and gnr data together
    df_out = df_surveydata_sorted.merge(df_gnr[['NR_PORT_GRP_PV', \
                                                'ARRIVEDEPART', \
                                                'non_response_wt']], \
                                        on=['NR_PORT_GRP_PV', \
                                            'ARRIVEDEPART'], \
                                        how = 'left')
    
    # Produce summary output
    df_out = df_out.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    # Setup for aggregation function; outlines variables to work on, columns to\
    # add and functions to perform
    summary_aggregation = {
                              'SHIFT_WT' : {'mean_resps_sh_wt' : 'mean',
                                            'prior_sum' : 'sum',
                                            'count_resps' : 'count'},
                              'non_response_wt' : {'mean_nr_wt' : 'mean'}
                              }

    df_summary_new = df_out.groupby(['NR_PORT_GRP_PV',\
                                'ARRIVEDEPART',\
                                'WEEKDAY_END_PV']).agg(summary_aggregation)
    
    print(df_summary)
    

# Call JSON configuration file for error logger setup
survey_support.setup_logging('IPS_logging_config_debug.json')

# Connect to Oracle
conn = cf.get_oracle_connection()
#parameters = cf.unload_parameters(136)


# Setup path to the base directory containing data files
root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_Non_Response_Weight"

# Retrieve SAS data and load into dataframes
path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
path_to_nonresponse_data = root_data_path + r"\nonresponsedata_1.sas7bdat"
path_to_psw_data = root_data_path + r"\psw.sas7bdat"
path_to_gnr_data = root_data_path + r"\gnr.sas7bdat"

df_surveydata = pd.read_sas(path_to_survey_data)
df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)
df_psw = pd.read_sas(path_to_psw_data)
df_gnr = pd.read_sas(path_to_gnr_data)

# Setup the columns sets used for the calculation steps
colset1 = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV']
     
colset2 = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'SHIFT_WT']

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
