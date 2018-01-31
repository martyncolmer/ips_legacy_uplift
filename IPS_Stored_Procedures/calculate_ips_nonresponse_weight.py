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
    Returns      : Two dataframes
    Requirements : 
    Dependencies : 
    """
    
    
    df_nonresponsedata_sorted = df_nonresponsedata.sort_values(colset1)
    df_surveydata_sorted = df_surveydata.sort_values(colset1)
    
    df_psw = df_surveydata_sorted.groupby(colset1)\
            ['SHIFT_WT'].agg({'SHIFT_WT' : 'mean'})
    
    # Flattens the column structure
    df_psw = df_psw.reset_index()
    
    # Only keep rows that exist in df_nonresponsedata_sorted 
    df_grossmignonresp = pd.merge(df_nonresponsedata_sorted, df_psw, on=\
            colset1, how='left')
    
    # Add gross values using the primary sampling weight and add two new columns
    # to df_grossmignonresp
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
    
    # Use only records in which NR_FLAG_PV is 0
    df_surveydata_sliced = df_surveydata.loc[df_surveydata['NR_FLAG_PV'] == 0]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    # Create two new columns as aggregations of SHIFT_WT
    df_sumresp = df_surveydata_sorted.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART'])\
            ['SHIFT_WT'].agg({\
            'gross_resp' : 'sum',
            'count_resps' : 'count'})
    
    # Flattens the column structure after adding the new gross_resp and count_resps columns
    df_sumresp = df_sumresp.reset_index()
    
    # Calculate the grossed number of T&T non-respondents of the non-response strata    
    
    # Use only records from the survey dataset where the NR_FLAG_PV is 1, then sort    
    df_surveydata_sliced = df_surveydata.loc[df_surveydata['NR_FLAG_PV'] == 1]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])     
    
    # Create new column using the sum of ShiftWt
    df_sumordnonresp = df_surveydata_sorted.groupby(['NR_PORT_GRP_PV', 'ARRIVEDEPART'])\
            ['SHIFT_WT'].agg({\
            'grossordnonresp' : 'sum'})
    
    # Flattens the column structure after adding the new grossordnonresp column
    df_sumordnonresp = df_sumordnonresp.reset_index()
    
    # Sort values in the three dataframes required for the next calculation
    df_sumordnonresp = df_sumordnonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_sumresp = df_sumresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    df_summignonresp = df_summignonresp.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
     
    # Use the calculated data frames to calculate the non-response weight
    
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
    df_gnr['non_response_wt'] = np.where(df_gnr['gross_resp'] != 0, (df_gnr['gnr'] + df_gnr['gross_resp']) / df_gnr['gross_resp'], np.NaN)
    
    # Sort df_gnr and df_surveydata ready for producing summary
    df_surveydata_sorted = df_surveydata.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    df_gnr = df_gnr.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])

    # Ensure only complete or partial responses are kept
    df_surveydata_sorted = df_surveydata_sorted.loc[df_surveydata_sorted['NR_FLAG_PV'] == 0]
    
    # Produce summary by merging survey data and gnr data together, then sort
    df_out = df_surveydata_sorted.merge(df_gnr[['NR_PORT_GRP_PV',
                                                'ARRIVEDEPART', 
                                                'non_response_wt']],
                                        on=['NR_PORT_GRP_PV',
                                            'ARRIVEDEPART'],
                                        how = 'left')

    df_out = df_out.sort_values(by=['NR_PORT_GRP_PV', 'ARRIVEDEPART'])
    
    # Create and add three new columns calculated using SHIFT_WT
    df_summary = df_out.groupby(['NR_PORT_GRP_PV',
                                'ARRIVEDEPART',
                                'WEEKDAY_END_PV'])['SHIFT_WT'].agg({'mean_resps_sh_wt' : 'mean',\
                                                                   'count_resps' : 'count',\
                                                                   'prior_sum' : 'sum'})
    
    # Flatten column structure
    df_summary.reset_index(inplace = True)
    
    
    # Create and add one new column calculated using 'non_response_wt' in a 
    # different dataframe due to difficulty in creating all four new columns
    # simultaneously in a single dataframe
    df_summary_nr = df_out.groupby(['NR_PORT_GRP_PV',
                                'ARRIVEDEPART',
                                'WEEKDAY_END_PV'])['non_response_wt'].agg({'mean_nr_wt' : 'mean'})
    
    # Flatten column structure
    df_summary_nr.reset_index(inplace = True)
    
    # Merge all four new columns into the same dataframe
    df_summary = df_summary.merge(df_summary_nr, on = ['NR_PORT_GRP_PV',
                                          'ARRIVEDEPART',
                                          'WEEKDAY_END_PV'],
                     how = 'outer')
    
    # Merge the updated dataframe with specific columns from GNR.
    df_summary = df_gnr[['NR_PORT_GRP_PV',
                                        'ARRIVEDEPART',
                                        'gnr',
                                        'gross_resp']].merge(df_summary,
                                  on = ['NR_PORT_GRP_PV',
                                        'ARRIVEDEPART'], how='outer')
                                        
    # Calculate new non_response_wt value if condition is met
    df_out['non_response_wt'] = np.where(df_out['MIG_FLAG_PV'] == 0,
                                 (df_out['non_response_wt'] * df_out['TANDTSI'])
                                / df_out['MIGSI'], df_out['non_response_wt'])
    
    # Reduce output to just key value pairs
    df_out = df_out[['SERIAL', 'non_response_wt']]
    
    return (df_out, df_summary)
    
def calc_nr_weight():
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Function called to setup and initiate the calculation
    Parameters   : NA
    Returns      : NA
    Requirements : 
    Dependencies : 
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
     
    # Connect to Oracle
    conn = cf.get_oracle_connection()
    #parameters = cf.unload_parameters(136) - Currently not working, to be replaced
     
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_Non_Response_Weight"
     
    # Retrieve SAS data and load into dataframes
    path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
    path_to_nonresponse_data = root_data_path + r"\nonresponsedata_1.sas7bdat"
     
     
    df_surveydata = pd.read_sas(path_to_survey_data)
    df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)
     
    # Setup the columns sets used for the calculation steps
    colset1 = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV']
          
    colset2 = ['NR_PORT_GRP_PV', 'ARRIVEDEPART']
     
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_nonresponsedata.columns = df_nonresponsedata.columns.str.upper()
     
    print("Start - Calculate NonResponse Weight.")
    do_ips_nrweight_calculation()
     
    weight_calculated_dataframes = do_ips_nrweight_calculation()
     
    # Extract the two data sets returned from do_ips_nrweight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]
     
    # Append the generated data to output tables
    
    # 31st Jan 2018 - below two lines commented out by James Burr due to the 
    # parameters referenced there do not work currently, due to the
    # unload_parameters function relying on a table which doesn't exist. This
    # parameters section will be replaced with a different method in the near
    # future at which point this code will be changed.
    
    #cf.insert_into_table_many(parameters['OutputData'], surveydata_dataframe)
    #cf.insert_into_table_many(parameters['SummaryData'], summary_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load NonResponse Weight calculation: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed NonResponse weight calculation.")
    cf.commit_to_audit_log("Create", "NonReponse", audit_message)
    print("Completed - Calculate NonResponse Weight")


