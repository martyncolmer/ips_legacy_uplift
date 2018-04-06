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
from main.io import CommonFunctions as cf


def do_ips_nrweight_calculation(SurveyData,NonResponseData,OutputData,SummaryData,ResponseTable,
              NRStratumDef,ShiftsStratumDef,var_NRtotals,var_NonMigTotals,var_SI,
              var_migSI,var_TandTSI,var_PSW,var_NRFlag,var_migFlag,var_respCount,
              var_NRWeight,var_meanSW,var_priorSum,var_meanNRW,var_grossResp,
              var_gnr,var_serialNum,minCountThresh):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs calculations to find the nonresponse weight.
    Parameters   : NA
    Returns      : Two dataframes
    Requirements : 
    Dependencies : 
    """
    
    df_nonresponsedata_sorted = df_nonresponsedata.sort_values(ShiftsStratumDef)
    df_surveydata_sorted = df_surveydata.sort_values(ShiftsStratumDef)
    
    df_psw = df_surveydata_sorted.groupby(ShiftsStratumDef)\
            [var_PSW].agg({var_PSW : 'mean'})
    
    # Flattens the column structure
    df_psw = df_psw.reset_index()
    
    # Only keep rows that exist in df_nonresponsedata_sorted 
    df_grossmignonresp = pd.merge(df_nonresponsedata_sorted, df_psw, on=\
            ShiftsStratumDef, how='left')
    
    # Add gross values using the primary sampling weight and add two new columns
    # to df_grossmignonresp
    df_grossmignonresp['grossmignonresp'] = df_grossmignonresp[var_PSW] * \
            df_grossmignonresp[var_NRtotals]
            
    df_grossmignonresp['grossordnonresp'] = df_grossmignonresp[var_PSW] * \
            df_grossmignonresp[var_NonMigTotals]
    
    # Validate that non-reponse totals can be grossed
    
    df_migtotal_not_zero = df_grossmignonresp[df_grossmignonresp['MIGTOTAL'] != 0]
    
    if(len(df_migtotal_not_zero[df_migtotal_not_zero['grossmignonresp'].isnull()]) > 0):
        logger.error('Error: Unable to gross up non-response total.')
    
    # Summarise over non-response strata
    df_grossmignonresp = df_grossmignonresp.sort_values(by=NRStratumDef)
    
    df_summignonresp = df_grossmignonresp.groupby(NRStratumDef).agg({\
            'grossmignonresp' : 'sum', 'grossordnonresp' : 'sum'})
    
    # Flattens the column structure after adding the new grossmignonresp and grossordnonresp columns
    df_summignonresp = df_summignonresp.reset_index()
    
    df_summignonresp = df_summignonresp.rename(columns = {'grossordnonresp' : 'grossinelresp'})
    
    # Calculate the grossed number of respondents over the non-response strata
    
    # Use only records in which NR_FLAG_PV is 0
    df_surveydata_sliced = df_surveydata.loc[df_surveydata[var_NRFlag] == 0]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=NRStratumDef)
    
    # Create two new columns as aggregations of SHIFT_WT
    df_sumresp = df_surveydata_sorted.groupby(NRStratumDef)\
            [var_PSW].agg({\
            var_grossResp : 'sum',
            var_respCount : 'count'})
    
    # Flattens the column structure after adding the new gross_resp and count_resps columns
    df_sumresp = df_sumresp.reset_index()
    
    # Calculate the grossed number of T&T non-respondents of the non-response strata    
    
    # Use only records from the survey dataset where the NR_FLAG_PV is 1, then sort    
    df_surveydata_sliced = df_surveydata.loc[df_surveydata[var_NRFlag] == 1]
    
    df_surveydata_sorted = df_surveydata_sliced.sort_values(by=NRStratumDef)     
    
    # Create new column using the sum of ShiftWt
    df_sumordnonresp = df_surveydata_sorted.groupby(NRStratumDef)\
            [var_PSW].agg({\
            'grossordnonresp' : 'sum'})
    
    # Flattens the column structure after adding the new grossordnonresp column
    df_sumordnonresp = df_sumordnonresp.reset_index()
    
    # Sort values in the three dataframes required for the next calculation
    df_sumordnonresp = df_sumordnonresp.sort_values(by=NRStratumDef)
    
    df_sumresp = df_sumresp.sort_values(by=NRStratumDef)
    
    df_summignonresp = df_summignonresp.sort_values(by=NRStratumDef)
     
    # Use the calculated data frames to calculate the non-response weight
    
    # Merge previously sorted dataframes into one, ensuring all rows from summignonresp are kept
    df_gnr = df_summignonresp.merge(df_sumresp, on=NRStratumDef, how = 'outer')
    
    df_gnr = df_gnr.sort_values(by=NRStratumDef)

    df_gnr = df_gnr.merge(df_sumordnonresp, on=NRStratumDef, how = 'left')
    
    # Replace all NaN values in columns with zero's
    df_gnr['grossmignonresp'].fillna(0, inplace =True)
    df_gnr['grossinelresp'].fillna(0, inplace =True)
    df_gnr['grossordnonresp'].fillna(0, inplace =True)
    
    # Add in two new columns with checks to prevent division by 0 
    df_gnr[var_gnr] = np.where(df_gnr[var_grossResp] != 0, df_gnr['grossordnonresp'] + df_gnr['grossmignonresp'] + df_gnr['grossinelresp'], 0)
    df_gnr[var_NRWeight] = np.where(df_gnr[var_grossResp] != 0, (df_gnr[var_gnr] + df_gnr[var_grossResp]) / df_gnr[var_grossResp], np.NaN)
    
    df_gross_resp_is_zero = df_gnr[df_gnr['gross_resp'] == 0]
    
    if(len(df_gross_resp_is_zero) > 0):
        logger.error('Error: Gross response is 0.')
    
    # Sort df_gnr and df_surveydata ready for producing summary
    df_surveydata_sorted = df_surveydata.sort_values(by=NRStratumDef)
    df_gnr = df_gnr.sort_values(by=NRStratumDef)

    # Ensure only complete or partial responses are kept
    df_surveydata_sorted = df_surveydata_sorted.loc[df_surveydata_sorted[var_NRFlag] == 0]
    
    # Produce summary by merging survey data and gnr data together, then sort
    df_out = df_surveydata_sorted.merge(df_gnr[NRStratumDef + [var_NRWeight]],
                                        on=NRStratumDef,
                                        how = 'left')

    df_out = df_out.sort_values(by=NRStratumDef)
    
    # Create and add three new columns calculated using SHIFT_WT
    df_summary = df_out.groupby(ShiftsStratumDef)[var_PSW].agg({var_meanSW : 'mean',\
                                                                var_respCount : 'count',\
                                                                var_priorSum : 'sum'})
    
    # Flatten column structure
    df_summary.reset_index(inplace = True)
    
    
    # Create and add one new column calculated using 'non_response_wt' in a 
    # different dataframe due to difficulty in creating all four new columns
    # simultaneously in a single dataframe
    df_summary_nr = df_out.groupby(ShiftsStratumDef)[var_NRWeight].agg({var_meanNRW : 'mean'})
    
    # Flatten column structure
    df_summary_nr.reset_index(inplace = True)
    
    # Merge all four new columns into the same dataframe
    df_summary = df_summary.merge(df_summary_nr, on = ShiftsStratumDef,
                                  how = 'outer')
    
    # Merge the updated dataframe with specific columns from GNR.
    df_summary = df_gnr[NRStratumDef + [var_gnr] + [var_grossResp] ].merge(df_summary,
                                  on = NRStratumDef, how='outer')
                                        
    # Calculate new non_response_wt value if condition is met
    df_out[var_NRWeight] = np.where(df_out[var_migFlag] == 0,
                                 (df_out[var_NRWeight] * df_out[var_TandTSI])
                                / df_out[var_migSI], df_out[var_NRWeight])
    
    # Perform data validation
    df_count_below_threshold = df_summary[df_summary['count_resps'] > 0]
    df_gnr_below_threshold = df_summary[df_summary['gnr'] > 0]
    
    df_merged_thresholds = df_count_below_threshold.merge(df_gnr_below_threshold
                                                          ,how = 'inner')

    df_merged_thresholds = df_merged_thresholds[df_merged_thresholds['count_resps'] < 30]
    
    df_merged_thresholds = df_merged_thresholds[['NR_PORT_GRP_PV'
                                                 , 'ARRIVEDEPART']]
    
    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_merged_thresholds.iterrows():
        threshold_string += "___||___" \
                         + df_merged_thresholds.columns[0] + " : " + str(record[0]) + " | "\
                         + df_merged_thresholds.columns[1] + " : " + str(record[1])
    if len(df_merged_thresholds) > 0:
        logger.warning('WARNING: Respondent count below minimum threshold for : ' 
                                     + threshold_string)
    
    # Reduce output to just key value pairs
    df_out = df_out[[var_serialNum, var_NRWeight]]
    
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
    
    global logger 
    logger = cf.database_logger()
     
    # Connect to Oracle
    conn = cf.get_oracle_connection()
    #parameters = cf.unload_parameters(136) - Currently not working, to be replaced
     
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_Non_Response_Weight"
     
    # Retrieve SAS data and load into dataframes
    path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
    path_to_nonresponse_data = root_data_path + r"\nonresponsedata_1.sas7bdat"
     
    global df_surveydata
    global df_nonresponsedata
     
    df_surveydata = pd.read_sas(path_to_survey_data)
    df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)
     
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_nonresponsedata.columns = df_nonresponsedata.columns.str.upper()
     
    print("Start - Calculate NonResponse Weight.")
    #do_ips_nrweight_calculation()
     
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


def calculate(SurveyData,NonResponseData,OutputData,SummaryData,ResponseTable,
              NRStratumDef,ShiftsStratumDef,var_NRtotals,var_NonMigTotals,var_SI,
              var_migSI,var_TandTSI,var_PSW,var_NRFlag,var_migFlag,var_respCount,
              var_NRWeight,var_meanSW,var_priorSum,var_meanNRW,var_grossResp,
              var_gnr,var_serialNum,minCountThresh):
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
    
    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_Non_Response_Weight"
    path_to_survey_data = root_data_path + r"\surveydata_1.sas7bdat"
    path_to_nonresponse_data = root_data_path + r"\nonresponsedata_1.sas7bdat"

    global df_surveydata
    global df_nonresponsedata  

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    #df_nonresponsedata = SAS7BDAT(path_to_nonresponse_data).to_data_frame()
    # This method is untested with a range of data sets but is faster
    #df_surveydata = pd.read_sas(path_to_survey_data)
    #df_nonresponsedata = pd.read_sas(path_to_nonresponse_data)
    
    # Import data via SQL
    df_surveydata = cf.get_table_values(SurveyData)
    df_nonresponsedata = cf.get_table_values(NonResponseData)
    
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_nonresponsedata.columns = df_nonresponsedata.columns.str.upper()

    print("Start - Calculate NonResponse Weight.")     
    weight_calculated_dataframes = do_ips_nrweight_calculation(SurveyData,NonResponseData,OutputData,SummaryData,ResponseTable,
                                                              NRStratumDef,ShiftsStratumDef,var_NRtotals,var_NonMigTotals,var_SI,
                                                              var_migSI,var_TandTSI,var_PSW,var_NRFlag,var_migFlag,var_respCount,
                                                              var_NRWeight,var_meanSW,var_priorSum,var_meanNRW,var_grossResp,
                                                              var_gnr,var_serialNum,minCountThresh)
     
    # Extract the two data sets returned from do_ips_nrweight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]
     
    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OutputData, surveydata_dataframe)
    cf.insert_dataframe_into_table(SummaryData, summary_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load NonResponse Weight calculation: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed NonResponse weight calculation.")
    cf.commit_to_audit_log("Create", "NonReponse", audit_message)
    print("Completed - Calculate NonResponse Weight")


if __name__ == '__main__':
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE',
              NonResponseData = 'SAS_NON_RESPONSE_DATA', 
              OutputData = 'SAS_NON_RESPONSE_WT', 
              SummaryData = 'SAS_PS_NON_RESPONSE', 
              ResponseTable = 'SAS_RESPONSE', 
              NRStratumDef = ['NR_PORT_GRP_PV', 
                              'ARRIVEDEPART'],  
              ShiftsStratumDef = ['NR_PORT_GRP_PV', 
                                  'ARRIVEDEPART',
                                  'WEEKDAY_END_PV'],   
              var_NRtotals = 'MIGTOTAL', 
              var_NonMigTotals = 'ORDTOTAL', 
              var_SI = '', 
              var_migSI = 'MIGSI', 
              var_TandTSI = 'TANDTSI', 
              var_PSW = 'SHIFT_WT', 
              var_NRFlag = 'NR_FLAG_PV', 
              var_migFlag = 'MIG_FLAG_PV', 
              var_respCount = 'COUNT_RESPS', 
              var_NRWeight = 'NON_RESPONSE_WT', 
              var_meanSW = 'MEAN_RESPS_SH_WT', 
              var_priorSum = 'PRIOR_SUM', 
              var_meanNRW = 'MEAN_NR_WT', 
              var_grossResp = 'GROSS_RESP', 
              var_gnr = 'GNR', 
              var_serialNum = 'SERIAL', 
              minCountThresh = '30')
            