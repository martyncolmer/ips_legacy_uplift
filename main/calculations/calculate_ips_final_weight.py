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


def do_ips_final_wt_calculation(SurveyData,OutputData,SummaryData,ResponseTable
                                ,var_serialNum,var_shiftWeight,var_NRWeight
                                ,var_minWeight,var_trafficWeight,var_unsampWeight
                                ,var_imbWeight,var_finalWeight,var_recordsDisplayed):
    """
    Author       : James Burr
    Date         : 15 Feb 2018
    Purpose      : Generates the IPS Final Weight value
    Parameters   : SurveyData - the IPS survey records for the relevant period
                   OutputData - Oracle table to hold the output data
                   SummaryData - Oracle tale to hold the output summary
                   var_serialNum - Variable holding the serial number for the record
                   var_shiftWeight - Variable holding the name of the shift weight field
                   var_NRWeight - Variable holding the name of the nr weight field
                   var_minWeight - Variable holding the name of the min weight field
                   var_trafficWeight - Variable holding the name of the traffic weight field
                   var_unsampWeight - Variable holding the name of the unsampled weight field
                   var_imbWeight - Variable holding the name of the imbalance weight field
                   var_finalWeight - Variable holding the name of the final weight field
                   var_recordsDisplayed - Number of records to display
    Returns      : Dataframes - df_summary(dataframe containing random sample of rows)
                   ,df_output(dataframe containing serial number and calculated final weight)
    Requirements : NA
    Dependencies : NA
    """
    
    # Calculate the final weight value in a new column
    
    df_final_weight = df_surveydata
    
    df_final_weight[var_finalWeight] = df_final_weight[var_shiftWeight] * \
                                         df_final_weight[var_NRWeight] * \
                                         df_final_weight[var_minWeight] * \
                                         df_final_weight[var_trafficWeight] * \
                                         df_final_weight[var_unsampWeight] * \
                                         df_final_weight[var_imbWeight]

    # Generate summary output
    totalObs = len(df_final_weight.index)
    
    df_summary = df_final_weight[[var_serialNum, var_shiftWeight, var_NRWeight
                                  ,var_minWeight, var_trafficWeight, var_unsampWeight
                                  ,var_imbWeight, var_finalWeight]]
    
    # Sort summary, then select var_recordsDisplayed number of random rows for
    # inclusion in the summary dataset
    df_summary = df_summary.sample(var_recordsDisplayed)
    
    df_summary = df_summary.sort_values(var_serialNum)
    
    # Condense output dataset to the two required variables
    df_output = df_final_weight[[var_serialNum, var_finalWeight]]
    
    return (df_output, df_summary)
    
    
def calculate(SurveyData,OutputData,SummaryData,ResponseTable,var_serialNum
              ,var_shiftWeight,var_NRWeight,var_minWeight,var_trafficWeight
              ,var_unsampWeight,var_imbWeight,var_finalWeight,var_recordsDisplayed):
    """
    Author       : James Burr
    Date         : 15 Feb 2018
    Purpose      :
    Parameters   : 
    Returns      : 
    Requirements : 
    Dependencies :
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Final Weighting"
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    global df_surveydata

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
    
    # This method is untested with a range of data sets but is faster
    df_surveydata = pd.read_sas(path_to_survey_data)

    # Import data via SQL
    #df_surveydata = cf.get_table_values(SurveyData)

    df_surveydata.columns = df_surveydata.columns.str.upper()

    print("Start - Calculate Final Weight")
    weight_calculated_dataframes = do_ips_final_wt_calculation(df_surveydata
                                                               ,OutputData
                                                               ,SummaryData
                                                               ,ResponseTable
                                                               ,var_serialNum
                                                               ,var_shiftWeight
                                                               ,var_NRWeight
                                                               ,var_minWeight
                                                               ,var_trafficWeight
                                                               ,var_unsampWeight
                                                               ,var_imbWeight
                                                               ,var_finalWeight
                                                               ,var_recordsDisplayed)

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # Append the generated data to output tables
    cf.insert_into_table_many(OutputData, surveydata_dataframe)
    cf.insert_into_table_many(SummaryData, summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Final Weight calculation: %s()" %function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Final weight calculation.")
    cf.commit_to_audit_log("Create", "FinalWeight", audit_message)
    
    print("Completed - Calculate Final Weight")


if (__name__ == '__main__'):
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE'
              ,OutputData = 'SAS_FINAL_WT'
              ,SummaryData = 'SAS_PS_FINAL'
              ,ResponseTable = 'SAS_RESPONSE'
              ,var_serialNum = 'SERIAL'
              ,var_shiftWeight = 'SHIFT_WT'
              ,var_NRWeight = 'NON_RESPONSE_WT'
              ,var_minWeight = 'MINS_WT'
              ,var_trafficWeight = 'TRAFFIC_WT'
              ,var_unsampWeight = 'UNSAMP_TRAFFIC_WT'
              ,var_imbWeight = 'IMBAL_WT'
              ,var_finalWeight = 'FINAL_WT'
              ,var_recordsDisplayed = 20)
    