import sys
import os
import logging
import inspect
import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict
from sas7bdat import SAS7BDAT
import survey_support
from main.io import CommonFunctions as cf
path_to_data = r"../../tests/data/final_weight"
path_to_data_ns3 = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Final Weighting"

def do_ips_final_wt_calculation(df_surveydata, OutputData, SummaryData, ResponseTable
                                , var_serialNum, var_shiftWeight, var_NRWeight
                                , var_minWeight, var_trafficWeight, var_unsampWeight
                                , var_imbWeight, var_finalWeight, var_recordsDisplayed):
    """
    Author       : James Burr / Nassir Mohammad
    Date         : 17 Apr 2018
    Purpose      : Generates the IPS Final Weight value
    Parameters   : df_surveydata - the IPS survey records for the relevant period
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

    # test - Start
    path_to_test = path_to_data_ns3 + r"/output_set_in.sas7bdat"
    test_df = SAS7BDAT(path_to_test).to_data_frame()
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(df_final_weight, test_df, check_dtype=False)
    # test - End

    # Generate summary output
    df_summary = df_final_weight[[var_serialNum, var_shiftWeight, var_NRWeight
        , var_minWeight, var_trafficWeight, var_unsampWeight
        , var_imbWeight, var_finalWeight]]

    # cannot test since we have random sample
    # path_to_test = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Final Weighting" + r"/summary_seed_rank.sas7bdat"
    # test_df = SAS7BDAT(path_to_test).to_data_frame()
    # test_df.columns = test_df.columns.str.upper()
    # assert_frame_equal(df_summary, test_df, check_dtype=False)


    # Sort summary, then select var_recordsDisplayed number of random rows for
    # inclusion in the summary dataset
    df_summary = df_summary.sample(var_recordsDisplayed)

    # # cannot test since we have random sample
    # path_to_test = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Final Weighting" + r"/summary_rank_sort.sas7bdat"
    # test_df = SAS7BDAT(path_to_test).to_data_frame()
    # test_df.columns = test_df.columns.str.upper()
    # assert_frame_equal(df_summary, test_df, check_dtype=False)

    df_summary = df_summary.sort_values(var_serialNum)

    # cannot test since we have random sample
    # path_to_test = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Final Weighting" + r"/summary_if_le.sas7bdat"
    # test_df = SAS7BDAT(path_to_test).to_data_frame()
    # test_df.columns = test_df.columns.str.upper()
    # assert_frame_equal(df_summary, test_df, check_dtype=False)

    # Condense output dataset to the two required variables
    df_output = df_final_weight[[var_serialNum, var_finalWeight]]

    # test - Start
    path_to_test = path_to_data_ns3 + r"/output_keep_serial_finalweight.sas7bdat"
    test_df = SAS7BDAT(path_to_test).to_data_frame()
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(df_output, test_df, check_dtype=False)
    # test - End

    return (df_output, df_summary)


def calculate(SurveyData, OutputData, SummaryData, ResponseTable, var_serialNum
              , var_shiftWeight, var_NRWeight, var_minWeight, var_trafficWeight
              , var_unsampWeight, var_imbWeight, var_finalWeight, var_recordsDisplayed):
    """
    Author       : James Burr / Nassir Mohammad
    Date         : 17 Apr 2018
    Purpose      : Calculates the IPS Final Weight
    Parameters   : SurveyData = the IPS survey records for the period
				   OutputData = Oracle table to hold the output data
				   SummaryData = Oracle table to hold the output summary
				   ResponseTable = Oracle table to hold response information (status etc.)
				   var_SerialNum = Variable holding the record serial number (UID)
				   var_shiftWeight = Variable holding the name of the shift weight field
				   var_NRWeight = Variable holding the name of the NR weight field
				   var_minWeight = Variable holding the name of the min weight field
				   var_trafficWeight = Variable holding the name of the traffic wht field
			       var_OOHWeight = Variable holding the name of the OOH weight field
				   var_imbWeight = Variable holding the name of the imbalance weight field
				   var_finalWeight = Variable holding the name of the final weight field
				   var_recordsDisplayed = number of records to display in summary output
    Returns      : dataframe tuple: (surveydata_dataframe, summary_dataframe)
    Requirements : do_ips_final_wt_calculation()
    Dependencies :
    """

    print("Start - Calculate Final Weight")

    # import data set

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Setup path to the base directory containing data files
    path_to_survey_data = path_to_data + r"/surveydata.pkl"

    df_surveydata = pd.read_pickle(path_to_survey_data)

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()

    # This method is untested with a range of data sets but is faster
    # df_surveydata = pd.read_sas(path_to_survey_data)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)

    df_surveydata.columns = df_surveydata.columns.str.upper()

    # test - Start
    path_to_test = path_to_data_ns3 + r"/surveydata.sas7bdat"
    test_df = SAS7BDAT(path_to_test).to_data_frame()
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(df_surveydata, test_df, check_dtype=False)
    # test - End

    print("Start - do_ips_final_wt_calculation()")
    weight_calculated_dataframes = do_ips_final_wt_calculation(df_surveydata
                                                               , OutputData
                                                               , SummaryData
                                                               , ResponseTable
                                                               , var_serialNum
                                                               , var_shiftWeight
                                                               , var_NRWeight
                                                               , var_minWeight
                                                               , var_trafficWeight
                                                               , var_unsampWeight
                                                               , var_imbWeight
                                                               , var_finalWeight
                                                               , var_recordsDisplayed)

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # re-index
    surveydata_dataframe.index = range(surveydata_dataframe.shape[0])
    summary_dataframe.index = range(summary_dataframe.shape[0])

    # test - Start
    # test surveydata_dataframe only
    path_to_test = path_to_data_ns3 + r"/output_final.sas7bdat"
    test_df = SAS7BDAT(path_to_test).to_data_frame()
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(surveydata_dataframe, test_df, check_dtype=False)
    # test - End

    # note:  we cannot test summary_dataframe against SAS original output since we take random samples

    return (surveydata_dataframe, summary_dataframe)

    # TODO - move code below when main refactored
    # # Append the generated data to output tables
    # cf.insert_dataframe_into_table(OutputData, surveydata_dataframe)
    # cf.insert_dataframe_into_table(SummaryData, summary_dataframe)
    #
    # # Retrieve current function name using inspect:
    # # 0 = frame object, 3 = function name.
    # # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    # function_name = str(inspect.stack()[0][3])
    # audit_message = "Load Final Weight calculation: %s()" %function_name
    #
    # # Log success message in SAS_RESPONSE and AUDIT_LOG
    # cf.database_logger().info("SUCCESS - Completed Final weight calculation.")
    # cf.commit_to_audit_log("Create", "FinalWeight", audit_message)

    #print("Completed - Calculate Final Weight")


if (__name__ == '__main__'):
    calculate(SurveyData='SAS_SURVEY_SUBSAMPLE'
              , OutputData='SAS_FINAL_WT'
              , SummaryData='SAS_PS_FINAL'
              , ResponseTable='SAS_RESPONSE'
              , var_serialNum='SERIAL'
              , var_shiftWeight='SHIFT_WT'
              , var_NRWeight='NON_RESPONSE_WT'
              , var_minWeight='MINS_WT'
              , var_trafficWeight='TRAFFIC_WT'
              , var_unsampWeight='UNSAMP_TRAFFIC_WT'
              , var_imbWeight='IMBAL_WT'
              , var_finalWeight='FINAL_WT'
              , var_recordsDisplayed=20)
