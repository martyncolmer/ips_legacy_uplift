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


def do_ips_minweight_calculation(SurveyData, OutputData, SummaryData, ResponseTable, MinStratumDef,
              var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight,
              var_minCount, var_fullRespCount, var_minFlag, var_sumPriorWeightMin,
              var_sumPriorWeightFull, var_sumPriorWeightAll, var_sumPostWeight,
              var_casesCarriedForward, minCountThresh):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      :
    Parameters   : 
    Returns      : 
    Requirements : 
    Dependencies :
    """

    
    df_surveydata_new = df_surveydata[df_surveydata[var_shiftWeight].notnull() &
                                          (df_surveydata[var_NRWeight].notnull())]
    
    df_surveydata_new['SWNRwght'] = df_surveydata_new[var_shiftWeight] * df_surveydata_new[var_NRWeight]
    
    df_surveydata_sorted = df_surveydata_new.sort_values(MinStratumDef)
    
    # Summarise the minimum responses by the strata
    df_mins = df_surveydata_sorted[df_surveydata_sorted[var_minFlag] == 1]
    
    df_summin = df_mins.groupby(MinStratumDef)\
            ['SWNRwght'].agg({\
            var_sumPriorWeightMin : 'sum',
            var_minCount : 'count'})
    
    df_summin.reset_index(inplace = True)
    
    # Summarise only full responses by strata
    df_fulls = df_surveydata_sorted[df_surveydata_sorted[var_minFlag] == 0]
    
    df_sumfull = df_fulls.groupby(MinStratumDef)\
            ['SWNRwght'].agg({\
            var_sumPriorWeightFull : 'sum',
            var_fullRespCount : 'count'})
    
    df_sumfull.reset_index(inplace = True)
    
    # Summarise the mig slot interviews by the strata
    df_migs = df_surveydata_sorted[df_surveydata_sorted[var_minFlag] == 2]
    
    df_summig = df_migs.groupby(MinStratumDef)\
            ['SWNRwght'].agg({\
            'sumPriorWeightMigs' : 'sum'})
    
    df_summig.reset_index(inplace = True)
    
    # Calculate the minimum weight by the strata
    df_summin.sort_values(MinStratumDef)
    df_sumfull.sort_values(MinStratumDef)
    df_summig.sort_values(MinStratumDef)
    
    df_summary = pd.merge(df_sumfull, df_summig, on = MinStratumDef,
                          how = 'outer')
    
    df_summary = df_summary.merge(df_summin, on = MinStratumDef,
                                  how = 'outer')
    
    df_check_prior_gross_fulls = df_summary[df_summary[var_sumPriorWeightFull] <= 0]
    
    if(df_check_prior_gross_fulls.empty == False  & df_summin.empty == False):
        cf.database_logger().error('Error: No complete or partial responses')
    else:
        df_summary[var_minWeight] = np.where(df_summary[var_sumPriorWeightFull] > 0,
                                         (df_summary[var_sumPriorWeightMin] + 
                                         df_summary[var_sumPriorWeightFull]) /
                                         df_summary[var_sumPriorWeightFull],
                                         1)
    
    # Replace missing values with 0
    df_summary[var_sumPriorWeightMin].fillna(0, inplace = True)
    df_summary[var_sumPriorWeightFull].fillna(0, inplace = True)
    df_summary['sumPriorWeightMigs'].fillna(0, inplace = True)
    
    df_summary[var_sumPriorWeightAll] = df_summary[var_sumPriorWeightMin] + \
                                    df_summary[var_sumPriorWeightFull] + \
                                    df_summary['sumPriorWeightMigs']
    
    df_summary = df_summary.sort_values(MinStratumDef)
    
    df_summary[var_minWeight] = np.where(df_summary[var_sumPriorWeightFull] > 0,
                                     (df_summary[var_sumPriorWeightMin] +
                                     df_summary[var_sumPriorWeightFull]) /
                                     df_summary[var_sumPriorWeightFull],
                                     df_summary[var_minWeight])
    
    df_out = df_summary.merge(df_surveydata_sorted, on = MinStratumDef,
                              how = 'outer')
    
    # Set mins_wt to either 0 or 1 conditionally, then calculate the postweight value
    df_out[var_minWeight] = np.where(df_out[var_minFlag] == 1.0, 0, df_out[var_minWeight])
    df_out[var_minWeight] = np.where(df_out[var_minFlag] == 2.0, 1, df_out[var_minWeight])
    df_out['SWNRMINwght'] = df_out[var_shiftWeight] * \
                            df_out[var_NRWeight] * \
                            df_out[var_minWeight]
    
    df_out_sliced = df_out[df_out[var_minFlag] != 1]
    df_postsum = df_out_sliced.groupby(MinStratumDef)\
            ['SWNRMINwght'].agg({\
            var_sumPostWeight : 'sum',
            var_casesCarriedForward : 'count'})
    
    df_postsum.reset_index(inplace = True)
    
    df_postsum.sort_values(MinStratumDef)
    
    # Merge the updated dataframe with specific columns from GNR.
    df_summary = df_summary.merge(df_postsum, on = MinStratumDef, how='outer')
    
    df_summary.drop([var_sumPostWeight, var_casesCarriedForward], axis=1, inplace=True) 
    
    # Perform data validation
    df_fulls_below_threshold = df_summary[df_summary[var_fullRespCount] < 30]
    df_mins_below_threshold = df_summary[df_summary[var_minCount] > 0]
    
    df_merged_thresholds = df_fulls_below_threshold.merge(df_mins_below_threshold
                                                          ,how = 'inner')
    df_merged_thresholds = df_merged_thresholds[MinStratumDef]
    
    # Collect data outside of specified threshold
    threshold_string = ""
    for index, record in df_merged_thresholds.iterrows():
        threshold_string += "___||___" \
                         + df_merged_thresholds.columns[0] + " : " + str(record[0]) + " | "\
                         + df_merged_thresholds.columns[1] + " : " + str(record[1])
    if len(df_merged_thresholds) > 0:
        cf.database_logger().warning('WARNING: Minimums weight outside thresholds for: ' 
                                     + threshold_string)   
    
    df_out = df_out[[var_serialNum, var_minWeight]]
    
    return (df_out, df_summary)
    

def calculate(SurveyData, OutputData, SummaryData, ResponseTable, MinStratumDef,
              var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight,
              var_minCount, var_fullRespCount, var_minFlag, var_sumPriorWeightMin,
              var_sumPriorWeightFull, var_sumPriorWeightAll, var_sumPostWeight,
              var_casesCarriedForward, minCountThresh):
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      : Performs the setup required for the calculation function, then
                   calls the function
    Parameters   : 
    Returns      : 
    Requirements : 
    Dependencies :
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Setup path to the base directory containing data files
    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate Minimums Weight"
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


    print("Start - Calculate Minimums Weight")
    weight_calculated_dataframes = do_ips_minweight_calculation(SurveyData, OutputData, SummaryData, ResponseTable, MinStratumDef,
                                                                var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight,
                                                                var_minCount, var_fullRespCount, var_minFlag, var_sumPriorWeightMin,
                                                                var_sumPriorWeightFull, var_sumPriorWeightAll, var_sumPostWeight,
                                                                var_casesCarriedForward, minCountThresh)

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # Append the generated data to output tables
    cf.insert_dataframe_into_table(OutputData, surveydata_dataframe)
    cf.insert_dataframe_into_table(SummaryData, summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Minimums Weight calculation: %s()" %function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Minimums weight calculation.")
    cf.commit_to_audit_log("Create", "MinimumsWeight", audit_message)
    print("Completed - Calculate Minimums Weight")


if __name__ == '__main__':
    calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE',
              OutputData = 'SAS_MINIMUMS_WT', 
              SummaryData = 'SAS_PS_MINIMUMS', 
              ResponseTable = 'SAS_RESPONSE', 
              MinStratumDef = ['MINS_PORT_GRP_PV',
                               'MINS_CTRY_GRP_PV'],
              var_serialNum = 'SERIAL', 
              var_shiftWeight = 'SHIFT_WT',
              var_NRWeight = 'NON_RESPONSE_WT', 
              var_minWeight = 'MINS_WT',
              var_minCount = 'MINS_CASES',
              var_fullRespCount = 'FULLS_CASES',
              var_minFlag = 'MINS_FLAG_PV',
              var_sumPriorWeightMin = 'PRIOR_GROSS_MINS',
              var_sumPriorWeightFull = 'PRIOR_GROSS_FULLS',
              var_sumPriorWeightAll = 'PRIOR_GROSS_ALL',
              var_sumPostWeight = 'POST_SUM',
              var_casesCarriedForward = 'CASES_CARRIED_FWD',
              minCountThresh = '30')