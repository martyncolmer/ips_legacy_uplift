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


def do_ips_minweight_calculation():
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      :
    Parameters   : 
    Returns      : 
    Requirements : 
    Dependencies :
    """

    
    df_surveydata_new = df_surveydata[df_surveydata['SHIFT_WT'].notnull() &
                                          (df_surveydata['NON_RESPONSE_WT'].notnull())]
    
    df_surveydata_new['SWNRwght'] = df_surveydata_new['SHIFT_WT'] * df_surveydata_new['NON_RESPONSE_WT']
    
    df_surveydata_sorted = df_surveydata_new.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    
    # Summarise the minimum responses by the strata
    df_mins = df_surveydata_sorted[df_surveydata_sorted['MINS_FLAG_PV'] == 1]
    
    df_summin = df_mins.groupby(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])\
            ['SWNRwght'].agg({\
            'prior_gross_mins' : 'sum',
            'v' : 'count'})
    
    df_summin.reset_index(inplace = True)
    
    # Summarise only full responses by strata
    df_fulls = df_surveydata_sorted[df_surveydata_sorted['MINS_FLAG_PV'] == 0]
    
    df_sumfull = df_mins.groupby(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])\
            ['SWNRwght'].agg({\
            'prior_gross_fulls' : 'sum',
            'fulls_cases' : 'count'})
    
    df_sumfull.reset_index(inplace = True)
    
    # Summarise the mig slot interviews by the strata
    df_migs = df_surveydata_sorted[df_surveydata_sorted['MINS_FLAG_PV'] == 2]
    
    df_summig = df_migs.groupby(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])\
            ['SWNRwght'].agg({\
            'sumPriorWeightMigs' : 'sum'})
    
    df_summig.reset_index(inplace = True)
    
    # Calculate the minimum weight by the strata
    df_summin.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    df_sumfull.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    df_summig.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    
    df_summary = df_summin.merge(df_sumfull, df_summig, how = 'outer')
    
    # Replace missing values with 0
    df_summary['prior_gross_mins'].fillna(0, inplace =True)
    df_summary['prior_gross_fulls'].fillna(0, inplace =True)
    df_summary['sumPriorWeightMigs'].fillna(0, inplace =True)
    
    df_summary['prior_gross_all'] = df_summary['prior_gross_mins'] + \
                                    df_summary['prior_gross_fulls'] + \
                                    df_summary['sumPriorWeightMigs']
    
    
    

def calc_minimums_weight():
    """
    Author       : James Burr
    Date         : Jan 2018
    Purpose      :
    Parameters   : 
    Returns      : 
    Requirements : 
    Dependencies :
    """

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Connect to Oracle and unload parameter list
    conn = cf.get_oracle_connection()
    global parameters
    parameters = cf.unload_parameters(205)

    # Load SAS file into dataframe (this data will come from Oracle eventually)

    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate Minimums Weight"
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()

    # This method is untested with a range of data sets but is faster
    global df_surveydata
    df_surveydata = pd.read_sas(path_to_survey_data)

    print("Start - Calculate Shift Weight")
    weight_calculated_dataframes = do_ips_minweight_calculation()

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # Output to Excel for show and tell SAS comparison
    surveydata_dataframe.to_csv('surveydata_dataframe.csv')
    summary_dataframe.to_csv('summary_dataframe.csv')

    # Append the generated data to output tables
    cf.insert_into_table_many(parameters['OutputData'], surveydata_dataframe)
    cf.insert_into_table_many(parameters['SummaryData'], summary_dataframe)

    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load Shift Weight calculation: %s()" %function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Shift weight calculation.")
    cf.commit_to_audit_log("Create", "ShiftWeigh", audit_message)
    print("Completed - Calculate Shift Weight")


if __name__ == '__main__':
    calc_minimums_weight()