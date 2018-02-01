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
            'mins_cases' : 'count'})
    
    df_summin.reset_index(inplace = True)
    
    # Summarise only full responses by strata
    df_fulls = df_surveydata_sorted[df_surveydata_sorted['MINS_FLAG_PV'] == 0]
    
    df_sumfull = df_fulls.groupby(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])\
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
    
    df_summary = pd.merge(df_sumfull, df_summig, on = ['MINS_PORT_GRP_PV',
                                                              'MINS_CTRY_GRP_PV'],
                          how = 'outer')
    
    df_summary = df_summary.merge(df_summin, on = ['MINS_PORT_GRP_PV',
                                                              'MINS_CTRY_GRP_PV'],
                                  how = 'outer')
    
    df_summary['mins_wt'] = np.where(df_summary['prior_gross_fulls'] > 0,
                                     (df_summary['prior_gross_mins'] + 
                                     df_summary['prior_gross_fulls']) /
                                     df_summary['prior_gross_fulls'],
                                     1)
    
    # Replace missing values with 0
    df_summary['prior_gross_mins'].fillna(0, inplace = True)
    df_summary['prior_gross_fulls'].fillna(0, inplace = True)
    df_summary['sumPriorWeightMigs'].fillna(0, inplace = True)
    
    df_summary['prior_gross_all'] = df_summary['prior_gross_mins'] + \
                                    df_summary['prior_gross_fulls'] + \
                                    df_summary['sumPriorWeightMigs']
    
    df_summary = df_summary.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    
    df_summary['mins_wt'] = np.where(df_summary['prior_gross_fulls'] > 0,
                                     (df_summary['prior_gross_mins'] +
                                     df_summary['prior_gross_fulls']) /
                                     df_summary['prior_gross_fulls'],
                                     df_summary['mins_wt'])
    
    df_out = df_summary.merge(df_surveydata_sorted, on = ['MINS_PORT_GRP_PV',
                                                              'MINS_CTRY_GRP_PV'],
                              how = 'outer')
    
    # Set mins_wt to either 0 or 1 conditionally, then calculate the postweight value
    df_out['mins_wt'] = np.where(df_out['MINS_FLAG_PV'] == 1.0, 0, df_out['mins_wt'])
    df_out['mins_wt'] = np.where(df_out['MINS_FLAG_PV'] == 2.0, 1, df_out['mins_wt'])
    df_out['SWNRMINwght'] = df_out['SHIFT_WT'] * \
                            df_out['NON_RESPONSE_WT'] * \
                            df_out['mins_wt']
    
    df_out_sliced = df_out[df_out['MINS_FLAG_PV'] != 1]
    df_postsum = df_out_sliced.groupby(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])\
            ['SWNRMINwght'].agg({\
            'post_sum' : 'sum',
            'cases_carried_forward' : 'count'})
    
    df_postsum.reset_index(inplace = True)
    
    df_postsum.sort_values(['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    
    # Merge the updated dataframe with specific columns from GNR.
    df_summary = df_summary.merge(df_postsum,
                                  on = ['MINS_PORT_GRP_PV',
                                        'MINS_CTRY_GRP_PV'], how='outer')
    
    df_summary.drop(['post_sum', 'cases_carried_forward'], axis=1, inplace=True) 
    
    df_out = df_out[['SERIAL', 'mins_wt']]
    
    return (df_out, df_summary)
    

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
    #parameters = cf.unload_parameters(205)

    # Load SAS file into dataframe (this data will come from Oracle eventually)

    root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate Minimums Weight"
    path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"

    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()

    # This method is untested with a range of data sets but is faster
    global df_surveydata
    df_surveydata = pd.read_sas(path_to_survey_data)

    print("Start - Calculate Minimums Weight")
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
    audit_message = "Load Minimums Weight calculation: %s()" %function_name

    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed Minimums weight calculation.")
    cf.commit_to_audit_log("Create", "MinimumsWeight", audit_message)
    print("Completed - Calculate Minimums Weight")


if __name__ == '__main__':
    calc_minimums_weight()