'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_minimums_weight import do_ips_minweight_calculation


def test_calculate():
    # This is an integration test as it runs the entire step

    # Import the test data
    test_survey = pd.read_pickle('../data/minimums_input.pkl')

    # Set the imported columns to be uppercase
    test_survey.columns = test_survey.columns.str.upper()

    # Run the test
    output_dataframes = do_ips_minweight_calculation(df_surveydata=test_survey,
                                               OutputData='SAS_MINIMUMS_WT',
                                               SummaryData='SAS_PS_MINIMUMS',
                                               ResponseTable='SAS_RESPONSE',
                                               MinStratumDef=['MINS_PORT_GRP_PV',
                                                              'MINS_CTRY_GRP_PV'],
                                               var_serialNum='SERIAL',
                                               var_shiftWeight='SHIFT_WT',
                                               var_NRWeight='NON_RESPONSE_WT',
                                               var_minWeight='MINS_WT',
                                               var_minCount='MINS_CASES',
                                               var_fullRespCount='FULLS_CASES',
                                               var_minFlag='MINS_FLAG_PV',
                                               var_sumPriorWeightMin='PRIOR_GROSS_MINS',
                                               var_sumPriorWeightFull='PRIOR_GROSS_FULLS',
                                               var_sumPriorWeightAll='PRIOR_GROSS_ALL',
                                               var_sumPostWeight='POST_SUM',
                                               var_casesCarriedForward='CASES_CARRIED_FWD',
                                               minCountThresh='30')
    output_data = output_dataframes[0]
    summary_data = output_dataframes[1]
    # Import the expected result
    test_output = pd.read_pickle('../data/minimums_output.pkl')
    test_summary = pd.read_pickle('../data/minimums_summary.pkl')
    # Set the imported columns to be uppercase
    test_output.columns = test_output.columns.str.upper()
    test_summary.columns = test_summary.columns.str.upper()
    test_summary = test_summary[summary_data.columns.str.upper()]
    test_summary.drop(['_TYPE_', '_FREQ_'], axis=1)

    test_summary.index = range(0, len(test_summary))
    summary_data.index = range(0, len(summary_data))

    # Check the results match
    assert_frame_equal(output_data, test_output)
    assert_frame_equal(summary_data, test_summary)


if __name__ == '__main__':
    test_calculate()
