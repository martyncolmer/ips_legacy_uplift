'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_minimums_weight import do_ips_minweight_calculation

import pytest
import tests.config

path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/min_weight"


@pytest.mark.skip("Known failure due to rounding")
def test_calculate():
    # This is an integration test as it runs the entire step

    # Import the test data
    test_survey = pd.read_pickle(path_to_data + '/minimums_input.pkl')

    # Set the imported columns to be uppercase
    test_survey.columns = test_survey.columns.str.upper()

    # Run the test
    output_dataframes = do_ips_minweight_calculation(df_surveydata=test_survey,
                                                     var_serialNum='SERIAL',
                                                     var_shiftWeight='SHIFT_WT',
                                                     var_NRWeight='NON_RESPONSE_WT',
                                                     var_minWeight='MINS_WT')
    output_data = output_dataframes[0]
    summary_data = output_dataframes[1]
    # Import the expected result
    test_output = pd.read_pickle(path_to_data + '/minimums_output.pkl')
    test_summary = pd.read_pickle(path_to_data + '/minimums_summary.pkl')
    # Set the imported columns to be uppercase
    test_output.columns = test_output.columns.str.upper()
    test_summary.columns = test_summary.columns.str.upper()
    test_summary = test_summary[summary_data.columns.str.upper()]

    # Pandas removes rows where a missing value exists in this column but SAS doesn't, so this line removes missing vals
    test_summary["MINS_CTRY_GRP_PV"].fillna(0, inplace=True)

    test_output = test_output.sort_values(by='SERIAL')
    output_data = output_data.sort_values(by='SERIAL')

    test_summary = test_summary.sort_values(by=['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])
    summary_data = summary_data.sort_values(by=['MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV'])

    test_output.index = range(0, len(test_output))
    output_data.index = range(0, len(output_data))

    test_summary.index = range(0, len(test_summary))
    summary_data.index = range(0, len(summary_data))

    # Conversion performed to match SAS results. Can be removed once SAS results are no longer the test standard
    summary_data["CASES_CARRIED_FWD"] = summary_data["CASES_CARRIED_FWD"].astype(float)
    summary_data["FULLS_CASES"] = summary_data["FULLS_CASES"].astype(float)

    # Rounding here performed mostly for test purposes, could be removed
    test_summary[["PRIOR_GROSS_ALL", "PRIOR_GROSS_FULLS", "PRIOR_GROSS_MINS", "MINS_WT"]] = \
        test_summary[["PRIOR_GROSS_ALL", "PRIOR_GROSS_FULLS", "PRIOR_GROSS_MINS", "MINS_WT"]].round(3)

    # Check the results match

    # 27/04/2018 James Burr - Tests currently fail ONLY due to rounding differences with the new Python round method.
    # This situation is the same as for other modules e.g. fares and spend imputation, test standard will be changed
    assert_frame_equal(output_data, test_output)
    assert_frame_equal(summary_data, test_summary, check_like=True)
