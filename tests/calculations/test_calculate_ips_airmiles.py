'''
Created on 05 March 2018

@author: Thomas Mahoney
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_airmiles import do_ips_airmiles_calculation


def test_calculate():
    # This is an integration test as it runs the entire step

    # Import the test data
    test_survey = pd.read_pickle('tests/data/airmiles_input.pkl')

    # Set the imported columns to be uppercase
    test_survey.columns = test_survey.columns.str.upper()

    # Run the test
    output_data = do_ips_airmiles_calculation(df_surveydata=test_survey,
                                              var_serial='SERIAL')

    # Import the expected result
    test_result = pd.read_pickle('tests/data/airmiles_output.pkl')

    # Set the imported columns to be uppercase
    test_result.columns = test_result.columns.str.upper()

    # Check the results match
    assert_frame_equal(output_data, test_result)
