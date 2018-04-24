'''
Created on 12 Mar 2018

@author: thorne1
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations import calculate_ips_spend_imputation as spend


def test_calculate():
    # This is an integration test as it runs the entire step
    test_survey = pd.read_pickle('../data/spend_imp_surveydata.pkl')

    # Retrieve Python output data
    py_survey_data = spend.do_ips_spend_imputation(test_survey, var_serial="SERIAL", measure="mean")
    # Cleanse
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    # Retrieve SAS Survey Data output and cleanse
    test_result_survey = pd.read_pickle('../data/spend_imp_output_merge_eligible.pkl')
    test_result_survey.columns = test_result_survey.columns.str.upper()
    test_result_survey = test_result_survey.sort_values(by='SERIAL')
    test_result_survey.index = range(0, len(test_result_survey))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data
                       , test_result_survey
                       , check_column_type=False)
