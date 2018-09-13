'''
Created on 10 May 2018

@author: thorne1
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal

from main.calculations import calculate_ips_town_and_stay_expenditure as tse
import tests.config

path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH

def test_calculate():
    # This is an integration test as it runs the entire step
    test_survey = pd.read_pickle(path_to_data + '/town_and_stay/town_and_stay.pkl')

    # Retrieve Python output data and cleanse
    py_survey_data = tse.do_ips_town_exp_imp(df_survey_data=test_survey,
                                                 var_serial="SERIAL",
                                                 var_final_wt="FINAL_WT")
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    # Retrieve SAS Survey Data output and cleanse
    test_result_survey = pd.read_pickle(path_to_data + '/import/output/post_import_TOWN_AND_STAY.pkl')
    test_result_survey.columns = test_result_survey.columns.str.upper()
    test_result_survey = test_result_survey.sort_values(by='SERIAL')
    test_result_survey.index = range(0, len(test_result_survey))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey, check_column_type=False)

