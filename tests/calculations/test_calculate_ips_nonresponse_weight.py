'''
Created on 17th April 2018

@author: James Burr
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal

from main.calculations import calculate_ips_nonresponse_weight as non_resp

path_to_data = r"tests/data/calculations/october_2017/non_response_weight"


def test_calculate():
    # This is an integration test as it runs the entire step
    test_survey = pd.read_pickle(path_to_data + '/non_response_survey_data.pkl')
    test_nr_data = pd.read_pickle(path_to_data + '/non_response_data.pkl')
    result_py_data = non_resp.do_ips_nrweight_calculation(test_survey, test_nr_data,
                                                          'NON_RESPONSE_WT', 'SERIAL')

    # Retrieve and clean Python survey data output
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    # From Python output data, retrieve Summary Data and cleanse
    py_summary_data = result_py_data[1]
    py_summary_data.index = range(0, len(py_summary_data))

    # Retrieve SAS Survey Data output and cleanse
    test_result_survey = pd.read_pickle(path_to_data + '/non_response_weight_output.pkl')
    test_result_survey.columns = test_result_survey.columns.str.upper()
    test_result_survey = test_result_survey.sort_values(by='SERIAL')
    test_result_survey.index = range(0, len(test_result_survey))
    # test_result_survey.replace("", np.NaN, inplace=True)

    # Retrieve SAS Summary Data output and cleanse
    test_result_summary = pd.read_pickle(path_to_data + '/non_response_weight_summary.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    test_result_summary.index = range(0, len(test_result_summary))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey, check_dtype=False, check_like=True)
    assert_frame_equal(py_summary_data, test_result_summary, check_dtype=False, check_like=True)
