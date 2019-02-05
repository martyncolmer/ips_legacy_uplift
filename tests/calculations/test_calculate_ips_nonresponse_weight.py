"""
Created on 17th April 2018
Modified last: 14th Sep 2018

@author: James Burr / Nassir Mohammad
"""

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations import calculate_ips_nonresponse_weight as non_resp

# columns to sort the summary results by in order to check calculated dataframes match expected results
NR_COLUMNS = ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT',
              'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']


@pytest.mark.parametrize('path_to_data', [
    r'../data/calculations/december_2017/non_response_weight',
    r'../data/calculations/november_2017/non_response_weight',
    # r'tests\data\calculations\october_2017\non_response_weight', # ignored as summary data test unavailable
])
def test_calculate(path_to_data):
    # read in data from csv
    df_surveydata = pd.read_csv(path_to_data + '/surveydata.csv', engine='python')
    df_nr_data = pd.read_csv(path_to_data + '/nonresponsedata.csv', engine='python')

    result_py_data = non_resp.do_ips_nrweight_calculation(df_surveydata, df_nr_data,
                                                          'NON_RESPONSE_WT', 'SERIAL')

    # Retrieve and sort python calculated dataframes
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    py_summary_data = result_py_data[1]
    py_summary_data.sort_values(by=NR_COLUMNS)
    py_summary_data.index = range(0, len(py_summary_data))

    test_result_survey = pd.read_csv(path_to_data + '/outputdata_final.csv', engine='python')
    test_result_survey.sort_values(by='SERIAL', inplace=True)
    test_result_survey.index = range(0, len(test_result_survey))

    test_result_summary = pd.read_csv(path_to_data + '/summarydata_final.csv', engine='python')
    test_result_summary.sort_values(by=NR_COLUMNS, inplace=True)
    test_result_summary.index = range(0, len(test_result_summary))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey, check_dtype=False, check_like=True,
                       check_less_precise=True)
    assert_frame_equal(py_summary_data, test_result_summary, check_dtype=False, check_like=True,
                       check_less_precise=True)
