'''
Created on 17th April 2018

@author: James Burr
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations import calculate_ips_nonresponse_weight as non_resp

from main.io import CommonFunctions as cf


def test_calculate():
    # This is an integration test as it runs the entire step
    test_survey = pd.read_pickle('../data/non_response_survey_data.pkl')
    test_nr_data = pd.read_pickle('../data/non_response_data.pkl')
    result_py_data = non_resp.do_ips_nrweight_calculation(test_survey,
                                                          test_nr_data,
              OutputData='SAS_NON_RESPONSE_WT',
              SummaryData='SAS_PS_NON_RESPONSE',
              ResponseTable='SAS_RESPONSE',
              NRStratumDef=['NR_PORT_GRP_PV',
                            'ARRIVEDEPART'],
              ShiftsStratumDef=['NR_PORT_GRP_PV',
                                'ARRIVEDEPART',
                                'WEEKDAY_END_PV'],
              var_NRtotals='MIGTOTAL',
              var_NonMigTotals='ORDTOTAL',
              var_SI='',
              var_migSI='MIGSI',
              var_TandTSI='TANDTSI',
              var_PSW='SHIFT_WT',
              var_NRFlag='NR_FLAG_PV',
              var_migFlag='MIG_FLAG_PV',
              var_respCount='COUNT_RESPS',
              var_NRWeight='NON_RESPONSE_WT',
              var_meanSW='MEAN_RESPS_SH_WT',
              var_priorSum='PRIOR_SUM',
              var_meanNRW='MEAN_NR_WT',
              var_grossResp='GROSS_RESP',
              var_gnr='GNR',
              var_serialNum='SERIAL',
              minCountThresh='30')

    # Retrieve and clean Python survey data output
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by = 'SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    # From Python output data, retrieve Summary Data and cleanse
    py_summary_data = result_py_data[1]
    py_summary_data.index = range(0, len(py_summary_data))

    # Retrieve SAS Survey Data output and cleanse
    test_result_survey = pd.read_pickle('../data/non_response_weight_output.pkl')
    test_result_survey.columns = test_result_survey.columns.str.upper()
    test_result_survey = test_result_survey.sort_values(by='SERIAL')
    test_result_survey.index = range(0, len(test_result_survey))
    #test_result_survey.replace("", np.NaN, inplace=True)

    # Retrieve SAS Summary Data output and cleanse
    test_result_summary = pd.read_pickle('../data/non_response_weight_summary.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    test_result_summary.index = range(0, len(test_result_summary))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey, check_dtype=False, check_like=True)
    assert_frame_equal(py_summary_data, test_result_summary, check_dtype=False, check_like=True)
