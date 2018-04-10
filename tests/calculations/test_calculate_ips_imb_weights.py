'''
Created on 6 Mar 2018

@author: thorne1
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations import calculate_ips_imb_weight as imb


def test_calculate():
    # This is an integration test as it runs the entire step
    test_survey = pd.read_pickle('../data/imb_weight_input.pkl')
    # Retrieve Python output data
    result_py_data = imb.do_ips_imbweight_calculation(test_survey
                               , OutputData = "SAS_IMBALANCE_WT"
                               , SummaryData = "SAS_PS_IMBALANCE"
                               , var_serialNum = "SERIAL"
                               , var_shiftWeight = "SHIFT_WT"
                               , var_NRWeight = "NON_RESPONSE_WT"
                               , var_minWeight = "MINS_WT"
                               , var_trafficWeight = "TRAFFIC_WT"
                               , var_OOHWeight = "UNSAMP_TRAFFIC_WT"
                               , var_imbalanceWeight = "IMBAL_WT"
                               , var_portroute = "PORTROUTE"
                               , var_flow = "FLOW"
                               , var_direction = "ARRIVEDEPART"
                               , var_pgFactor = "IMBAL_PORT_FACT_PV"
                               , var_cgFactor = "IMBAL_CTRY_FACT_PV"
                               , var_priorSum = "SUM_PRIOR_WT"
                               , var_postSum = "SUM_IMBAL_WT"
                               , var_eligible_flag = "IMBAL_ELIGIBLE_PV")
    
    # From Python output data, retrieve Survey Data and cleanse
    py_survey_data = result_py_data[0]
    py_survey_data = py_survey_data.sort_values(by = 'SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    # From Python output data, retrieve Summary Data and cleanse 
    py_summary_data = result_py_data[1]
    py_summary_data.index = range(0, len(py_summary_data))
    
    # Retrieve SAS Survey Data output and cleanse
    test_result_survey = pd.read_pickle('../data/imb_weight_surveydata_output.pkl')
    test_result_survey.columns = test_result_survey.columns.str.upper()
    test_result_survey = test_result_survey.sort_values(by = 'SERIAL')
    test_result_survey.index = range(0, len(test_result_survey))
#    test_result_survey.replace("", np.NaN, inplace=True)
    
    # Retrieve SAS Summary Data output and cleanse
    test_result_summary = pd.read_pickle('../data/imb_weight_summarydata_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()
    test_result_summary.index = range(0, len(test_result_summary))
    
    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey, check_dtype=False)
    assert_frame_equal(py_summary_data, test_result_summary, check_dtype=False)


if __name__ == '__main__':
    test_calculate()