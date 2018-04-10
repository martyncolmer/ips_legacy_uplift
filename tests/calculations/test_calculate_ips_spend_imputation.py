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
    py_survey_data = spend.do_ips_spend_imputation(test_survey
                                                   , OutputData="SAS_SPEND_IMP"
                                                   , var_serialNum="SERIAL"
                                                   , varStem=[["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR1_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL1_PV", "DUR1_PV", "PUR2_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR1_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR1_PV", "PUR2_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR1_PV", "PUR2_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL2_PV", "DUR2_PV", "PUR2_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL3_PV", "DUR2_PV", "PUR2_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR2_PV"]
                                                              , ["UK_OS_PV", "STAYIMPCTRYLEVEL4_PV", "DUR2_PV", "PUR3_PV"]
                                                              , ["UK_OS_PV", "DUR2_PV", "PUR3_PV"]]
                                                   , threshStem=[19, 12, 12, 12, 12, 12, 12, 12, 0, 0]
                                                   , numLevels=10
                                                   , donorVar="SPEND"
                                                   , outputVar="NEWSPEND"
                                                   , measure="mean"
                                                   , var_eligibleFlag="SPEND_IMP_ELIGIBLE_PV"
                                                   , var_impFlag="SPEND_IMP_FLAG_PV"
                                                   , var_impLevel="SPENDK"
                                                   , var_stay="STAY")
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


if __name__ == '__main__':
    test_calculate()