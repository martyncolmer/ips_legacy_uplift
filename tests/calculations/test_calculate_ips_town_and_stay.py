'''
Created on 10 May 2018

@author: thorne1
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations import calculate_ips_town_and_stay_expenditure as tse
from main.io import CommonFunctions as cf

def test_calculate():
    # This is an integration test as it runs the entire step
    test_survey = pd.read_pickle('../data/town_and_stay.pkl')

    # Retrieve Python output data and cleanse
    py_survey_data = tse.do_ips_spend_imputation(df_survey_data=test_survey,
                                                 var_serial="SERIAL",
                                                 var_flow="FLOW",
                                                 var_purpose_grp="PURPOSE_PV",
                                                 var_country_grp="STAYIMPCTRYLEVEL4_PV",
                                                 var_residence="RESIDENCE",
                                                 var_stay="STAY",
                                                 var_spend="SPEND",
                                                 var_final_wt="FINAL_WT",
                                                 var_eligible_flag="TOWN_IMP_ELIGIBLE_PV")
    py_survey_data = py_survey_data.sort_values(by='SERIAL')
    py_survey_data.index = range(0, len(py_survey_data))

    # Retrieve SAS Survey Data output and cleanse
    test_result_survey = pd.read_pickle('../data/import/output/post_import_TOWN_AND_STAY.pkl')
    test_result_survey.columns = test_result_survey.columns.str.upper()
    test_result_survey = test_result_survey.sort_values(by='SERIAL')
    test_result_survey.index = range(0, len(test_result_survey))

    # Assert dfs are equal
    assert_frame_equal(py_survey_data, test_result_survey, check_column_type=False)

if __name__ == "__main__":
    test_calculate()
    cf.beep()
