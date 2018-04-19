'''
Created on April 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_unsampled_weight import calculate, do_ips_unsampled_weight_calculation
import pytest

path_to_data = '../../tests/data/unsampled_weight'

@pytest.mark.unsampled
def test_calculate():
    print("Started testing IPS unsampled weight - calculate()")
    (output_dataframe, summary_dataframe) =  calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE',
                                            OutputData = 'SAS_UNSAMPLED_OOH_WT',
                                            SummaryData = 'SAS_PS_UNSAMPLED_OOH',
                                            ResponseTable = 'SAS_RESPONSE',
                                            var_serialNum = 'SERIAL',
                                            var_shiftWeight = 'SHIFT_WT',
                                            var_NRWeight = 'NON_RESPONSE_WT',
                                            var_minWeight = 'MINS_WT',
                                            var_trafficWeight = 'TRAFFIC_WT',
                                            OOHStrataDef =['UNSAMP_PORT_GRP_PV',
                                            'UNSAMP_REGION_GRP_PV',
                                            'ARRIVEDEPART'],
                                            PopTotals = 'SAS_UNSAMPLED_OOH_DATA',
                                            var_totals = 'UNSAMP_TOTAL',
                                            MaxRuleLength = '512',
                                            ModelGroup = 'C_GROUP',
                                            var_modelGroup = 'C_GROUP',
                                            var_OOHWeight = 'UNSAMP_TRAFFIC_WT',
                                            GESBoundType = 'G',
                                            GESUpperBound = '',
                                            GESLowerBound = '1.0',
                                            GESMaxDiff = '1E-8',
                                            GESMaxIter = '50',
                                            GESMaxDist = '1E-8',
                                            var_caseCount = 'CASES',
                                            var_OOHWeightSum = 'SUM_UNSAMP_TRAFFIC_WT',
                                            var_priorWeightSum = 'SUM_PRIOR_WT',
                                            minCountThresh = 30)

    test_df = pd.read_pickle(path_to_data + r"/output_final.pkl")
    test_df.columns = test_df.columns.str.upper()
    assert_frame_equal(output_dataframe, test_df)

    test_summary = pd.read_pickle(path_to_data + r"/summary_final.pkl")
    test_summary.columns = test_summary.columns.str.upper()
    assert_frame_equal(summary_dataframe, test_summary, check_like=True, check_dtype=False)

    print("test_calculate completed successfully")

@pytest.mark.unsampled
def test_do_ips_unsampled_weight_calculation():
    x = do_ips_unsampled_weight_calculation(df_surveydata, summary, var_serialNum, var_shiftWeight, var_NRWeight,
                                        var_minWeight, var_trafficWeight, OOHStrataDef, df_ustotals,
                                        var_totals, MaxRuleLength, var_modelGroup, output,
                                        var_OOHWeight, var_caseCount, var_priorWeightSum,
                                        var_OOHWeightSum, GESBoundType, GESUpperBound, GESLowerBound,
                                        GESMaxDiff, GESMaxIter, GESMaxDist, minCountThresh)



