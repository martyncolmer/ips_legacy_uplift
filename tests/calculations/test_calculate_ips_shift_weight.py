'''
Created on April 2018

@author: Nassir Mohammad
'''

import pandas as pd
import pytest

from main.calculations.calculate_ips_shift_weight import calculate_factor, calculate, \
                                                         do_ips_shift_weight_calculation,calculate_ips_shift_factor, \
                                                         calculate_ips_shift_factor

@pytest.mark.shiftweight
class TestCalculateFactor(object):
    d = {'col1': [1, 2, 3, 4, 5], 'col2': [1, 2, 3, 4, 5]}
    df = pd.DataFrame(data=d)

    def test_calculate_factor_with_valid_columns(self):
        pass
        # pass
        # print("Started testing calculate_factor with columns")
        #
        # #mergedDF.apply(calculate_factor, axis=1, args=(var_shiftFlag,))
        #
        #
        # df = pd.
        # result = calculate_factor(row, flag=1)

    def test_calculate_factor_for_zerodivisionerror_with_invalid_columns(self):
        pass

    def test_calculate_factor_with_zero_flag_parameter(self):
        pass

    def test_calculate_factor_with_dataframe_apply(self):
        pass

@pytest.mark.shiftweight
def test_calculate():
    print("Started testing IPS Shift Weight - calculate()")

    (df_surveydata, df_summary) = calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE'
                                             , ShiftsData = 'SAS_SHIFT_DATA'
                                             , OutputData = 'SAS_SHIFT_WT'
                                             , SummaryData = 'SAS_PS_SHIFT_DATA'
                                             , ResponseTable = 'SAS_RESPONSE'
                                             , ShiftsStratumDef = ['SHIFT_PORT_GRP_PV',
                                                                 'ARRIVEDEPART',
                                                                 'WEEKDAY_END_PV',
                                                                 'AM_PM_NIGHT_PV'],
                                             var_serialNum = 'SERIAL',
                                             var_shiftFlag = 'SHIFT_FLAG_PV',
                                             var_shiftFactor = 'SHIFT_FACTOR',
                                             var_totals = 'TOTAL',
                                             var_shiftNumber = 'SHIFTNO',
                                             var_crossingFlag = 'CROSSINGS_FLAG_PV',
                                             var_crossingsFactor = 'CROSSINGS_FACTOR',
                                             var_crossingNumber = 'SHUTTLE',
                                             var_SI = 'MIGSI',
                                             var_shiftWeight = 'SHIFT_WT',
                                             var_count = 'COUNT_RESPS',
                                             var_weightSum = 'SUM_SH_WT',
                                             var_minWeight = 'MIN_SH_WT',
                                             var_avgWeight = 'MEAN_SH_WT',
                                             var_maxWeight = 'MAX_SH_WT',
                                             var_summaryKey = 'SHIFT_PORT_GRP_PV',
                                             subStrata = ['SHIFT_PORT_GRP_PV',
                                                          'ARRIVEDEPART'],
                                             var_possibleCount = 'POSS_SHIFT_CROSS',
                                             var_sampledCount = 'SAMP_SHIFT_CROSS',
                                             minWeightThresh = '50',
                                             maxWeightThresh = '5000')

    # path_to_test = path_to_data + r"/output_rounded.pkl"
    # test_df_output_merge_final_rounded = pd.read_pickle(path_to_test)
    # test_df_output_merge_final_rounded.columns = test_df_output_merge_final_rounded.columns.str.upper()
    # assert_frame_equal(df_output_merge_final_rounded, test_df_output_merge_final_rounded)
    #
    # path_to_test = path_to_data + r"/summary_merge_sum_traftot.pkl"
    # test_summary_merge_sum_traftot = pd.read_pickle(path_to_test)
    # test_summary_merge_sum_traftot.columns = test_summary_merge_sum_traftot.columns.str.upper()
    # assert_frame_equal(df_summary_merge_sum_traftot, test_summary_merge_sum_traftot)

    print("test_calculate finished successfully")

@pytest.mark.shiftweight
def test_calculate_ips_shift_factor():
    pass

@pytest.mark.shiftweight
def test_calculate_ips_crossing_factor():
    pass

@pytest.mark.shiftweight
def test_do_ips_shift_weight_calculation():
    pass