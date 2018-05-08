'''
Created on May 2018

@author: Nassir Mohammad
'''


import math
import numpy as np
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from main.calculations.calculate_ips_shift_weight import calculate_factor, calculate, \
                                                         do_ips_shift_weight_calculation, calculate_ips_crossing_factor, \
                                                         calculate_ips_shift_factor

path_to_data = r"../data/shift_weight"


@pytest.mark.shiftweight
class TestCalculateFactor(object):

    def test_calculate_factor_with_non_zero_numerator_and_denominator(self):
        d = {'NUMERATOR': [1, 2, 3, 4, 5], 'DENOMINATOR': [9, 2, -1, 4, 5], 'flag': [1, 1, 2, 1, 1]}
        df = pd.DataFrame(data=d)
        df_new = df.apply(calculate_factor, axis=1, args=('flag',))

        # check all values match manual calculation
        df_man = df['NUMERATOR'] / df['DENOMINATOR']
        assert (all(np.where(df_new == df_man, True, False)))

    def test_calculate_factor_for_zero_division(self):
        d = {'NUMERATOR': [1, 2, 3, 4, 0], 'DENOMINATOR': [0, 4, 0, -4, 0], 'flag': [1, 1, 2, 1, 1]}
        df = pd.DataFrame(data=d)
        df_new = df.apply(calculate_factor, axis=1, args=('flag',))

        assert(df_new[0] == float("inf"))
        assert (df_new[1] == 0.5)
        assert (df_new[2] == float("inf"))
        assert (df_new[3] == -1.0)

        # check for 0/0
        assert (math.isnan(df_new[4]))

    def test_calculate_factor_with_zero_flag_parameter(self):
        d = {'NUMERATOR': [1, 2, 3, 4, 5], 'DENOMINATOR': [1, 2, 3, 4, 5], 'flag': [1, 0, 0, 1, 1]}
        df = pd.DataFrame(data=d)
        df_new = df.apply(calculate_factor, axis=1, args=('flag',))
        assert (math.isnan(df_new[1]))
        assert (math.isnan(df_new[2]))

    def test_calculate_factor_with_all_zero_flag_parameter(self):
        d = {'NUMERATOR': [1, 2, 3, 4, 5], 'DENOMINATOR': [1, 2, 3, 4, 5], 'flag': [0, 0, 0, 0, 0]}
        df = pd.DataFrame(data=d)
        df_new = df.apply(calculate_factor, axis=1, args=('flag',))
        assert (math.isnan(df_new[0]))
        assert (math.isnan(df_new[1]))
        assert (math.isnan(df_new[2]))
        assert (math.isnan(df_new[3]))
        assert (math.isnan(df_new[4]))

@pytest.mark.shiftweight
def test_calculate():

    (df_surveydata, df_summary) = calculate(SurveyData = 'SAS_SURVEY_SUBSAMPLE'
                                             , ShiftsData = 'SAS_SHIFT_DATA',
                                             var_serialNum = 'SERIAL',
                                             var_shiftWeight = 'SHIFT_WT')

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\out_2.pkl")
    df_test.columns = df_test.columns.str.upper()

    # sort by 'SERIAL only for comparison to SAS
    df_test_2 = df_test.sort_values(['SERIAL'])
    df_test_2.index = range(df_test_2.shape[0])
    assert_frame_equal(df_surveydata, df_test_2, check_dtype=False)
    # test code end

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\summary_3.pkl")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary, df_test, check_like=True)
    # test code end


@pytest.mark.shiftweight
def test_do_ips_shift_weight_calculation():

    df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
    df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")

    # uppercase all columns
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_shiftsdata.columns = df_shiftsdata.columns.str.upper()

    df_surveydata_out, df_summary_out = do_ips_shift_weight_calculation(df_surveydata,
                                                                        df_shiftsdata,
                                                                        var_serialNum='SERIAL',
                                                                        var_shiftWeight='SHIFT_WT')

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\out_2.pkl")
    df_test.columns = df_test.columns.str.upper()
    df_test_2 = df_test.sort_values(['SERIAL'])
    df_test_2.index = range(df_test_2.shape[0])
    assert_frame_equal(df_surveydata_out, df_test_2, check_dtype=False)
    # test code end

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\summary_3.pkl")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_summary_out, df_test, check_like=True)
    # test code end

@pytest.mark.shiftweight
def test_calculate_ips_shift_factor():

    df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
    df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")

    # uppercase all columns
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_shiftsdata.columns = df_shiftsdata.columns.str.upper()

    (df_totalsampledshifts, df_possibleshifts, df_surveydata_merge) = calculate_ips_shift_factor(df_shiftsdata,
                                                                                        df_surveydata)
    # test code start
    df_test = pd.read_pickle(path_to_data + r"\totalSampledShifts.pkl")
    df_test.columns = df_test.columns.str.upper()
    df_test = df_test.drop(['_TYPE_', '_FREQ_'], axis=1)
    assert_frame_equal(df_totalsampledshifts, df_test, check_dtype=False)
    # test code end

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\possibleShifts.pkl")
    df_test.columns = df_test.columns.str.upper()
    df_test = df_test.drop(['_TYPE_', '_FREQ_'], axis=1)
    assert_frame_equal(df_possibleshifts, df_test, check_dtype=False)
    # test code end

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\outputdata_sf.pkl")
    df_test.columns = df_test.columns.str.upper()
    # df_test = df_test.drop(['_TYPE_', '_FREQ_'], axis=1)
    assert_frame_equal(df_surveydata_merge, df_test, check_dtype=False)
    # test code end

@pytest.mark.shiftweight
def test_calculate_ips_crossing_factor():

    df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
    df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")

    # uppercase all columns
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_shiftsdata.columns = df_shiftsdata.columns.str.upper()

    df_surveydata = pd.read_pickle(path_to_data + r"\surveydata.pkl")
    df_shiftsdata = pd.read_pickle(path_to_data + r"\shiftsdata.pkl")

    # uppercase all columns
    df_surveydata.columns = df_surveydata.columns.str.upper()
    df_shiftsdata.columns = df_shiftsdata.columns.str.upper()

    # get the survey data input from calculate_ips_shift_factor()
    (_, _, df_surveydata_for_crossings) = calculate_ips_shift_factor(df_shiftsdata,
                                                                     df_surveydata,
                                                                     )

    (df_totalSampledCrossings, df_surveydata_merge) = calculate_ips_crossing_factor(df_shiftsdata,
                                                                                df_surveydata_for_crossings)

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\totalSampledCrossings.pkl")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_totalSampledCrossings, df_test.drop(['_TYPE_', '_FREQ_'], axis=1), check_dtype=False)
    # test code end

    # test code start
    df_test = pd.read_pickle(path_to_data + r"\outputdata_cf.pkl")
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_surveydata_merge, df_test)
    # test code end




