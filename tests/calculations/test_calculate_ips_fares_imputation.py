'''
Created on 15 Mar 2018

@author: burrj
'''
import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.ips_fares_imp import do_ips_fares_imputation


def test_calculate():
    # This is an integration test as it runs the entire step

    test_survey = pd.read_pickle('../data/fares_imp_input.pkl')

    result_data = do_ips_fares_imputation(test_survey
                                          , output='SAS_FARES_IMP'
                                          , var_serial_num='SERIAL', var_stem='VARS'
                                          , thresh_stem='THRESH', num_levels=9
                                          , donor_var='DVFARE', output_var='FARE'
                                          , measure='mean'
                                          , var_eligible_flag='FARES_IMP_ELIGIBLE_PV'
                                          , var_imp_flag='FARES_IMP_FLAG_PV'
                                          , var_imp_level='FAREK'
                                          , var_fare_age='FAGE_PV'
                                          , var_baby_fare='BABYFARE'
                                          , var_child_fare='CHILDFARE'
                                          , var_apd='APD_PV', var_package='DVPACKAGE'
                                          , var_fare_discount='DISCNT_F2_PV'
                                          , var_QMfare='QMFARE_PV'
                                          , var_package_cost='DVPACKCOST'
                                          , var_discounted_package_cost='DISCNT_PACKAGE_COST_PV'
                                          , var_persons='DVPERSONS', var_expenditure='DVEXPEND'
                                          , var_befaf='BEFAF', var_spend='SPEND'
                                          , var_spend_reason_key='SPENDIMPREASON'
                                          , var_duty_free='DUTY_FREE_PV'
                                          , var_old_package='PACKAGE')

    test_result_summary = pd.read_pickle('../data/fares_imp_output.pkl')
    test_result_summary.columns = test_result_summary.columns.str.upper()

    test_result_summary = test_result_summary.sort_values(by='SERIAL')
    test_result_summary.index = range(0, len(test_result_summary))
    result_data = result_data.sort_values(by='SERIAL')
    result_data.index = range(0, len(result_data))

    # The dataframes have different column orders; check_like is True which will
    # ignore that fact when checking if the data is the same.
    assert_frame_equal(result_data, test_result_summary, check_like=True)


test_calculate()