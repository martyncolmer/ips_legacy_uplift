'''
Created on 12 March 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from IPS_Stored_Procedures import IPS_traffic_weight_calc

def test_calculate():
    result_data = calculate(SurveyData='sas_survey_subsample'
              , OutputData='sas_traffic_wt'.upper()
              , SummaryData='sas_ps_traffic'.upper()
              , ResponseTable='sas_response'
              , var_serialNum='serial'.upper()
              , var_shiftWeight='shift_wt'.upper()
              , var_NRWeight='non_response_wt'.upper()
              , var_minWeight='mins_wt'.upper()
              , StrataDef=['samp_port_grp_pv'.upper(),
                           'arrivedepart'.upper()]
              , PopTotals='sas_traffic_data'
              , TotalVar='traffictotal'.upper()
              , MaxRuleLength='512'
              , ModelGroup='C_group'
              , GWeightVar='traffic_wt'.upper()
              , GESBoundType='G'
              , GESUpperBound=''
              , GESLowerBound='1.0'
              , GESMaxDiff='1E-8'
              , GESMaxIter='50'
              , GESMaxDist='1E-8'
              , var_count='cases'.upper()
              , var_trafficTotal='traffictotal'.upper()
              , var_postSum='sum_traffic_wt'.upper()
              , minCountThresh=30)