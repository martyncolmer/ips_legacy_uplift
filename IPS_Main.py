'''
Created on 8 Jan 2018

@author: Thomas Mahoney
'''
import sys
from IPSTransformation import CommonFunctions as cf
from IPS_Unallocated_Modules import transpose_survey_data
from IPS_Unallocated_Modules import populate_survey_subsample
from IPS_Unallocated_Modules import prepare_survey_data
from IPS_Unallocated_Modules import import_traffic_data
from IPS_Stored_Procedures import process_variables, calculate_ips_shift_weight

root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\TestInputFiles"
path_to_test_data = root_data_path + r"\testdata.sas7bdat"

path_to_shift_data = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts Q1 2017.csv'

run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'


connection = cf.get_oracle_connection()

# 1 - Import

# Import function

# Transpose survey Data then write results to SURVEY_VALUE and SURVEY_COLUMN tables
transpose_survey_data.transpose(path_to_test_data)

# Populate Survey sub-sample
populate_survey_subsample.populate(run_id,connection)

# Import Shift Data
import_traffic_data.import_traffic_data(path_to_shift_data)

# 2 - Process

print("Start - populate_survey_data_for_shift_wt")
prepare_survey_data.populate_survey_data_for_shift_wt(run_id, connection)

print("Start - populate_shift_data")
prepare_survey_data.populate_shift_data(run_id, connection)

print("Start - copy_shift_wt_pvs_for_survey_data")
prepare_survey_data.copy_shift_wt_pvs_for_survey_data(run_id, connection)

print("Start - Apply PVs On survey Data")
process_variables.process(in_dataset = 'survey',
                          in_intabname = 'SAS_SURVEY_SUBSAMPLE',
                          in_outtabname = 'SAS_SHIFT_SPV',
                          in_id = 'serial')

print("Start - update_survey_data_with_shift_wt_pv_output")
prepare_survey_data.update_survey_data_with_shift_wt_pv_output(connection)

print("Start - copy_shift_wt_pvs_for_shift_data")
prepare_survey_data.copy_shift_wt_pvs_for_shift_data(run_id, connection)

print("Start - Apply Shift Wt PVs On Shift Data")
process_variables.process(in_dataset = 'shift',
                          in_intabname = 'SAS_SHIFT_DATA',
                          in_outtabname = 'SAS_SHIFT_PV',
                          in_id = 'REC_ID')

print("Start - update_shift_data_with_pvs_output")
prepare_survey_data.update_shift_data_with_pvs_output(connection)


print("Start - Calculate Shift Weight")
calculate_ips_shift_weight.calculate(SurveyData = 'sas_survey_subsample',
                                     ShiftsData = 'sas_shift_data', 
                                     OutputData = 'sas_shift_wt', 
                                     SummaryData = 'sas_ps_shift_data', 
                                     ResponseTable = 'sas_response', 
                                     ShiftsStratumDef = ['shift_port_grp_pv', 
                                                         'arrivedepart',
                                                         'weekday_end_pv',
                                                         'am_pm_night_pv'], 
                                     var_serialNum = 'serial', 
                                     var_shiftFlag = 'shift_flag_pv', 
                                     var_shiftFactor = 'shift_factor', 
                                     var_totals = 'total', 
                                     var_shiftNumber = 'shiftno', 
                                     var_crossingFlag = 'crossings_flag_pv', 
                                     var_crossingsFactor = 'crossings_factor', 
                                     var_crossingNumber = 'shuttle', 
                                     var_SI = 'migSI', 
                                     var_shiftWeight = 'shift_wt', 
                                     var_count = 'count_resps', 
                                     var_weightSum = 'sum_sh_wt', 
                                     var_minWeight = 'min_sh_wt', 
                                     var_avgWeight = 'mean_sh_wt', 
                                     var_maxWeight = 'max_sh_wt', 
                                     var_summaryKey = 'shift_port_grp_pv', 
                                     subStrata = ['shift_port_grp_pv', 
                                                  'arrivedepart'], 
                                     var_possibleCount = 'poss_shift_cross', 
                                     var_sampledCount = 'samp_shift_cross', 
                                     minWeightThresh = '50', 
                                     maxWeightThresh = '5000')

print("Start - update_survey_data_with_shift_wt_results")
prepare_survey_data.update_survey_data_with_shift_wt_results(connection)

print("Start - store_survey_data_with_shift_wt_results")
prepare_survey_data.store_survey_data_with_shift_wt_results(run_id, connection)

print("Start - store_shift_wt_summary")
prepare_survey_data.store_shift_wt_summary(run_id, connection)


# 3 - Export
#export_data

print("At Export")

