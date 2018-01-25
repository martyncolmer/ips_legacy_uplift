'''
Created on 8 Jan 2018

@author: Thomas Mahoney
'''
import sys
from IPSTransformation import CommonFunctions as cf
from IPS_Unallocated_Modules import transpose_survey_data
from IPS_Unallocated_Modules import populate_survey_subsample
from IPS_Unallocated_Modules import prepare_survey_data
from IPS_Stored_Procedures import process_variables

root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\TestInputFiles"
path_to_test_data = root_data_path + r"\testdata.sas7bdat"

run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'


connection = cf.get_oracle_connection()

# 1 - Import

# Import function

# Transpose survey Data then write results to SURVEY_VALUE and SURVEY_COLUMN tables
transpose_survey_data.transpose(path_to_test_data)

# Populate Survey sub-sample
populate_survey_subsample.populate(run_id,connection)


# 2 - Process

# Shift Weight

prepare_survey_data.populate_survey_data_for_shift_wt(run_id, connection)

prepare_survey_data.populate_shift_data(run_id, connection)

prepare_survey_data.copy_shift_wt_pvs_for_survey_data(run_id, connection)

# Apply PV's to survey data
process_variables.process()
print("PV's processed")
sys.exit()

prepare_survey_data.update_survey_data_with_shift_wt_pv_output(connection)

prepare_survey_data.copy_shift_wt_pvs_for_shift_data(run_id, connection)

# Apply Shift Wt PVs On Shift Data

prepare_survey_data.update_shift_data_with_pvs_output(connection)

# Calculate Shift Weight

prepare_survey_data.update_survey_data_with_shift_wt_results(connection)

prepare_survey_data.store_survey_data_with_shift_wt_results(run_id, connection)

prepare_survey_data.store_shift_wt_summary(run_id, connection)




# 3 - Export
#export_data










# Calculate_ips_shift_weight
#calculate_ips_shift_weight

# Export
#export_data