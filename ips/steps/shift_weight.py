from ips.calculations import calculate_ips_shift_weight
from ips.db import data_management as idm
from ips.main import STEP_CONFIGURATION
from ips.utils import common_functions as cf, process_variables
import ips.dataimport.schemas.sas_shift_schema as sas_shift_schema


def shift_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 26 April 2018 / 2 October 2018
    Purpose      : Runs the shift weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = 'SHIFT_WEIGHT'

    # Populate Survey Data For Shift Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Populate Shift Data
    idm.populate_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Shift Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Shift Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SHIFT_SPV',
                              in_id='serial')

    # Update Survey Data with Shift Wt PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Copy Shift Wt PVs For Shift Data
    idm.copy_step_pvs_for_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Shift Wt PVs On Shift Data
    process_variables.process(dataset='shift',
                              in_table_name='SAS_SHIFT_DATA',
                              out_table_name='SAS_SHIFT_PV',
                              in_id='REC_ID')

    # Update Shift Data with PVs Output
    idm.update_step_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    shift_data = cf.get_table_values(STEP_CONFIGURATION[step_name]["data_table"])

    shift_data = sas_shift_schema.convert_dtype(shift_data)

    # Calculate Shift Weight
    survey_data_out, summary_data_out = \
        calculate_ips_shift_weight.do_ips_shift_weight_calculation(survey_data,
                                                                   shift_data,
                                                                   serial_number='SERIAL',
                                                                   shift_weight='SHIFT_WT')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data_out)

    # Update Survey Data With Shift Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Shift Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Shift Wt Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])
