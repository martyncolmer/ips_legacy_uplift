from calculations import calculate_ips_unsampled_weight
from io import data_management as idm
from main import STEP_CONFIGURATION
from utils import process_variables, common_functions as cf


def unsampled_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the unsampled weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "UNSAMPLED_WEIGHT"

    # Populate Survey Data For Unsampled Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Populate Unsampled Data
    idm.populate_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Unsampled Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Unsampled Wt PV On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_UNSAMPLED_OOH_SPV',
                              in_id='serial')

    # Update Survey Data with Unsampled Wt PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Copy Unsampled Wt PVs For Unsampled Data
    idm.copy_step_pvs_for_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Unsampled Wt PV On Unsampled Data
    process_variables.process(dataset='unsampled',
                              in_table_name='SAS_UNSAMPLED_OOH_DATA',
                              out_table_name='SAS_UNSAMPLED_OOH_PV',
                              in_id='REC_ID')

    # Update Unsampled Data With PV Output
    idm.update_step_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    unsampled_data = cf.get_table_values(STEP_CONFIGURATION[step_name]["data_table"])

    # Calculate Unsampled Weight
    output_data, summary_data = calculate_ips_unsampled_weight.do_ips_unsampled_weight_calculation(
        df_surveydata=survey_data,
        serial_num='SERIAL',
        shift_weight='SHIFT_WT',
        nr_weight='NON_RESPONSE_WT',
        min_weight='MINS_WT',
        traffic_weight='TRAFFIC_WT',
        out_of_hours_weight="UNSAMP_TRAFFIC_WT",
        df_ustotals=unsampled_data,
        min_count_threshold=30)

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], output_data)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data)

    # Update Survey Data With Unsampled Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Unsampled Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Unsampled Weight Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])
