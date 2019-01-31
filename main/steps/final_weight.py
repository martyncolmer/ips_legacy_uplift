from calculations import calculate_ips_final_weight
from db import data_management as idm
from utils import common_functions as cf
from main import STEP_CONFIGURATION


def final_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the final weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "FINAL_WEIGHT"

    # Populate Survey Data For Final Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Final Weight
    survey_data_out, summary_data_out = \
        calculate_ips_final_weight.do_ips_final_wt_calculation(survey_data,
                                                               serial_num='SERIAL',
                                                               shift_weight='SHIFT_WT',
                                                               non_response_weight='NON_RESPONSE_WT',
                                                               min_weight='MINS_WT',
                                                               traffic_weight='TRAFFIC_WT',
                                                               unsampled_weight='UNSAMP_TRAFFIC_WT',
                                                               imbalance_weight='IMBAL_WT',
                                                               final_weight='FINAL_WT')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data_out)

    # Update Survey Data With Final Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Final Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Final Weight Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])
