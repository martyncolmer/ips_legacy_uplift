from ips.calculations import calculate_ips_imb_weight
from ips.db import data_management as idm
from ips.main import STEP_CONFIGURATION
from ips.utils import common_functions as cf, process_variables


def imbalance_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the imbalance weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "IMBALANCE_WEIGHT"

    # Populate Survey Data For Imbalance Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Imbalance Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Imbalance Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_IMBALANCE_SPV',
                              in_id='serial')

    # Update Survey Data With Imbalance Wt PVs Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Imbalance Weight
    survey_data_out, summary_data_out = \
        calculate_ips_imb_weight.do_ips_imbweight_calculation(survey_data,
                                                              var_serialNum="SERIAL",
                                                              var_shiftWeight="SHIFT_WT",
                                                              var_NRWeight="NON_RESPONSE_WT",
                                                              var_minWeight="MINS_WT",
                                                              var_trafficWeight="TRAFFIC_WT",
                                                              var_OOHWeight="UNSAMP_TRAFFIC_WT",
                                                              var_imbalanceWeight="IMBAL_WT")

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data_out)

    # Update Survey Data With Imbalance Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Imbalance Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Imbalance Weight Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])
