from calculations import calculate_ips_minimums_weight
from io import data_management as idm
from main import STEP_CONFIGURATION
from utils import process_variables, common_functions as cf


def minimums_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the minimums weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "MINIMUMS_WEIGHT"

    # Populate Survey Data For Minimums Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Minimums Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Minimums Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_MINIMUMS_SPV',
                              in_id='serial')

    # Update Survey Data with Minimums Wt PVs Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Minimums Weight
    output_data, summary_data = \
        calculate_ips_minimums_weight.do_ips_minweight_calculation(df_surveydata=survey_data,
                                                                   var_serialNum='SERIAL',
                                                                   var_shiftWeight='SHIFT_WT',
                                                                   var_NRWeight='NON_RESPONSE_WT',
                                                                   var_minWeight='MINS_WT')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], output_data)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data)

    # Update Survey Data With Minimums Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Minimums Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Minimums Wt Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])
