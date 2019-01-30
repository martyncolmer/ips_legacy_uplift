from calculations import calculate_ips_town_and_stay_expenditure
from io import data_management as idm
from main import STEP_CONFIGURATION
from utils import process_variables, common_functions as cf


def town_stay_expenditure_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the town stay expenditure imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "TOWN_AND_STAY_EXPENDITURE"

    # Populate Survey Data For TSE Imputation
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy TSE Imputation PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply TSE Imputation PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TOWN_STAY_SPV',
                              in_id='serial')

    # Update Survey Data with TSE Imputation PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate TSE Imputation
    survey_data_out = calculate_ips_town_and_stay_expenditure.do_ips_town_exp_imp(survey_data,
                                                                                  var_serial="SERIAL",
                                                                                  var_final_wt="FINAL_WT")

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data With TSE Imputation Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With TSE Imputation Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])
