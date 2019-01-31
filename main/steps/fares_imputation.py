from calculations import calculate_ips_fares_imputation
from db import data_management as idm
from main import STEP_CONFIGURATION
from utils import process_variables, common_functions as cf


def fares_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the fares imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "FARES_IMPUTATION"

    # Populate Survey Data For Fares Imputation
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Fares Imp PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Fares Imp PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_FARES_SPV',
                              in_id='serial')

    # Update Survey Data with Fares Imp PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Fares Imputation
    survey_data_out = calculate_ips_fares_imputation.do_ips_fares_imputation(survey_data,
                                                                             var_serial='SERIAL',
                                                                             num_levels=9,
                                                                             measure='mean')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data With Fares Imp Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Fares Imp Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])
