from ips.calculations import calculate_ips_stay_imputation
from ips.db import data_management as idm
from ips import STEP_CONFIGURATION
from ips.utils import common_functions as cf, process_variables


def stay_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the stay imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "STAY_IMPUTATION"

    # Populate Survey Data For Stay Imputation
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Stay Imp PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Stay Imp PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_STAY_SPV',
                              in_id='serial')

    # Update Survey Data with Stay Imp PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Stay Imputation
    survey_data_out = calculate_ips_stay_imputation.do_ips_stay_imputation(survey_data,
                                                                           var_serial='SERIAL',
                                                                           num_levels=1,
                                                                           measure='mean')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data With Stay Imp Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Stay Imp Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])
