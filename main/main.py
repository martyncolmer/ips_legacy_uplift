'''
Created on 26 April 2018
@author: Thomas Mahoney

Refactored on 2 October 2018
@author: Elinor Thorne
'''

import json

from main.io import CommonFunctions as cf
from main.io import ips_data_management as idm
from main.utils import process_variables
from main.calculations import calculate_ips_shift_weight
from main.calculations import calculate_ips_nonresponse_weight
from main.calculations import calculate_ips_minimums_weight
from main.calculations import calculate_ips_traffic_weight
from main.calculations import calculate_ips_unsampled_weight
from main.calculations import calculate_ips_imb_weight
from main.calculations import calculate_ips_final_weight
from main.calculations import calculate_ips_stay_imputation
from main.calculations import calculate_ips_fares_imputation
from main.calculations import calculate_ips_spend_imputation
from main.calculations import calculate_ips_rail_imputation
from main.calculations import calculate_ips_regional_weights
from main.calculations import calculate_ips_airmiles

with open(r'data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)


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

    # Calculate Shift Weight
    survey_data_out, summary_data_out = calculate_ips_shift_weight.do_ips_shift_weight_calculation(survey_data,
                                                                                                   shift_data,
                                                                                                   var_serialNum='SERIAL',
                                                                                                   var_shiftWeight='SHIFT_WT')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data_out)

    # Update Survey Data With Shift Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Shift Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Shift Wt Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])


def non_response_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 26 April 2018
    Purpose      : Runs the non response weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Load configuration variables
    step_name = "NON_RESPONSE"

    # Populate Survey Data For Non Response Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Populate Non Response Data
    idm.populate_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Non Response Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Non Response Wt PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_NON_RESPONSE_SPV',
                              in_id='serial')

    # Update Survey Data with Non Response Wt PVs Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Copy Non Response Wt PVs for Non Response Data
    idm.copy_step_pvs_for_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Non Response Wt PVs On Non Response Data
    process_variables.process(dataset='non_response',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_NON_RESPONSE_PV',
                              in_id='REC_ID')

    # Update NonResponse Data With PVs Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    non_response_data = cf.get_table_values(STEP_CONFIGURATION[step_name]["data_table"])

    # Calculate Non Response Weight
    survey_data_out, summary_data_out = calculate_ips_nonresponse_weight.do_ips_nrweight_calculation(survey_data,
                                                                                                     non_response_data,
                                                                                                     'NON_RESPONSE_WT',
                                                                                                     'SERIAL')
    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data_out)

    # Update Survey Data With Non Response Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With NonResponse Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Non Response Wt Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])


def minimums_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the minimums weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
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
    output_data, summary_data = calculate_ips_minimums_weight.do_ips_minweight_calculation(df_surveydata=survey_data,
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


def traffic_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the traffic weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Load configuration variables
    step_name = "TRAFFIC_WEIGHT"

    # Populate Survey Data For Traffic Wt
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Populate Traffic Data
    idm.populate_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Traffic Wt PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Traffic Wt PV On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TRAFFIC_SPV',
                              in_id='serial')

    # Update Survey Data with Traffic Wt PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Copy Traffic Wt PVs For Traffic Data
    idm.copy_step_pvs_for_step_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Traffic Wt PV On Traffic Data
    process_variables.process(dataset='traffic',
                              in_table_name='SAS_TRAFFIC_DATA',
                              out_table_name='SAS_TRAFFIC_PV',
                              in_id='REC_ID')

    # Update Traffic Data With Traffic Wt PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # TODO: Pass correct parameters to do_ips_trafweight_calculation() once step has been refactored to include new R-GES
    # Calculate Traffic Weight
    output_data, summary_data = calculate_ips_traffic_weight.do_ips_trafweight_calculation(survey_data)

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], output_data)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data)

    # Update Survey Data With Traffic Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Traffic Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Traffic Wt Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])


def unsampled_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
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
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # TODO: Pass correct parameters to do_ips_unsampled_weight_calculation() once step has been refactored to include new R-GES
    # Calculate Unsampled Weight
    output_data, summary_data = calculate_ips_unsampled_weight.do_ips_unsampled_weight_calculation(survey_data)

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], output_data)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data)

    # Update Survey Data With Unsampled Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Unsampled Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Unsampled Weight Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])


def imbalance_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the imbalance weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "IMBALANCE_WEIGHT"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_IMBALANCE_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_imb_weight.calculate()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)
    generic_xml_steps.store_step_summary(run_id, connection, step)


def final_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the final weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "FINAL_WEIGHT"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)

    calculate_ips_final_weight.calculate()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)
    generic_xml_steps.store_step_summary(run_id, connection, step)


def stay_imputation_step(run_id,connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the stay imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "STAY_IMPUTATION"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_STAY_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_stay_imputation.ips_stay_imp()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


def fares_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the fares imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "FARES_IMPUTATION"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_FARES_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_fares_imputation.ips_fares_imp()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


def spend_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the spend imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "SPEND_IMPUTATION"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_spend_imputation.calculate()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


def rail_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the rail imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "RAIL_IMPUTATION"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_RAIL_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_rail_imputation.calculate()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


def regional_weights_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the regional weights steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "REGIONAL_WEIGHTS"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_REGIONAL_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_regional_weights.calculate()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


def town_stay_expenditure_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the town stay expenditure imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "TOWN_AND_STAY_EXPENDITURE"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)
    generic_xml_steps.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TOWN_STAY_SPV',
                              in_id='serial')

    generic_xml_steps.update_survey_data_with_step_pv_output(connection, step)

    # calculation for town stay (still not complete)

    generic_xml_steps.update_survey_data_with_step_results(connection, step)

    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


def airmiles_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the air miles calculation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "AIR_MILES"

    generic_xml_steps.populate_survey_data_for_step(run_id, connection, step)

    calculate_ips_airmiles.calculate()

    generic_xml_steps.update_survey_data_with_step_results(connection, step)
    generic_xml_steps.store_survey_data_with_step_results(run_id, connection, step)


if __name__ == '__main__':

    # Connection to the SQL server database
    connection = cf.get_sql_connection()
    # Run Id (this will be generated automatically and will be unique)
    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    # What was this for?
    version_id = 1891

    # -- Read configuration --
    with open('data/xml_steps_configuration.json') as config_file:
        step_configurations = json.load(config_file)

    # -- Processing --
    shift_weight_step(run_id, connection, step_configurations["SHIFT_WEIGHT"])
    non_response_weight_step(run_id, connection, step_configurations)
    minimums_weight_step(run_id, connection, step_configurations)
    traffic_weight_step(run_id, connection, step_configurations)
    unsampled_weight_step(run_id, connection, step_configurations)
    imbalance_weight_step(run_id, connection, step_configurations)
    final_weight_step(run_id, connection, step_configurations)
    stay_imputation_step(run_id, connection, step_configurations)
    fares_imputation_step(run_id, connection, step_configurations)
    spend_imputation_step(run_id, connection, step_configurations)
    rail_imputation_step(run_id, connection, step_configurations)
    regional_weights_step(run_id, connection,step_configurations)
    town_stay_expenditure_imputation_step(run_id, connection, step_configurations)
    airmiles_step(run_id, connection, step_configurations)
