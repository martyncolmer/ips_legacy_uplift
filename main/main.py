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
from main.calculations.calculate_ips_traffic_weight import do_ips_trafweight_calculation
from main.calculations import calculate_ips_unsampled_weight
from main.calculations import calculate_ips_imb_weight
from main.calculations import calculate_ips_final_weight
from main.calculations import calculate_ips_stay_imputation
from main.calculations import calculate_ips_fares_imputation
from main.calculations import calculate_ips_spend_imputation
from main.calculations import calculate_ips_rail_imputation
from main.calculations import calculate_ips_regional_weights
from main.calculations import calculate_ips_town_and_stay_expenditure
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

    survey_data.to_csv(r'S:\CASPA\IPS\Testing\scratch\survey_data_in-' + run_id + '.csv')
    shift_data.to_csv(r'S:\CASPA\IPS\Testing\scratch\shift_data-' + run_id + '.csv')

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
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 26 April 2018 / 2 October 2018
    Purpose      : Runs the non response weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
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
    idm.update_step_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

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
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the traffic weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
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
    idm.update_step_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    traffic_data = cf.get_table_values(STEP_CONFIGURATION[step_name]["data_table"])

    # Calculate Traffic Weight
    output_data, summary_data = do_ips_trafweight_calculation(df_survey=survey_data,
                                                              var_serialNum='SERIAL',
                                                              var_shiftWeight='SHIFT_WT',
                                                              var_NRWeight='NON_RESPONSE_WT',
                                                              var_minWeight='MINS_WT',
                                                              PopTotals=traffic_data,
                                                              GWeightVar='TRAFFIC_WT',
                                                              minCountThresh=30)

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
    survey_data_out, summary_data_out = calculate_ips_imb_weight.do_ips_imbweight_calculation(survey_data,
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
    survey_data_out, summary_data_out = calculate_ips_final_weight.do_ips_final_wt_calculation(survey_data,
                                                                                         var_serialNum='SERIAL',
                                                                                         var_shiftWeight='SHIFT_WT',
                                                                                         var_NRWeight='NON_RESPONSE_WT',
                                                                                         var_minWeight='MINS_WT',
                                                                                         var_trafficWeight='TRAFFIC_WT',
                                                                                         var_unsampWeight='UNSAMP_TRAFFIC_WT',
                                                                                         var_imbWeight='IMBAL_WT',
                                                                                         var_finalWeight='FINAL_WT')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["sas_ps_table"], summary_data_out)

    # Update Survey Data With Final Wt Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Final Wt Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])

    # Store Final Weight Summary
    idm.store_step_summary(run_id, connection, STEP_CONFIGURATION[step_name])


def stay_imputation_step(run_id,connection):
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


def spend_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the spend imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "SPEND_IMPUTATION"

    # Populate Survey Data For Spend Imputation
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Spend Imp PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Spend Imp PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='serial')

    # Update Survey Data with Spend Imp PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # # TODO: FOR DEBUGGING
    # import pandas as pd
    # sql = """
    #         SELECT [SERIAL], [SPEND] as 'newspend'
    #           FROM {}
    #           WHERE [SPEND_IMP_ELIGIBLE_PV] = '1'
    #         """.format(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    #
    # # Using comparison data populated by Python from unit test due
    # # to random values populated in OPERA_PV. NOT USING SAS BASELINE DATA
    # fares_input = pd.read_sql_query(sql, connection)
    # fares_input.to_csv(r'S:\CASPA\IPS\Testing\scratch\fares_input.csv')
    # # TODO: END DEBUGGING!

    # Calculate Spend Imputation
    survey_data_out = calculate_ips_spend_imputation.do_ips_spend_imputation(survey_data,
                                                                             var_serial="SERIAL",
                                                                             measure="mean")

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data With Spend Imp Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Spend Imp Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])


def rail_imputation_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the rail imputation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "RAIL_IMPUTATION"

    # Populate Survey Data For Rail Imputation
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Rail Imp PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Rail Imp PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_RAIL_SPV',
                              in_id='serial')

    # Update Survey Data with Rail Imp PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # # TODO: FOR DAVE
    # import pandas as pd
    # sql = """
    #             SELECT *
    #               FROM {}
    #             """.format(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    #
    # # Using comparison data populated by Python from unit test due
    # # to random values populated in OPERA_PV. NOT USING SAS BASELINE DATA
    # dave_results = pd.read_sql_query(sql, connection)
    # dave_results.to_csv(r'S:\CASPA\IPS\Testing\Dec_Data\Rail\rail_survey_input.csv')
    # # TODO: END DAVE!

    # Calculate Rail Imputation
    survey_data_out = calculate_ips_rail_imputation.do_ips_railex_imp(survey_data,
                                                                      var_serial='SERIAL',
                                                                      var_final_weight='FINAL_WT',
                                                                      minimum_count_threshold=30)

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data With Rail Imp Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Rail Imp Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])


def regional_weights_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the regional weights steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    """

    # Load configuration variables
    step_name = "REGIONAL_WEIGHTS"

    # Populate Survey Data For Regional Weights
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Copy Regional Weights PVs For Survey Data
    idm.copy_step_pvs_for_survey_data(run_id, connection, STEP_CONFIGURATION[step_name])

    # Apply Regional Weights PVs On Survey Data
    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_REGIONAL_SPV',
                              in_id='serial')

    # Update Survey Data with Regional Weights PV Output
    idm.update_survey_data_with_step_pv_output(connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # # TODO: FOR DAVE
    # import pandas as pd
    # sql = """
    #             SELECT [SERIAL],
    #                     [NIGHTS1],
    #                     [NIGHTS2],
    #                     [NIGHTS3],
    #                     [NIGHTS4],
    #                     [NIGHTS5],
    #                     [NIGHTS6],
    #                     [NIGHTS7],
    #                     [NIGHTS8],
    #                     [EXPENDITURE_WT],
    #                     [EXPENDITURE_WTK],
    #                     [STAY1K],
    #                     [STAY2K],
    #                     [STAY3K],
    #                     [STAY4K],
    #                     [STAY5K],
    #                     [STAY6K],
    #                     [STAY7K],
    #                     [STAY8K],
    #                     [STAY_WT],
    #                     [STAY_WTK],
    #                     [VISIT_WT],
    #                     [VISIT_WTK]
    #               FROM {}
    #             """.format(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    #
    # # Using comparison data populated by Python from unit test due
    # # to random values populated in OPERA_PV. NOT USING SAS BASELINE DATA
    # dave_results = pd.read_sql_query(sql, connection)
    # dave_results.to_csv(r'S:\CASPA\IPS\Testing\Dec_Data\Regional\regional_survey_input.csv')
    # # TODO: END DAVE!

    # Calculate Regional Weights
    survey_data_out = calculate_ips_regional_weights.do_ips_regional_weight_calculation(survey_data,
                                                                                        var_serial='SERIAL',
                                                                                        var_final_weight='FINAL_WT')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data With Regional Weights Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data With Regional Weights Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])


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

    # # TODO: FOR DAVE
    # import pandas as pd
    # sql = """
    #             SELECT *
    #               FROM {}
    #             """.format(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    #
    # # Using comparison data populated by Python from unit test due
    # # to random values populated in OPERA_PV. NOT USING SAS BASELINE DATA
    # dave_results = pd.read_sql_query(sql, connection)
    # dave_results.to_csv(r'S:\CASPA\IPS\Testing\Dec_Data\Town and Expenditure\town_and_stay_survey_input.csv')
    # # TODO: END DAVE!

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


def airmiles_step(run_id, connection):
    """
    Author       : Thomas Mahoney / Elinor Thorne
    Date         : 30 April 2018 / 2 October 2018
    Purpose      : Runs the air miles calculation steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Load configuration variables
    step_name = "AIR_MILES"

    # Populate Survey Data For Air Miles
    idm.populate_survey_data_for_step(run_id, connection, STEP_CONFIGURATION[step_name])

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Calculate Air Miles
    survey_data_out = calculate_ips_airmiles.do_ips_airmiles_calculation(df_surveydata=survey_data, var_serial='SERIAL')

    # Insert data to SQL
    cf.insert_dataframe_into_table(STEP_CONFIGURATION[step_name]["temp_table"], survey_data_out)

    # Update Survey Data with Air Miles Results
    idm.update_survey_data_with_step_results(connection, STEP_CONFIGURATION[step_name])

    # Store Survey Data with Air Miles Results
    idm.store_survey_data_with_step_results(run_id, connection, STEP_CONFIGURATION[step_name])


if __name__ == '__main__':

    # Run Id (this will be generated automatically and will be unique)
    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    # Connection to the SQL server database
    connection = cf.get_sql_connection()

    # -- Processing --
    shift_weight_step(run_id, connection)
    non_response_weight_step(run_id, connection)
    minimums_weight_step(run_id, connection)
    traffic_weight_step(run_id, connection)
    unsampled_weight_step(run_id, connection)
    imbalance_weight_step(run_id, connection)
    final_weight_step(run_id, connection)
    stay_imputation_step(run_id, connection)
    fares_imputation_step(run_id, connection)
    spend_imputation_step(run_id, connection)
    rail_imputation_step(run_id, connection)
    regional_weights_step(run_id, connection)
    town_stay_expenditure_imputation_step(run_id, connection)
    airmiles_step(run_id, connection)
