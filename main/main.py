'''
Created on 26 April 2018

@author: Thomas Mahoney
'''

import sys
from main.io import CommonFunctions as cf, generic
from main.io import import_data
from main.io import import_traffic_data
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


def import_step(run_id, version_id):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the import step of the ips process
    Params       : run_id - the id for the current run.
                   version_id - the version id for the current run.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Import data paths (these will be passed in through the user)
    survey_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\ips1712bv4_amtspnd.sas7bdat"
    shift_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts Q1 2017.csv'
    nr_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response Q1 2017.csv'
    sea_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv'
    tunnel_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv'
    air_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017.csv'
    unsampled_data_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv'

    # Import survey data function to go here
    import_data.import_survey_data(survey_data_path, run_id, version_id)

    # Import Shift Data
    import_traffic_data.import_traffic_data(run_id, shift_data_path)
    import_traffic_data.import_traffic_data(run_id, nr_data_path)
    import_traffic_data.import_traffic_data(run_id, sea_data_path)
    import_traffic_data.import_traffic_data(run_id, tunnel_data_path)
    import_traffic_data.import_traffic_data(run_id, air_data_path)
    import_traffic_data.import_traffic_data(run_id, unsampled_data_path)


def shift_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 26 April 2018
    Purpose      : Runs the shift weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "SHIFT_WEIGHT"

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.populate_step_data(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SHIFT_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)
    generic.copy_step_pvs_for_step_data(run_id, connection, step)

    process_variables.process(dataset='shift',
                              in_table_name='SAS_SHIFT_DATA',
                              out_table_name='SAS_SHIFT_PV',
                              in_id='REC_ID')

    generic.update_survey_data_with_step_pv_output(connection, step)

    sas_survey_data = cf.get_table_values("SAS_SURVEY_SUBSAMPLE")
    sas_shift_data = cf.get_table_values("SAS_SHIFT_DATA")
    calculate_ips_shift_weight.do_ips_shift_weight_calculation(sas_survey_data,
                                                               sas_shift_data,
                                                               var_serialNum='SERIAL',
                                                               var_shiftWeight='SHIFT_WT')

    generic.update_survey_data_with_step_results(connection, step)

    generic.store_survey_data_with_step_results(run_id, connection, step)

    generic.store_step_summary(run_id, connection, step)


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

    step = "NON_RESPONSE"

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.populate_step_data(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_NON_RESPONSE_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)
    generic.copy_step_pvs_for_step_data(run_id, connection, step)

    process_variables.process(dataset='non_response',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_NON_RESPONSE_PV',
                              in_id='REC_ID')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_nonresponse_weight.calculate(SurveyData='SAS_SURVEY_SUBSAMPLE',
                                               NonResponseData='SAS_NON_RESPONSE_DATA',
                                               OutputData='SAS_NON_RESPONSE_WT',
                                               SummaryData='SAS_PS_NON_RESPONSE',
                                               ResponseTable='SAS_RESPONSE',
                                               NRStratumDef=['NR_PORT_GRP_PV',
                                                             'ARRIVEDEPART'],
                                               ShiftsStratumDef=['NR_PORT_GRP_PV',
                                                                 'ARRIVEDEPART',
                                                                 'WEEKDAY_END_PV'],
                                               var_NRtotals='MIGTOTAL',
                                               var_NonMigTotals='ORDTOTAL',
                                               var_SI='',
                                               var_migSI='MIGSI',
                                               var_TandTSI='TANDTSI',
                                               var_PSW='SHIFT_WT',
                                               var_NRFlag='NR_FLAG_PV',
                                               var_migFlag='MIG_FLAG_PV',
                                               var_respCount='COUNT_RESPS',
                                               var_NRWeight='NON_RESPONSE_WT',
                                               var_meanSW='MEAN_RESPS_SH_WT',
                                               var_priorSum='PRIOR_SUM',
                                               var_meanNRW='MEAN_NR_WT',
                                               var_grossResp='GROSS_RESP',
                                               var_gnr='GNR',
                                               var_serialNum='SERIAL',
                                               minCountThresh='30')

    print("Start - update_survey_data_with_non_response_wt_results")
    generic.update_survey_data_with_step_results(connection, step)

    print("Start - store_survey_data_with_non_response_wt_results")
    generic.store_survey_data_with_step_results(run_id, connection, step)

    print("Start - store_non_response_wt_summary")
    generic.store_step_summary(run_id, connection, step)


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

    step = "MINIMUMS_WEIGHT"

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_MINIMUMS_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_minimums_weight.calculate(SurveyData='SAS_SURVEY_SUBSAMPLE',
                                            OutputData='SAS_NON_RESPONSE_DATA',
                                            SummaryData='SAS_PS_MINIMUMS',
                                            ResponseTable='SAS_RESPONSE',
                                            MinStratumDef=['MINS_PORT_GRP_PV',
                                                           'MINS_CTRY_GRP_PV'],
                                            var_serialNum='SERIAL',
                                            var_shiftWeight='SHIFT_WT',
                                            var_NRWeight='NON_RESPONSE_WT',
                                            var_minWeight='MINS_WT',
                                            var_minCount='MINS_CASES',
                                            var_fullRespCount='FULLS_CASES',
                                            var_minFlag='MINS_FLAG_PV',
                                            var_sumPriorWeightMin='PRIOR_GROSS_MINS',
                                            var_sumPriorWeightFull='PRIOR_GROSS_FULLS',
                                            var_sumPriorWeightAll='PRIOR_GROSS_ALL',
                                            var_sumPostWeight='POST_SUM',
                                            var_casesCarriedForward='CASES_CARRIED_FWD',
                                            minCountThresh='30')

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)
    generic.store_step_summary(run_id, connection, step)


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

    step = "TRAFFIC_WEIGHT"

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.populate_step_data(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TRAFFIC_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)
    generic.copy_step_pvs_for_step_data(run_id, connection, step)

    process_variables.process(dataset='traffic',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_TRAFFIC_PV',
                              in_id='REC_ID')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_traffic_weight.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)
    generic.store_step_summary(run_id, connection, step)


def unsampled_weight_step(run_id, connection):
    """
    Author       : Thomas Mahoney
    Date         : 30 April 2018
    Purpose      : Runs the unsampled weight steps of the ips process
    Params       : run_id - the id for the current run.
                   connection - a connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    step = "UNSAMPLED_WEIGHT"

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.populate_step_data(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_UNSAMPLED_OOH_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)
    generic.copy_step_pvs_for_step_data(run_id, connection, step)

    process_variables.process(dataset='unsampled',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_UNSAMPLED_OOH_PV',
                              in_id='REC_ID')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_unsampled_weight.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)
    generic.store_step_summary(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_IMBALANCE_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_imb_weight.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)
    generic.store_step_summary(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)

    calculate_ips_final_weight.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)
    generic.store_step_summary(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_STAY_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_stay_imputation.ips_stay_imp()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_FARES_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_fares_imputation.ips_fares_imp()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_spend_imputation.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_RAIL_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_rail_imputation.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_REGIONAL_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    calculate_ips_regional_weights.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)
    generic.copy_step_pvs_for_survey_data(run_id, connection, step)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TOWN_STAY_SPV',
                              in_id='serial')

    generic.update_survey_data_with_step_pv_output(connection, step)

    # calculation for town stay (still not complete)

    generic.update_survey_data_with_step_results(connection, step)

    generic.store_survey_data_with_step_results(run_id, connection, step)


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

    generic.populate_survey_data_for_step(run_id, connection, step)

    calculate_ips_airmiles.calculate()

    generic.update_survey_data_with_step_results(connection, step)
    generic.store_survey_data_with_step_results(run_id, connection, step)


if __name__ == '__main__':

    # Connection to the SQL server database
    connection = cf.get_oracle_connection()
    # Run Id (this will be generated automatically and will be unique)
    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    # What was this for?
    version_id = 1891

    # -- Import --
    import_step(run_id, version_id)

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

    # -- Export --
    sys.exit()