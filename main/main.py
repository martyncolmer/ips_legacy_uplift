'''
Created on 26 April 2018

@author: Thomas Mahoney
'''

import sys
from main.io import CommonFunctions as cf
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
# from main.calculations import calculate_ips_town_and_stay_expenditure_imputation
from main.calculations import calculate_ips_airmiles
from IPS_XML import shift_weight
from IPS_XML import non_response
from IPS_XML import minimums_weight
from IPS_XML import traffic_weight
from IPS_XML import unsampled_weight
from IPS_XML import imbalance_weight
from IPS_XML import final_weight
from IPS_XML import stay_imputation
from IPS_XML import fares_imputation
from IPS_XML import spend_imputation
from IPS_XML import rail_imputation
from IPS_XML import regional_weights
from IPS_XML import town_and_stay_expenditure
from IPS_XML import air_miles


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

    shift_weight.populate_survey_data_for_shift_wt(run_id, connection)

    shift_weight.populate_shift_data(run_id, connection)

    shift_weight.copy_shift_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SHIFT_SPV',
                              in_id='serial')

    shift_weight.update_survey_data_with_shift_wt_pv_output(connection)

    shift_weight.copy_shift_wt_pvs_for_shift_data(run_id, connection)

    process_variables.process(dataset='shift',
                              in_table_name='SAS_SHIFT_DATA',
                              out_table_name='SAS_SHIFT_PV',
                              in_id='REC_ID')

    shift_weight.update_shift_data_with_pvs_output(connection)

    calculate_ips_shift_weight.calculate(SurveyData='SAS_SURVEY_SUBSAMPLE',
                                         ShiftsData='SAS_SHIFT_DATA',
                                         OutputData='SAS_SHIFT_WT',
                                         SummaryData='SAS_PS_SHIFT_DATA',
                                         ResponseTable='SAS_RESPONSE',
                                         ShiftsStratumDef=['SHIFT_PORT_GRP_PV',
                                                           'ARRIVEDEPART',
                                                           'WEEKDAY_END_PV',
                                                           'AM_PM_NIGHT_PV'],
                                         var_serialNum='SERIAL',
                                         var_shiftFlag='SHIFT_FLAG_PV',
                                         var_shiftFactor='SHIFT_FACTOR',
                                         var_totals='TOTAL',
                                         var_shiftNumber='SHIFTNO',
                                         var_crossingFlag='CROSSINGS_FLAG_PV',
                                         var_crossingsFactor='CROSSINGS_FACTOR',
                                         var_crossingNumber='SHUTTLE',
                                         var_SI='MIGSI',
                                         var_shiftWeight='SHIFT_WT',
                                         var_count='COUNT_RESPS',
                                         var_weightSum='SUM_SH_WT',
                                         var_minWeight='MIN_SH_WT',
                                         var_avgWeight='MEAN_SH_WT',
                                         var_maxWeight='MAX_SH_WT',
                                         var_summaryKey='SHIFT_PORT_GRP_PV',
                                         subStrata=['SHIFT_PORT_GRP_PV',
                                                    'ARRIVEDEPART'],
                                         var_possibleCount='POSS_SHIFT_CROSS',
                                         var_sampledCount='SAMP_SHIFT_CROSS',
                                         minWeightThresh='50',
                                         maxWeightThresh='5000')

    print("Start - update_survey_data_with_shift_wt_results")
    shift_weight.update_survey_data_with_shift_wt_results(connection)

    print("Start - store_survey_data_with_shift_wt_results")
    shift_weight.store_survey_data_with_shift_wt_results(run_id, connection)

    print("Start - store_shift_wt_summary")
    shift_weight.store_shift_wt_summary(run_id, connection)


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

    non_response.populate_survey_data_for_non_response_wt(run_id, connection)

    non_response.populate_non_response_data(run_id, connection)

    non_response.copy_non_response_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_NON_RESPONSE_SPV',
                              in_id='serial')

    non_response.update_survey_data_with_non_response_wt_pv_output(connection)

    non_response.copy_non_response_wt_pvs_for_non_response_data(run_id, connection)

    process_variables.process(dataset='non_response',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_NON_RESPONSE_PV',
                              in_id='REC_ID')

    non_response.update_non_response_data_with_pvs_output(connection)

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
    non_response.update_survey_data_with_non_response_wt_results(connection)

    print("Start - store_survey_data_with_non_response_wt_results")
    non_response.store_survey_data_with_non_response_wt_results(run_id, connection)

    print("Start - store_non_response_wt_summary")
    non_response.store_non_response_wt_summary(run_id, connection)


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

    minimums_weight.populate_survey_data_for_min_wt(run_id, connection)

    minimums_weight.copy_min_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_MINIMUMS_SPV',
                              in_id='serial')

    minimums_weight.update_survey_data_with_min_wt_pv_output(connection)

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

    minimums_weight.update_survey_data_with_min_wt_results(connection)

    minimums_weight.store_survey_data_with_min_wt_results(run_id, connection)

    minimums_weight.store_min_wt_summary(run_id, connection)


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

    traffic_weight.populate_survey_data_for_traffic_wt(run_id, connection)

    traffic_weight.populate_traffic_data(run_id, connection)

    traffic_weight.copy_traffic_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TRAFFIC_SPV',
                              in_id='serial')

    traffic_weight.update_survey_data_with_traffic_wt_pv_output(connection)

    traffic_weight.copy_traffic_wt_pvs_for_traffic_data(run_id, connection)

    process_variables.process(dataset='traffic',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_TRAFFIC_PV',
                              in_id='REC_ID')

    traffic_weight.update_traffic_data_with_pvs_output(connection)

    calculate_ips_traffic_weight.calculate()

    traffic_weight.update_survey_data_with_traffic_wt_results(connection)

    traffic_weight.store_survey_data_with_traffic_wt_results(run_id, connection)

    traffic_weight.store_traffic_wt_summary(run_id, connection)


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

    unsampled_weight.populate_survey_data_for_unsampled_wt(run_id,connection)

    unsampled_weight.populate_unsampled_data(run_id,connection)

    unsampled_weight.copy_unsampled_wt_pvs_for_survey_data(run_id,connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_UNSAMPLED_OOH_SPV',
                              in_id='serial')

    unsampled_weight.update_survey_data_with_unsampled_wt_pv_output(connection)

    unsampled_weight.copy_unsampled_wt_pvs_for_unsampled_data(run_id,connection)

    process_variables.process(dataset='unsampled',
                              in_table_name='SAS_NON_RESPONSE_DATA',
                              out_table_name='SAS_UNSAMPLED_OOH_PV',
                              in_id='REC_ID')

    unsampled_weight.update_unsampled_data_with_pv_output(connection)

    calculate_ips_unsampled_weight.calculate()

    unsampled_weight.update_survey_data_with_unsampled_wt_results(connection)

    unsampled_weight.store_survey_data_with_unsampled_wt_results(run_id,connection)

    unsampled_weight.store_unsampled_wt_summary(run_id,connection)


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

    imbalance_weight.populate_survey_data_for_imbalance_wt(run_id, connection)

    imbalance_weight.copy_imbalance_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_IMBALANCE_SPV',
                              in_id='serial')

    imbalance_weight.update_survey_data_with_imbalance_wt_pvs_output(connection)

    calculate_ips_imb_weight.calculate()

    imbalance_weight.update_survey_data_with_imbalance_wt_results(connection)

    imbalance_weight.store_survey_data_with_imbalance_wt_results(run_id, connection)

    imbalance_weight.store_imbalance_weight_summary(run_id, connection)


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

    final_weight.populate_survey_data_for_final_wt(run_id, connection)

    calculate_ips_final_weight.calculate()

    final_weight.update_survey_data_with_final_wt_results(connection)

    final_weight.store_survey_data_with_final_wt_results(run_id, connection)

    final_weight.store_final_weight_summary(run_id, connection)


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

    stay_imputation.populate_survey_data_for_stay_imputation(run_id, connection)

    stay_imputation.copy_stay_imp_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_STAY_SPV',
                              in_id='serial')

    stay_imputation.update_survey_data_with_stay_imp_pv_output(connection)

    calculate_ips_stay_imputation.ips_stay_imp()

    stay_imputation.update_survey_data_with_stay_imp_results(connection)

    stay_imputation.store_survey_data_with_stay_imp_results(run_id,connection)


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

    fares_imputation.populate_survey_data_for_fares_imputation(run_id, connection)

    fares_imputation.copy_fares_imp_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_FARES_SPV',
                              in_id='serial')

    fares_imputation.update_survey_data_with_fares_imp_pv_output(connection)

    calculate_ips_fares_imputation.ips_fares_imp()

    fares_imputation.update_survey_data_with_fares_imp_results(connection)

    fares_imputation.store_survey_data_with_fares_imp_results(run_id, connection)


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

    spend_imputation.populate_survey_data_for_spend_imputation(run_id, connection)

    spend_imputation.copy_spend_imp_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SPEND_SPV',
                              in_id='serial')

    spend_imputation.update_survey_data_with_spend_imp_pv_output(connection)

    calculate_ips_spend_imputation.calculate()

    spend_imputation.update_survey_data_with_spend_imp_results(connection)

    spend_imputation.store_survey_data_with_spend_imp_results(run_id, connection)


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

    rail_imputation.populate_survey_data_for_rail_imputation(run_id, connection)

    rail_imputation.copy_rail_imp_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_RAIL_SPV',
                              in_id='serial')

    rail_imputation.update_survey_data_with_rail_imp_pv_output(connection)

    calculate_ips_rail_imputation.calculate()

    rail_imputation.update_survey_data_with_rail_imp_results(connection)

    rail_imputation.store_survey_data_with_rail_imp_results(run_id, connection)


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

    regional_weights.populate_survey_data_for_regional_wt(run_id, connection)

    regional_weights.copy_regional_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_REGIONAL_SPV',
                              in_id='serial')

    regional_weights.update_survey_data_with_regional_wt_pv_output(connection)

    calculate_ips_regional_weights.calculate()

    regional_weights.update_survey_data_with_regional_wt_results(connection)

    regional_weights.store_survey_data_with_regional_wt_results(run_id, connection)


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

    town_and_stay_expenditure.populate_survey_data_for_TSE_imputation(run_id, connection)

    town_and_stay_expenditure.copy_TSE_imputationt_pvs_for_survey_data(run_id, connection)

    process_variables.process(dataset='survey',
                              in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_TOWN_STAY_SPV',
                              in_id='serial')

    town_and_stay_expenditure.update_survey_data_with_TSE_imputation_pv_output(connection)

    # calculation for town stay (still not complete)

    town_and_stay_expenditure.update_survey_data_with_TSE_imputation_results(connection)

    town_and_stay_expenditure.store_survey_data_with_TSE_imputation_results(run_id, connection)


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

    air_miles.populate_survey_data_for_air_miles(run_id, connection)

    calculate_ips_airmiles.calculate()

    air_miles.update_survey_data_with_air_miles_results(connection)

    air_miles.store_survey_data_with_air_miles_results(run_id, connection)


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
