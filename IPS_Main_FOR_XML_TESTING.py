from main.calculations import calculate_ips_minimums_weight
from IPS_XML import minimums_weight, traffic_weight
from main.io import CommonFunctions as cf
from IPS_Stored_Procedures import process_variables


if __name__ == '__main__':

    connection = cf.get_oracle_connection()

    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    calculate_ips_minimums_weight.calculate(SurveyData='SAS_SURVEY_SUBSAMPLE',
              OutputData='SAS_MINIMUMS_WT',
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

    minimums_weight.update_survey_data_with_min_wt_pv_output(connection)

    minimums_weight.store_survey_data_with_min_wt_results(connection)

    minimums_weight.store_min_wt_summary(connection)

    # Traffic stuff
    traffic_weight.populate_survey_data_for_traffic_wt(connection)

    traffic_weight.populate_traffic_data(run_id, connection)

    traffic_weight.copy_traffic_wt_pvs_for_survey_data(run_id, connection)

    process_variables.process(in_dataset=)

    traffic_weight.update_survey_data_with_traffic_wt_pv_output(connection)

    traffic_weight.copy_traffic_wt_pvs_for_traffic_data(run_id, connection)

    process_variables

    traffic_weight.update_traffic_data_with_pvs_output(connection)