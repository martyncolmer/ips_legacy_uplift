import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
import ips.utils.common_functions as cf
import ips.db.data_management as idm
from ips.utils.common_functions import get_sql_connection
from ips.db.data_management import SURVEY_SUBSAMPLE_TABLE


TEST_DATA_DIR = 'tests/data/ips_data_management/'

# This function establishes a database connection to be used in the test functions
@pytest.fixture(scope='module')
def database_connection():
    '''
    This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again.
    '''
    return get_sql_connection()


# Lower lever test, ensures the nullify function used through some of the idm processes works appropriately
def test_nullify_survey_subsample_values(database_connection):

    # Delete any previous null test records from the tables
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'automated_nullify_test')

    # Get test data
    test_data = pd.read_csv(TEST_DATA_DIR + 'nullify_test_import.csv', dtype=object)

    # Insert the imported data into the survey_subsample table
    cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)

    # Ensure there are no null values in the PV columns
    assert test_data['SHIFT_PORT_GRP_PV'].isnull().sum() == 0
    assert test_data['WEEKDAY_END_PV'].isnull().sum() == 0
    assert test_data['AM_PM_NIGHT_PV'].isnull().sum() == 0
    assert test_data['SHIFT_FLAG_PV'].isnull().sum() == 0
    assert test_data['CROSSINGS_FLAG_PV'].isnull().sum() == 0
    assert test_data['SHIFT_WT'].isnull().sum() == 0

    idm.nullify_survey_subsample_values("automated_nullify_test", database_connection, ["[SHIFT_PORT_GRP_PV]",
                                                                                           "[WEEKDAY_END_PV]",
                                                                                           "[AM_PM_NIGHT_PV]",
                                                                                           "[SHIFT_FLAG_PV]",
                                                                                           "[CROSSINGS_FLAG_PV]",
                                                                                           "[SHIFT_WT]"])

    # Get the pv columns from the survey_subsample table and ensure each record now has a null value
    result = cf.select_data('SHIFT_PORT_GRP_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', 'automated_nullify_test')
    assert result['SHIFT_PORT_GRP_PV'].isnull().sum() == len(result)

    result = cf.select_data('WEEKDAY_END_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "automated_nullify_test")
    assert result['WEEKDAY_END_PV'].isnull().sum() == len(result)

    result = cf.select_data('AM_PM_NIGHT_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "automated_nullify_test")
    assert result['AM_PM_NIGHT_PV'].isnull().sum() == len(result)

    result = cf.select_data('SHIFT_FLAG_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "automated_nullify_test")
    assert result['SHIFT_FLAG_PV'].isnull().sum() == len(result)

    result = cf.select_data('CROSSINGS_FLAG_PV', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "automated_nullify_test")
    assert result['CROSSINGS_FLAG_PV'].isnull().sum() == len(result)

    result = cf.select_data('SHIFT_WT', idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "automated_nullify_test")
    assert result['SHIFT_WT'].isnull().sum() == len(result)

    # Clear down inserted nullify test values
    cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'automated_nullify_test')


@pytest.mark.parametrize('name, delete_tables, nullify_pvs', [
    ("SHIFT_WEIGHT", ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"], ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]", "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"]),
    ("NON_RESPONSE", ["[dbo].[SAS_NON_RESPONSE_WT]", "[dbo].[SAS_PS_NON_RESPONSE]"], ["[NR_PORT_GRP_PV]", "[MIG_FLAG_PV]", "[NR_FLAG_PV]", "[NON_RESPONSE_WT]"]),
    ("MINIMUMS_WEIGHT", ["[dbo].[SAS_MINIMUMS_WT]", "[dbo].[SAS_PS_MINIMUMS]"], ["[MINS_PORT_GRP_PV]", "[MINS_CTRY_GRP_PV]", "[MINS_NAT_GRP_PV]", "[MINS_CTRY_PORT_GRP_PV]", "[MINS_FLAG_PV]", "[MINS_WT]"]),
    ("TRAFFIC_WEIGHT", ["[dbo].[SAS_TRAFFIC_WT]", "[dbo].[SAS_PS_TRAFFIC]"], ["[SAMP_PORT_GRP_PV]", "[FOOT_OR_VEHICLE_PV]", "[HAUL_PV]", "[TRAFFIC_WT]"]),
    ("UNSAMPLED_WEIGHT", ["[dbo].[SAS_UNSAMPLED_OOH_WT]", "[dbo].[SAS_PS_UNSAMPLED_OOH]"], ["[UNSAMP_PORT_GRP_PV]", "[UNSAMP_REGION_GRP_PV]", "[UNSAMP_TRAFFIC_WT]"]),
    ("IMBALANCE_WEIGHT", ["[dbo].[SAS_IMBALANCE_WT]", "[dbo].[SAS_PS_IMBALANCE]"], ["[IMBAL_PORT_GRP_PV]", "[IMBAL_PORT_FACT_PV]", "[IMBAL_CTRY_FACT_PV]", "[IMBAL_ELIGIBLE_PV]", "[IMBAL_WT]"]),
    ("FINAL_WEIGHT", ["[dbo].[SAS_FINAL_WT]", "[dbo].[SAS_PS_FINAL]"], ["[FINAL_WT]"]),
    ("STAY_IMPUTATION", ["[dbo].[SAS_STAY_IMP]"], ["[STAY_IMP_FLAG_PV]", "[STAY_IMP_ELIGIBLE_PV]", "[STAYIMPCTRYLEVEL1_PV]", "[STAYIMPCTRYLEVEL2_PV]", "[STAYIMPCTRYLEVEL3_PV]", "[STAYIMPCTRYLEVEL4_PV]", "[STAY_PURPOSE_GRP_PV]", "[STAY]", "[STAYK]"]),
    ("FARES_IMPUTATION", ["[dbo].[SAS_FARES_IMP]"], ["[FARES_IMP_FLAG_PV]", "[FARES_IMP_ELIGIBLE_PV]", "[DISCNT_PACKAGE_COST_PV]", "[DISCNT_F1_PV]", "[DISCNT_F2_PV]", "[FAGE_PV]", "[OPERA_PV]", "[TYPE_PV]", "[UKPORT1_PV]", "[UKPORT2_PV]", "[UKPORT3_PV]", "[UKPORT4_PV]", "[OSPORT1_PV]", "[OSPORT2_PV]", "[OSPORT3_PV]", "[OSPORT4_PV]", "[APD_PV]", "[QMFARE_PV]", "[DUTY_FREE_PV]", "[FARE]", "[FAREK]", "[SPEND]","[SPENDIMPREASON]"]),
    ("SPEND_IMPUTATION", ["[dbo].[SAS_SPEND_IMP]"], ["[SPEND_IMP_FLAG_PV]", "[SPEND_IMP_ELIGIBLE_PV]", "[UK_OS_PV]", "[PUR1_PV]", "[PUR2_PV]", "[PUR3_PV]", "[DUR1_PV]", "[DUR2_PV]", "[SPENDK]"]),
    ("RAIL_IMPUTATION", ["[dbo].[SAS_RAIL_IMP]"], ["[RAIL_CNTRY_GRP_PV]", "[RAIL_EXERCISE_PV]", "[RAIL_IMP_ELIGIBLE_PV]"]),
    ("REGIONAL_WEIGHTS", ["[dbo].[SAS_REGIONAL_IMP]"], ["[PURPOSE_PV]", "[STAYIMPCTRYLEVEL1_PV]", "[STAYIMPCTRYLEVEL2_PV]","[STAYIMPCTRYLEVEL3_PV]", "[STAYIMPCTRYLEVEL4_PV]", "[REG_IMP_ELIGIBLE_PV]", "[VISIT_WT]", "[VISIT_WTK]", "[STAY_WT]", "[STAY_WTK]", "[EXPENDITURE_WT]", "[EXPENDITURE_WTK]", "[NIGHTS1]", "[NIGHTS2]", "[NIGHTS3]", "[NIGHTS4]", "[NIGHTS5]", "[NIGHTS6]", "[NIGHTS7]", "[NIGHTS8]", "[STAY1K]", "[STAY2K]", "[STAY3K]", "[STAY4K]", "[STAY5K]", "[STAY6K]", "[STAY7K]", "[STAY8K]"]),
    ("TOWN_AND_STAY_EXPENDITURE", ["[dbo].[SAS_TOWN_STAY_IMP]"], ["[PURPOSE_PV]", "[STAYIMPCTRYLEVEL1_PV]", "[STAYIMPCTRYLEVEL2_PV]", "[STAYIMPCTRYLEVEL3_PV]" , "[STAYIMPCTRYLEVEL4_PV]", "[TOWN_IMP_ELIGIBLE_PV]", "[SPEND1]", "[SPEND2]", "[SPEND3]", "[SPEND4]", "[SPEND5]", "[SPEND6]" , "[SPEND7]", "[SPEND8]"]),
    ("AIR_MILES", ["[dbo].[SAS_AIR_MILES]"], ["[DIRECTLEG]", "[OVLEG]", "[UKLEG]"]),
    ])
def test_populate_survey_data(name, delete_tables, nullify_pvs, database_connection):

    # This test is parameterised. The values for the arguments of this test function
    # are taken from the parameters specified in pytest.mark.parametrize

    # Delete existing survey data from table where RUN_ID matches our test id
    cf.delete_from_table(SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', '9e5c1872-3f8e-4ae5-85dc-c67a602d011e')

    # Read the test data in from a csv file
    test_data = pd.read_csv(TEST_DATA_DIR + "populate_survey_data/survey_subsample.csv", dtype=object)

    # Insert the test data into survey_subsample table
    cf.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, test_data)

    # Setup step configuration
    step_config = {'nullify_pvs': nullify_pvs,
                   'name': name,
                   'delete_tables': delete_tables}

    # Run test function
    idm.populate_survey_data_for_step(run_id='9e5c1872-3f8e-4ae5-85dc-c67a602d011e', conn=database_connection, step_configuration=step_config)

    # Get test_result from sas_survey_subsample table
    test_result = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    # Write the test results to a csv
    test_result.to_csv(TEST_DATA_DIR + "populate_survey_data/test_result.csv", index=False)

    # Import the expected result (this result varies if the TRAFFIC_WEIGHT or UNSAMPLED_WEIGHT step is being tested)
    if name == 'TRAFFIC_WEIGHT' or name == 'UNSAMPLED_WEIGHT':
        expected_result = pd.read_csv(TEST_DATA_DIR + "populate_survey_data/populate_result_traffic_unsampled.csv")
    else:
        expected_result = pd.read_csv(TEST_DATA_DIR + "populate_survey_data/populate_result.csv")

    # Import the test result
    test_result = pd.read_csv(TEST_DATA_DIR + "populate_survey_data/test_result.csv")

    # Sort the values by SERIAL
    expected_result = expected_result.sort_values(by='SERIAL')
    test_result = test_result.sort_values(by='SERIAL')

    # Reset the dataframe's indexes so correct rows are compared
    expected_result.index = range(0, len(expected_result))
    test_result.index = range(0, len(test_result))

    # Check all deleted tables are empty
    for table in step_config['delete_tables']:
        delete_result = cf.get_table_values(table)
        assert delete_result.empty

    # Check all nullified columns are NULL
    for column in step_config['nullify_pvs']:
        column_name = column.replace('[', '').replace(']', '')
        result = cf.select_data(column_name, idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "9e5c1872-3f8e-4ae5-85dc-c67a602d011e")
        assert result[column_name].isnull().sum() == len(result)

    # Check results match
    assert_frame_equal(expected_result, test_result, check_dtype=False, check_like=True)


@pytest.mark.parametrize('table_name, data_table, insert_to_populate, step_data, sas_step_data, result_data', [
    ("[dbo].[SHIFT_DATA]", "[dbo].[SAS_SHIFT_DATA]", ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]", "[AM_PM_NIGHT]"], 'shift_data.csv', 'sas_shift_data.csv', 'shift_data_result.csv'),
    ("[dbo].[NON_RESPONSE_DATA]", "[dbo].[SAS_NON_RESPONSE_DATA]", ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[AM_PM_NIGHT]", "[SAMPINTERVAL]", "[MIGTOTAL]", "[ORDTOTAL]"], 'non_response_data.csv', 'sas_non_response_data.csv', 'non_response_data_result.csv'),
    ("[dbo].[TRAFFIC_DATA]", "[dbo].[SAS_TRAFFIC_DATA]", ["[PORTROUTE]", "[ARRIVEDEPART]", "[TRAFFICTOTAL]", "[PERIODSTART]", "[PERIODEND]", "[AM_PM_NIGHT]", "[HAUL]", "[VEHICLE]"], 'traffic_data.csv', 'sas_traffic_data.csv', 'traffic_data_result.csv'),
    ("[dbo].[UNSAMPLED_OOH_DATA]", "[dbo].[SAS_UNSAMPLED_OOH_DATA]", ["[PORTROUTE]", "[REGION]", "[ARRIVEDEPART]", "[UNSAMP_TOTAL]"], 'unsampled_ooh_data.csv', 'sas_unsampled_ooh_data.csv', 'unsampled_ooh_data_result.csv'),
])
def test_populate_step_data(table_name, data_table, insert_to_populate, step_data, sas_step_data, result_data, database_connection):

    # This test is parameterised. The values for the arguments of this test function
    # are taken from the parameters specified in pytest.mark.parametrize

    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    # Setup step configuration
    step_config = {"table_name": table_name,
                   "data_table": data_table,
                   "insert_to_populate": insert_to_populate,
                   }

    # Clear existing test records from the shift_data table
    cf.delete_from_table(step_config['table_name'], 'RUN_ID', '=', '9e5c1872-3f8e-4ae5-85dc-c67a602d011e')

    # Get test data from file
    test_data = pd.read_csv(TEST_DATA_DIR + "populate_step_data/"+step_data, dtype=object)

    # Insert test data into table
    cf.insert_dataframe_into_table(step_config["table_name"], test_data)

    # Run XML step which deletes old data from sas_survey_subsample and repopulates it with the new data
    idm.populate_step_data(run_id, database_connection, step_config)

    # Get test_result from (sas) external data table
    test_result = cf.get_table_values(step_config['data_table'])

    # Write the test results to a csv
    test_result.to_csv(TEST_DATA_DIR + "populate_step_data/" + result_data, index=False)

    # Import both the expected result and test result from the csv files
    expected_result = pd.read_csv(TEST_DATA_DIR + "populate_step_data/" + sas_step_data)
    test_result = pd.read_csv(TEST_DATA_DIR + "populate_step_data/" + result_data)

    # Nullify the rec_id for comparison (this needs to be done because the expected result contains no rec_id)
    expected_result['REC_ID'] = ''
    test_result['REC_ID'] = ''

    # Sort records to match order
    if table_name == '[dbo].[SHIFT_DATA]':
        expected_result = expected_result.sort_values(by=['PORTROUTE', 'WEEKDAY'])
        test_result = test_result.sort_values(by=['PORTROUTE', 'WEEKDAY'])
    elif table_name == '[dbo].[NON_RESPONSE_DATA]':
        expected_result = expected_result.sort_values(by=['PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'AM_PM_NIGHT', 'SAMPINTERVAL', 'MIGTOTAL', 'ORDTOTAL'])
        test_result = test_result.sort_values(by=['PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'AM_PM_NIGHT', 'SAMPINTERVAL', 'MIGTOTAL', 'ORDTOTAL'])
    elif table_name == '[dbo].[UNSAMPLED_OOH_DATA]':
        expected_result = expected_result.sort_values(by=['PORTROUTE', 'REGION', 'ARRIVEDEPART', 'UNSAMP_TOTAL'])
        test_result = test_result.sort_values(by=['PORTROUTE', 'REGION', 'ARRIVEDEPART', 'UNSAMP_TOTAL'])
    elif table_name == '[dbo].[TRAFFIC_DATA]':
        expected_result = expected_result.sort_values(by=['PORTROUTE', 'ARRIVEDEPART', 'TRAFFICTOTAL', 'HAUL'])
        test_result = test_result.sort_values(by=['PORTROUTE', 'ARRIVEDEPART', 'TRAFFICTOTAL', 'HAUL'])

    # Reset the dataframe's indexes so correct rows are compared
    expected_result.index = range(0, len(expected_result))
    test_result.index = range(0, len(test_result))

    # Check results match
    assert_frame_equal(expected_result, test_result, check_dtype=False, check_like=True)


@pytest.mark.parametrize('step_name, pv_columns, spv_table', [
    ("SHIFT_WEIGHT", ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'", "'SHIFT_FLAG_PV'", "'CROSSINGS_FLAG_PV'"], "[dbo].[SAS_SHIFT_SPV]"),
    ("NON_RESPONSE", ["'NR_PORT_GRP_PV'", "'MIG_FLAG_PV'", "'NR_FLAG_PV'"], "[dbo].[SAS_NON_RESPONSE_SPV]"),
    ("MINIMUMS_WEIGHT", ["'MINS_PORT_GRP_PV'", "'MINS_CTRY_GRP_PV'", "'MINS_FLAG_PV'"], "[dbo].[SAS_MINIMUMS_SPV]"),
    ("TRAFFIC_WEIGHT", ["'SAMP_PORT_GRP_PV'"], "[dbo].[SAS_TRAFFIC_SPV]"),
    ("UNSAMPLED_WEIGHT", ["'UNSAMP_PORT_GRP_PV'", "'UNSAMP_REGION_GRP_PV'"], "[dbo].[SAS_UNSAMPLED_OOH_SPV]"),
    ("IMBALANCE_WEIGHT", ["'IMBAL_PORT_GRP_PV'", "'IMBAL_CTRY_FACT_PV'", "'IMBAL_PORT_FACT_PV'", "'IMBAL_ELIGIBLE_PV'"], "[dbo].[SAS_IMBALANCE_SPV]"),
    ("STAY_IMPUTATION", ["'STAY_IMP_FLAG_PV'", "'STAY_IMP_ELIGIBLE_PV'", "'STAYIMPCTRYLEVEL1_PV'", "'STAYIMPCTRYLEVEL2_PV'", "'STAYIMPCTRYLEVEL3_PV'", "'STAYIMPCTRYLEVEL4_PV'", "'STAY_PURPOSE_GRP_PV'"], "[dbo].[SAS_STAY_SPV]"),
    ("FARES_IMPUTATION", ["'FARES_IMP_FLAG_PV'", "'FARES_IMP_ELIGIBLE_PV'", "'DISCNT_F1_PV'", "'DISCNT_PACKAGE_COST_PV'", "'DISCNT_F2_PV'", "'FAGE_PV'", "'TYPE_PV'", "'OPERA_PV'", "'UKPORT1_PV'", "'UKPORT2_PV'", "'UKPORT3_PV'", "'UKPORT4_PV'", "'OSPORT1_PV'", "'OSPORT2_PV'", "'OSPORT3_PV'", "'OSPORT4_PV'", "'APD_PV'", "'QMFARE_PV'", "'DUTY_FREE_PV'"], "[dbo].[SAS_FARES_SPV]"),
    ("SPEND_IMPUTATION", ["'SPEND_IMP_ELIGIBLE_PV'", "'SPEND_IMP_FLAG_PV'", "'UK_OS_PV'", "'PUR1_PV'", "'PUR2_PV'", "'PUR3_PV'", "'DUR1_PV'", "'DUR2_PV'"], "[dbo].[SAS_SPEND_SPV]"),
    ("RAIL_IMPUTATION", ["'RAIL_CNTRY_GRP_PV'", "'RAIL_EXERCISE_PV'", "'RAIL_IMP_ELIGIBLE_PV'"], "[dbo].[SAS_RAIL_SPV]"),
    ("REGIONAL_WEIGHTS", ["'PURPOSE_PV'", "'STAYIMPCTRYLEVEL1_PV'", "'STAYIMPCTRYLEVEL2_PV'", "'STAYIMPCTRYLEVEL3_PV'", "'STAYIMPCTRYLEVEL4_PV'", "'REG_IMP_ELIGIBLE_PV'"], "[dbo].[SAS_REGIONAL_SPV]"),
    ("TOWN_AND_STAY_EXPENDITURE", ["'PURPOSE_PV'", "'STAYIMPCTRYLEVEL1_PV'", "'STAYIMPCTRYLEVEL2_PV'", "'STAYIMPCTRYLEVEL3_PV'", "'STAYIMPCTRYLEVEL4_PV'", "'TOWN_IMP_ELIGIBLE_PV'"], "[dbo].[SAS_TOWN_STAY_SPV]")
])
def test_copy_step_pvs_for_survey_data(step_name, pv_columns, spv_table, database_connection):

    # This test is parameterised. The values for the arguments of this test function
    # are taken from the parameters specified in pytest.mark.parametrize

    # Setup step configuration variables
    step_config = {'name': step_name,
                   'spv_table': spv_table,
                   'pv_columns': pv_columns}

    run_id = 'TEMPLATE'

    idm.copy_step_pvs_for_survey_data(run_id, database_connection, step_config)

    # Get all values from the sas_process_variables table
    results = cf.get_table_values(idm.SAS_PROCESS_VARIABLES_TABLE)

    # Copy the PV column names
    pv_names = step_config['pv_columns']

    # Strip quotation marks out of the pv_names to use in column comparisons
    for i in range(0, len(pv_names)):
        pv_names[i] = pv_names[i].replace("'", "")

    # Check number of PV records moved matches number passed in through step configuration.
    assert len(results) == len(step_config['pv_columns'])

    # Ensure the pv_names in the results data frame match the expected pv names
    for name in results['PROCVAR_NAME']:
        assert name.upper() in pv_names

    # Get the spv_table values and ensure all records have been deleted
    results = cf.get_table_values(step_config['spv_table'])
    assert len(results) == 0
