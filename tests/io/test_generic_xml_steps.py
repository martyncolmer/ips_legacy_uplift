from main.io import import_data
from main.io import import_traffic_data
import main.io.CommonFunctions as cf
from main.io.CommonFunctions import get_oracle_connection
import main.io.generic_xml_steps as gxs
import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.main import shift_weight_step

@pytest.fixture(scope='module')
def database_connection():
    return get_oracle_connection()

@pytest.fixture(scope='module')
def import_data_into_database():
    run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    version_id = 1891

    # Import data paths (these will be passed in through the user)
    survey_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\ips1712bv4_amtspnd.sas7bdat"
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


def test_nullify_survey_subsample_pv_values(database_connection):
    test_data = pd.read_pickle('tests/data/generic_xml_steps/nullify_pv_survey_data.pkl')
    # test to make sure our test data is different from the data after applying the function
    assert test_data['SHIFT_PORT_GRP_PV'].isnull().sum() == 0
    assert test_data['WEEKDAY_END_PV'].isnull().sum() == 0

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)
    gxs.nullify_survey_subsample_pv_values("nullify-test", database_connection, ["[SHIFT_PORT_GRP_PV]",
                                                                                 "[WEEKDAY_END_PV]"])

    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'nullify-test')

    result = cf.select_data('SHIFT_PORT_GRP_PV', gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', 'nullify-test')
    assert result['SHIFT_PORT_GRP_PV'].isnull().sum() == len(result)
    result = cf.select_data('WEEKDAY_END_PV', gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', "nullify-test")
    assert result['WEEKDAY_END_PV'].isnull().sum() == len(result)


def test_move_survey_subsample_to_sas_table(database_connection):
    test_data = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample.pkl')

    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)
    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    gxs.move_survey_subsample_to_sas_table('move-survey-test', database_connection, step_name="")

    result = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')

    # one record has a value beyond the RESPNSE range
    assert len(result) == (len(test_data)-1)
    assert result.columns.isin(gxs.COLUMNS_TO_MOVE).sum() == len(gxs.COLUMNS_TO_MOVE)

    test_result = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample_result.pkl')
    assert_frame_equal(result, test_result)


def test_move_survey_subsample_to_sas_table_traffic_weight(database_connection):
    test_data = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample.pkl')

    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)
    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    gxs.move_survey_subsample_to_sas_table('move-survey-test', database_connection, step_name="TRAFFIC_WEIGHT")

    result = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')

    # two records have a value beyond the RESPNSE range
    assert len(result) == (len(test_data)-2)
    assert result.columns.isin(gxs.COLUMNS_TO_MOVE).sum() == len(gxs.COLUMNS_TO_MOVE)

    test_result = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample_traffic_result.pkl')
    assert_frame_equal(result, test_result)


def test_populate_survey_data_for_step(database_connection):
    # this is an integration of the above tests so we will keep things simple

    test_data = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample.pkl')
    cf.insert_dataframe_into_table(gxs.SURVEY_SUBSAMPLE_TABLE, test_data)

    step_config = {'nullify_pvs': ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]"],
                   'name': 'SHIFT_WEIGHT',
                   'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"]}
    gxs.populate_survey_data_for_step('move-survey-test', database_connection, step_config)

    cf.delete_from_table(gxs.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', 'move-survey-test')

    result = cf.get_table_values(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)
    test_result = pd.read_pickle('tests/data/generic_xml_steps/move_survey_subsample_result.pkl')
    assert_frame_equal(result, test_result)

    cf.delete_from_table(gxs.SAS_SURVEY_SUBSAMPLE_TABLE)

    result = cf.get_table_values("SAS_SHIFT_WT")
    assert len(result) == 0

    result = cf.get_table_values("SAS_PS_SHIFT_DATA")
    assert len(result) == 0


def test_populate_step_data(database_connection):
    step_config = {"table_name": "[dbo].[SHIFT_DATA]",
                   "data_table": "[dbo].[SAS_SHIFT_DATA]",
                   "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
                                          "[AM_PM_NIGHT]"],
                   }

    # setup test data/tables
    test_data = pd.read_pickle('tests/data/generic_xml_steps/populate_step_data.pkl')
    cf.insert_dataframe_into_table(step_config["table_name"], test_data)
    cf.delete_from_table(step_config['data_table'])
    run_id = 'populate-step-data'

    gxs.populate_step_data(run_id, database_connection, step_config)

    result = cf.get_table_values(step_config['data_table'])

    # clean test data before actually testing results
    cf.delete_from_table(step_config['table_name'], 'RUN_ID', '=', run_id)
    cf.delete_from_table(step_config['data_table'])

    test_result = pd.read_pickle('tests/data/generic_xml_steps/populate_step_data_result.pkl')
    assert_frame_equal(result, test_result)


@pytest.mark.skip('this takes very long')
def test_shift_weight_step(database_connection):
    step_config = {"nullify_pvs": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]", "[SHIFT_FLAG_PV]", "[CROSSINGS_FLAG_PV]", "[SHIFT_WT]"],
                   'name': 'SHIFT_WEIGHT',
                   'delete_tables': ["[dbo].[SAS_SHIFT_WT]", "[dbo].[SAS_PS_SHIFT_DATA]"],
                   "table_name": "[dbo].[SHIFT_DATA]",
                   "data_table": "[dbo].[SAS_SHIFT_DATA]",
                   "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
                                          "[AM_PM_NIGHT]"],
                   "spv_table": "[dbo].[SAS_SHIFT_SPV]",
                   "pv_columns": ["'SHIFT_PORT_GRP_PV'", "'WEEKDAY_END_PV'", "'AM_PM_NIGHT_PV'"],
                   "pv_columns2": ["[SHIFT_PORT_GRP_PV]", "[WEEKDAY_END_PV]", "[AM_PM_NIGHT_PV]"],
                   "order": 0,
                   "pv_table": "[dbo].[SAS_SHIFT_PV]",
                   "weight_table": "[dbo].[SAS_SHIFT_WT]",
                   "sas_ps_table": "[dbo].[SAS_PS_SHIFT_DATA]",
                   "results_columns": ["[SHIFT_WT]"],
                   "ps_table": "[dbo].[PS_SHIFT_DATA]",
                   "ps_columns": ["[RUN_ID]", "[SHIFT_PORT_GRP_PV]", "[ARRIVEDEPART]", "[WEEKDAY_END_PV]",
                                  "[AM_PM_NIGHT_PV]", "[MIGSI]", "[POSS_SHIFT_CROSS]", "[SAMP_SHIFT_CROSS]",
                                  "[MIN_SH_WT]", "[MEAN_SH_WT]", "[MAX_SH_WT]", "[COUNT_RESPS]", "[SUM_SH_WT]"],

                   }

    shift_weight_step('9e5c1872-3f8e-4ae5-85dc-c67a602d011e', database_connection, step_config)

