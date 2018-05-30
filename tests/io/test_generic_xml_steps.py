from main.io import import_data
from main.io import import_traffic_data
import main.io.CommonFunctions as cf
from main.io.CommonFunctions import get_oracle_connection
import main.io.generic_xml_steps as gxs
import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal

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