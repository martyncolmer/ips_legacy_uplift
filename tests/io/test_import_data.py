from main.io import import_data
import pytest
import json
import pandas as pd
from main.io import CommonFunctions as cf
from main.io import import_traffic_data
from main.io import ips_data_management as idm

with open('data/xml_steps_configuration.json') as config_file:
    STEP_CONFIGURATION = json.load(config_file)

RUN_ID = 'import_test'
CLEAN_UP_BEFORE = True
CLEAN_UP_AFTER = False

@pytest.fixture(scope='module')
def database_connection():
    '''
    This fixture provides the database connection. It is added to the function argument of each test
    and picked up by the test from there. The fixture allows us to re-use the same database connection
    over and over again.
    '''
    return cf.get_sql_connection()


def setup_module(module):
    """ setup any state specific to the execution of the given module."""
    print("Setup")
    if CLEAN_UP_BEFORE:
        reset_tables()


def teardown_module(module):
    """ Run any cleanup functionality after the import tests have run."""
    print("Teardown")
    if CLEAN_UP_AFTER:
        reset_tables()


def reset_tables():
    """ Deletes records from tables associated with the import test. """

    print("Deleting records from tables associated with the import test...")
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID)
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID + "_OCTOBER_2017")
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID + "_NOVEMBER_2017")
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID + "_DECEMBER_2017")
    # cf.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', RUN_ID + "_Q3_2017")

    tables_to_delete = [idm.SURVEY_SUBSAMPLE_TABLE,
                        "TRAFFIC_DATA",
                        "SHIFT_DATA",
                        "NON_RESPONSE_DATA",
                        "UNSAMPLED_OOH_DATA"]

    for table in tables_to_delete:
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID)
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_OCTOBER_2017")
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_NOVEMBER_2017")
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_DECEMBER_2017")
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_Q3_2017")

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")

    print("Import table test records deleted.")



@pytest.mark.parametrize('dataset, data_path, table_name, step', [
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\shift_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'shift_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\shift_weight\shiftsdata.csv', 'SAS_SHIFT_DATA', 'shift_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\non_response_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'nr_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\non_response_weight\nonresponsedata.csv', 'SAS_NON_RESPONSE_DATA', 'nr_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\min_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'min_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\traffic_weight\survey_input.csv', 'SAS_SURVEY_SUBSAMPLE', 'traffic_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\traffic_weight\trtotals.csv', 'SAS_TRAFFIC_DATA', 'traffic_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\unsampled_weight\survey_input.csv', 'SAS_SURVEY_SUBSAMPLE', 'unsampled_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\unsampled_weight\ustotals.csv', 'SAS_UNSAMPLED_OOH_DATA', 'unsampled_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\imbalance_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'imbalance_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\final_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'final_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\stay\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'stay_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\fares\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'fares_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\spend\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'spend_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\rail\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'rail_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\regional_weights\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'regional_weights'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\town_and_stay\input_townspend.csv', 'SAS_SURVEY_SUBSAMPLE', 'town_stay_and_exp_imputation'),
    ])
def test_insert_step_data(dataset, data_path, table_name, step):

    # Get CSV content
    df = pd.read_csv(data_path, engine='python')
    
    if 'REC_ID' in df.columns:
        df.drop(['REC_ID'], axis=1, inplace=True)

    cf.insert_dataframe_into_table(table_name, df)

    print('imported.')



@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\survey\ips1710b_1.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\survey\ips1711h_1.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\survey\ips1712bv4_amtspnd.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\survey\quarter32017.csv'),
    ])
def test_import_survey_data(dataset, data_path):

    import_data.import_survey_data(data_path, RUN_ID + dataset)

    assert True

#
# def test_import_survey_data_november_2017():
#     survey_data_path = r'tests\data\ips_data_management\import_data\survey\ips1711h_1.csv'
#
#     import_data.import_survey_data(survey_data_path, RUN_ID + "_NOVEMBER_2017")
#
#     assert True
#
#
# def test_import_survey_data_december_2017():
#     survey_data_path = r'tests\data\ips_data_management\import_data\survey\ips1712bv4_amtspnd.csv'
#
#     import_data.import_survey_data(survey_data_path, RUN_ID + "_DECEMBER_2017")
#
#     assert True
#
#
# def test_import_survey_data_q3_2017():
#     survey_data_path = r'tests\data\ips_data_management\import_data\survey\quarter32017.csv'
#
#     import_data.import_survey_data(survey_data_path, RUN_ID + "_Q3_2017")
#
#     assert True


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Poss shifts Oct 2017.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Poss shifts Nov 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Poss shifts Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Possible shifts Q3 2017.csv'),
    ])
def test_import_shift_data(dataset, data_path):

    import_traffic_data.import_traffic_data( RUN_ID + dataset, data_path)

    assert True
    pass


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Oct_NRMFS.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Nov_NRMFS.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Dec17_NR.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Non Response Q3 2017.csv'),
    ])
def test_import_non_response_data(dataset, data_path):

    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    assert True


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Sea Traffic Oct 2017.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Sea Traffic Nov 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Sea Traffic Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Sea Traffic Q3 2017.csv'),
    ])
def test_import_sea_data(dataset, data_path):

    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    assert True


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Tunnel Traffic Oct 2017.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Tunnel Traffic NovQ3 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Tunnel Traffic Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Tunnel Traffic Q3 2017.csv'),
    ])
def test_import_tunnel_data(dataset, data_path):

    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    assert True


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Air Sheet Oct 2017 VBA2nd.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Air Sheet Nov 2017 VBA.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Air Sheet Dec 2017 VBA.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\CAA Q3 2017.csv'),
    ])
def test_import_air_data(dataset, data_path):

    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    assert True


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Unsampled Traffic Oct 2017'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Unsampled Traffic Nov 2017'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Unsampled Traffic Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Unsampled Traffic Q3 2017'),
    ])
def test_import_unsampled_data(dataset, data_path):

    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    assert True


@pytest.mark.skip
def test_import_data_into_database():
    '''
    This function prepares all the data necessary to run all 14 steps.
    The input files have been edited to make sure data types match the database tables.
    Note that no process variables are uploaded and are expected to be present in the database.
    '''

    cf.delete_from_table('SURVEY_SUBSAMPLE', 'RUN_ID', '=', RUN_ID)
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')

    # Import data paths (these will be passed in through the user)
    survey_data_path = r'tests\data\ips_data_management\import_data\minimums_weight\surveydata.csv'
    shift_data_path = r'tests\data\ips_data_management\import_data\external\Poss shifts Dec 2017.csv'
    nr_data_path = r'tests\data\ips_data_management\import_data\external\Dec17_NR.csv'
    sea_data_path = r'tests\data\ips_data_management\import_data\external\Sea Traffic Dec 2017.csv'
    tunnel_data_path = r'tests\data\ips_data_management\import_data\external\Tunnel Traffic Dec 2017.csv'
    air_data_path = r'tests\data\ips_data_management\import_data\external\Air Sheet Dec 2017 VBA.csv'
    unsampled_data_path = r'tests\data\ips_data_management\import_data\external\Unsampled Traffic Dec 2017.csv'

    # Get CSV content
    df = pd.read_csv(survey_data_path)

    # Add the generated run id to the dataset.
    df['RUN_ID'] = pd.Series(RUN_ID, index=df.index)

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table('SURVEY_SUBSAMPLE', df)

    # Import the external files into the database.
    import_traffic_data.import_traffic_data(RUN_ID, shift_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, nr_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, sea_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, tunnel_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, air_data_path)
    import_traffic_data.import_traffic_data(RUN_ID, unsampled_data_path)

@pytest.mark.skip
def test_import_survey_data_old():
    # Import data paths (these will be passed in through the user)
    survey_data_path = r'tests\data\ips_data_management\import_data\minimums_weight\surveydata.csv'
    shift_data_path = r'tests\data\ips_data_management\import_data\external\Poss shifts Dec 2017.csv'
    nr_data_path = r'tests\data\ips_data_management\import_data\external\Dec17_NR.csv'
    sea_data_path = r'tests\data\ips_data_management\import_data\external\Sea Traffic Dec 2017.csv'
    tunnel_data_path = r'tests\data\ips_data_management\import_data\external\Tunnel Traffic Dec 2017.csv'
    air_data_path = r'tests\data\ips_data_management\import_data\external\Air Sheet Dec 2017 VBA.csv'
    unsampled_data_path = r'tests\data\ips_data_management\import_data\external\Unsampled Traffic Dec 2017.csv'

    # Delete table content
    cf.delete_from_table('SURVEY_SUBSAMPLE', 'RUN_ID', '=', RUN_ID)
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')

    df = pd.read_csv(survey_data_path)

    # Add the generated run id to the dataset.
    df['RUN_ID'] = pd.Series(RUN_ID, index=df.index)

    # Insert the imported data into the survey_subsample table on the database.
    cf.insert_dataframe_into_table('SURVEY_SUBSAMPLE', df)

    # Import the external files into the database.
    cf.import_traffic_data.import_traffic_data(RUN_ID, shift_data_path)
    cf.import_traffic_data.import_traffic_data(RUN_ID, nr_data_path)
    cf.import_traffic_data.import_traffic_data(RUN_ID, sea_data_path)
    cf.import_traffic_data.import_traffic_data(RUN_ID, tunnel_data_path)
    cf.import_traffic_data.import_traffic_data(RUN_ID, air_data_path)
    cf.import_traffic_data.import_traffic_data(RUN_ID, unsampled_data_path)
