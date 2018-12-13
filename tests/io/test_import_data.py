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
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Deletes records from tables associated with the import test.
    Parameters    : NA
    Returns       : NA
    """

    """ Deletes records from tables associated with the import test. """

    print("Deleting records from tables associated with the import test...")

    tables_to_delete_run_id = [idm.SURVEY_SUBSAMPLE_TABLE,
                        "TRAFFIC_DATA",
                        "SHIFT_DATA",
                        "NON_RESPONSE_DATA",
                        "UNSAMPLED_OOH_DATA"]

    for table in tables_to_delete_run_id:
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID)
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_OCTOBER_2017")
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_NOVEMBER_2017")
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_DECEMBER_2017")
        cf.delete_from_table(table, 'RUN_ID', '=', RUN_ID + "_Q3_2017")

    tables_to_delete_all = [
        'SAS_SURVEY_SUBSAMPLE',
        'SAS_SHIFT_DATA',
        'SAS_NON_RESPONSE_DATA',
        'SAS_TRAFFIC_DATA',
        'SAS_UNSAMPLED_OOH_DATA',
    ]

    for table in tables_to_delete_all:
        cf.delete_from_table(table)

    print("Import table test records deleted.")


@pytest.mark.parametrize('dataset, data_path, table_name, step', [
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\shift_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'shift_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\shift_weight\shiftsdata.csv', 'SAS_SHIFT_DATA', 'shift_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\non_response_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'nr_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\non_response_weight\nonresponsedata.csv', 'SAS_NON_RESPONSE_DATA', 'nr_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\min_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'min_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\traffic_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'traffic_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\traffic_weight\trafficdata.csv', 'SAS_TRAFFIC_DATA', 'traffic_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\unsampled_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'unsampled_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\unsampled_weight\unsampleddata.csv', 'SAS_UNSAMPLED_OOH_DATA', 'unsampled_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\imbalance_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'imbalance_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\final_weight\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'final_weight'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\stay\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'stay_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\fares\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'fares_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\spend\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'spend_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\rail\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'rail_imputation'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\regional_weights\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'regional_weights'),
    ('_DECEMBER_2017', r'tests\data\calculations\december_2017\town_and_stay\surveydata.csv', 'SAS_SURVEY_SUBSAMPLE', 'town_stay_and_exp_imputation'),
    ])
def test_insert_step_data(dataset, data_path, table_name, step):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests unit test data can be loaded into correct tables for processing.
    Parameters    : dataset - Used as a suffix in cases where a RUN_ID is needed.
                    data_path - The import file path.
                    table_name - The name of the import's target table
                    step = Used as part of a suffix where a RUN_ID is needed.
    Returns       : NA
    """

    # Clear the tables before importing the test data
    reset_tables()

    # Generate a dataframe from the csv file
    df = pd.read_csv(data_path, engine='python')

    # If the data contains a REC_ID column, drop it as the value is generated once the record is added to the SQL table.
    if 'REC_ID' in df.columns:
        df.drop(['REC_ID'], axis=1, inplace=True)

    # Insert the dataframe into the target SQL Server table
    cf.insert_dataframe_into_table(table_name, df)

    # Assert that the number of records in the table matches the number of records in our import dataset.
    assert len(df.index) == len(cf.get_table_values(table_name))


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\survey\ips1710b_1.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\survey\ips1711h_1.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\survey\ips1712bv4_amtspnd.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\survey\quarter32017.csv'),
    ])
def test_import_survey_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests running the import_survey_data function correctly inserts a dataset into SQL Server from
                    a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_data.import_survey_data(data_path, RUN_ID + dataset)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'SURVEY_SUBSAMPLE', 'RUN_ID', RUN_ID + dataset)) > 0


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Poss shifts Oct 2017.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Poss shifts Nov 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Poss shifts Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Possible shifts Q3 2017.csv'),
    ])
def test_import_shift_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests that running the import_traffic_data function with shift data correctly inserts a dataset into
                    SQL Server from a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'SHIFT_DATA', 'RUN_ID', RUN_ID + dataset)) > 0


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Oct_NRMFS.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Nov_NRMFS.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Dec17_NR.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Non Response Q3 2017.csv'),
    ])
def test_import_non_response_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests that running the import_traffic_data function with non response data correctly inserts a
                    dataset into SQL Server from a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'NON_RESPONSE_DATA', 'RUN_ID', RUN_ID + dataset)) > 0


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Sea Traffic Oct 2017.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Sea Traffic Nov 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Sea Traffic Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Sea Traffic Q3 2017.csv'),
    ])
def test_import_sea_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests that running the import_traffic_data function with sea data correctly inserts a dataset into
                    SQL Server from a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'TRAFFIC_DATA', 'RUN_ID', RUN_ID + dataset)) > 0


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Tunnel Traffic Oct 2017.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Tunnel Traffic Nov 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Tunnel Traffic Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Tunnel Traffic Q3 2017.csv'),
    ])
def test_import_tunnel_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests that running the import_traffic_data function with tunnel data correctly inserts a dataset
                    into SQL Server from a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'TRAFFIC_DATA', 'RUN_ID', RUN_ID + dataset)) > 0


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Air Sheet Oct 2017 VBA2nd.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Air Sheet Nov 2017 VBA.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Air Sheet Dec 2017 VBA.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\CAA Q3 2017.csv'),
    ])
def test_import_air_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests that running the import_traffic_data function with air data correctly inserts a dataset into
                    SQL Server from a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'TRAFFIC_DATA', 'RUN_ID', RUN_ID + dataset)) > 0


@pytest.mark.parametrize('dataset, data_path', [
    ('_OCTOBER_2017', r'tests\data\ips_data_management\import_data\external\october\Unsampled Traffic Oct 20172nd.csv'),
    ('_NOVEMBER_2017', r'tests\data\ips_data_management\import_data\external\november\Unsampled Traffic Nov 2017.csv'),
    ('_DECEMBER_2017', r'tests\data\ips_data_management\import_data\external\december\Unsampled Traffic Dec 2017.csv'),
    ('_Q3_2017', r'tests\data\ips_data_management\import_data\external\q3\Unsampled Traffic Q3 2017.csv'),
    ])
def test_import_unsampled_data(dataset, data_path):
    """
    Author        : Thomas Mahoney
    Date          : 7 Sep 2018
    Purpose       : Tests that running the import_traffic_data function with unsampled data correctly inserts a dataset
                    into SQL Server from a csv source file.
    Parameters    : dataset - This value specifies the period the data was collected in, it is used in RUN_ID creation.
                    data_path - The import file path.
    Returns       : NA
    """

    # Run the import script using the specified data and the test RUN_ID
    import_traffic_data.import_traffic_data(RUN_ID + dataset, data_path)

    # Assert that records were added to the SURVEY_SUBSAMPLE table under the given RUN_ID.
    assert len(cf.select_data('*', 'UNSAMPLED_OOH_DATA', 'RUN_ID', RUN_ID + dataset)) > 0
