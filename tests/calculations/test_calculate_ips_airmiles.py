'''
Created on 17 Sep 2019

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from main.calculations.calculate_ips_airmiles import do_ips_airmiles_calculation
import utils.common_functions as cf

# define the table names
OUT_TABLE_NAME = "SAS_AIR_MILES"
SAS_SURVEY_SUBSAMPLE_TABLE = "[dbo].[SAS_SURVEY_SUBSAMPLE]"

def clear_tables():

    # clear the input SQL server tables for the step
    cf.delete_from_table(SAS_SURVEY_SUBSAMPLE_TABLE)

    # clear the output table
    cf.delete_from_table(OUT_TABLE_NAME)


def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)


@pytest.mark.parametrize('path_to_data', [
    r'tests\data\calculations\december_2017\air_miles',
    r'tests\data\calculations\november_2017\air_miles',
    r'tests\data\calculations\october_2017\air_miles',
    ])
def test_calculate(path_to_data):

    # clear the input and output tables
    clear_tables()

    # read in data from csv
    df_airmiles = pd.read_csv(path_to_data + '/airmiles.csv', engine='python')

    # put data into SQL server and read back
    df_airmiles_import = convert_dataframe_to_sql_format(SAS_SURVEY_SUBSAMPLE_TABLE, df_airmiles)

    # Run the calculation
    output_data = do_ips_airmiles_calculation(df_surveydata=df_airmiles_import, var_serial='SERIAL')

    # Retrieve and sort python calculated dataframes
    py_out_data = output_data
    py_out_data = py_out_data.sort_values(by='SERIAL')
    py_out_data.index = range(0, len(py_out_data))

    # insert the csv output data into SQL and read back, this is for testing against data pulled from SQL Server
    test_result = pd.read_csv(path_to_data + '/airmiles_output.csv', engine='python')
    test_result_sql = convert_dataframe_to_sql_format(OUT_TABLE_NAME, test_result)
    test_result_survey_sql = test_result_sql.sort_values(by='SERIAL')
    test_result_survey_sql.index = range(0, len(test_result_survey_sql))

    # clear the input and output tables
    clear_tables()

    # Check the results match
    assert_frame_equal(py_out_data, test_result_survey_sql,  check_dtype=False, check_like=True)
