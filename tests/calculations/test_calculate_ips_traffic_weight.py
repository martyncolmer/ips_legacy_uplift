'''
Created on 6 Nov 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_traffic_weight import do_ips_trafweight_calculation_with_R
import utils.common_functions as cf
import pytest
import main.io.data_management as idm
import main.calculations.calculate_ips_traffic_weight as tr_calc


def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)


def clear_tables():

    # clear the input SQL server tables for the step
    cf.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)
    cf.delete_from_table(tr_calc.POP_TOTALS)

    # clear the output tables and summary tables
    cf.delete_from_table(tr_calc.OUTPUT_TABLE_NAME)
    cf.delete_from_table(tr_calc.SUMMARY_TABLE_NAME)

    # clear the auxillary tables
    cf.delete_from_table(tr_calc.SURVEY_TRAFFIC_AUX_TABLE)

    # drop aux tables and r created tables
    cf.drop_table(tr_calc.POP_PROWVEC_TABLE)
    cf.drop_table(tr_calc.R_TRAFFIC_TABLE)


@pytest.mark.parametrize('path_to_data', [
    r'tests\data\calculations\december_2017\traffic_weight',
    r'tests\data\calculations\november_2017\traffic_weight',
    r'tests\data\calculations\october_2017\traffic_weight',
    ])
def test_calculate(path_to_data):

    # clear the input and output tables
    clear_tables()

    # read in data from csv
    df_surveydata = pd.read_csv(path_to_data + '/surveydata.csv', engine='python')
    df_tr_data = pd.read_csv(path_to_data + '/trafficdata.csv', engine='python')

    # ----------------------------------------------
    # put dataframes into SQL server and read back
    # ----------------------------------------------

    # If the data contains a REC_ID column, drop it as the value is generated once the record is added to the SQL table.
    if 'REC_ID' in df_tr_data.columns:
        df_tr_data.drop(['REC_ID'], axis=1, inplace=True)

    df_surveydata_import = convert_dataframe_to_sql_format(idm.SAS_SURVEY_SUBSAMPLE_TABLE, df_surveydata)
    df_tr_data_import = convert_dataframe_to_sql_format(tr_calc.POP_TOTALS, df_tr_data)

    # do the calculation
    df_output_merge_final, df_output_summary = do_ips_trafweight_calculation_with_R(df_surveydata_import, df_tr_data_import)

    # test start - turn on when testing/refactoring intermediate steps
    df_test = pd.read_csv(path_to_data + '/output_final.csv', engine='python')
    df_test.columns = df_test.columns.str.upper()
    assert_frame_equal(df_output_merge_final, df_test, check_dtype=False, check_less_precise=True)

    df_test2 = pd.read_csv(path_to_data + '/summary_final.csv', engine='python')
    df_test2.columns = df_test2.columns.str.upper()
    assert_frame_equal(df_output_summary, df_test2, check_dtype=False, check_less_precise=True)