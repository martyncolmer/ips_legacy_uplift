'''
Created on 12 March 2018

@author: Nassir Mohammad
'''

import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.utils import import_data
from main.io import CommonFunctions as cf


def test_import_survey_data():
    survey_data_path = r"../data/import/output/post_import_SURVEY_SUBSAMPLE.csv"
    #df_expected_result = pd.read_pickle(survey_data_path)
    df_expected_result = pd.read_csv(survey_data_path)

    #populate_survey_subsample.extract_survey_data(r'../data/import/input/input_data.pkl',1891)

    # Connection to the database
    conn = cf.get_oracle_connection()
    sql = "SELECT * from SURVEY_SUBSAMPLE WHERE RUN_ID = 'TEST-RUN-ID'"
    df_output = pd.read_sql(sql, conn)

    # Remove the RUN_ID column as they are unique per run.
    df_output = df_output.drop('RUN_ID',axis =1)
    df_expected_result = df_expected_result.drop('RUN_ID', axis =1)

    # Sort data by serial number
    df_output = df_output.sort_values(by='SERIAL')
    df_expected_result = df_expected_result.sort_values(by='SERIAL')

    # Reset index the data so indexes line up
    df_output.index = range(0, len(df_output))
    df_expected_result.index = range(0, len(df_expected_result))

    # Replace any nan values with None types
    df_output = df_output.where((pd.notnull(df_output)), None)
    df_expected_result = df_expected_result.where((pd.notnull(df_expected_result)), None)

    # for x in df_output.columns:
    #     df_expected_result[x] = df_expected_result[x].astype(df_output[x].dtypes.name)



    assert_frame_equal(df_output, df_expected_result,check_dtype=False,check_like=True)
    print("DONE")
