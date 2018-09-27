import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.calculations.calculate_ips_minimums_weight import do_ips_minweight_calculation
import main.io.CommonFunctions as cf
import pytest
import tests.config

path_to_data = r"tests/data/calculations/" + tests.config.TEST_MONTH + "/min_weight"

OUTPUT_TABLE_NAME = 'SAS_MINIMUMS_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_MINIMUMS'

# SAS data for november and october are incorrect.
# November lacks the NR weight calculation required to run this step.
# October is missing the final summary output of the step stopping us
# from comparing the summaries produced.

@pytest.mark.parametrize('data_path', [
    r'tests\data\calculations\december_2017\min_weight',
    #r'tests\data\calculations\november_2017\min_weight',
    #r'tests\data\calculations\october_2017\min_weight',
    ])
def test_calculate(data_path):
    """
    Author        : Thomas Mahoney
    Date          : 11 Sep 2018
    Purpose       : Tests the calculation function of the minimums weight step works as expected.
    Parameters    : data_path - The file path to the data folder (contains import and expected results csv files).
    Returns       : NA
    """

    # Clear the survey import table
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Read the test input data in and write it to the import table
    path_to_surveydata = data_path + r"\surveydata.csv"
    df_surveydata = pd.read_csv(path_to_surveydata, engine='python')
    cf.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', df_surveydata)

    # Read the data from SQL (as it will in the production ready system)
    df_surveydata = cf.get_table_values('SAS_SURVEY_SUBSAMPLE')

    # Run the calculation step
    output_data, summary_data = do_ips_minweight_calculation(df_surveydata=df_surveydata,
                                                             var_serialNum='SERIAL',
                                                             var_shiftWeight='SHIFT_WT',
                                                             var_NRWeight='NON_RESPONSE_WT',
                                                             var_minWeight='MINS_WT')

    def convert_dataframe_to_sql_format(table_name, dataframe):
        cf.insert_dataframe_into_table(table_name, dataframe)
        return cf.get_table_values(table_name)

    # Write the test result data to SQL then pull it back for comparison
    df_survey_result = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, output_data)
    df_summary_result = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, summary_data)

    # Clear down the result tables
    cf.delete_from_table(OUTPUT_TABLE_NAME)
    cf.delete_from_table(SUMMARY_TABLE_NAME)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\outputdata_final.csv"
    df_survey_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_survey_expected = convert_dataframe_to_sql_format(OUTPUT_TABLE_NAME, df_survey_expected)

    # Sort the dataframes for comparison
    df_survey_result = df_survey_result.sort_values('SERIAL')
    df_survey_result.index = range(0, len(df_survey_result))
    df_survey_expected = df_survey_expected.sort_values('SERIAL')
    df_survey_expected.index = range(0, len(df_survey_expected))

    assert_frame_equal(df_survey_result, df_survey_expected)

    # Write the expected result data to SQL then pull it back for comparison
    path_to_survey_result = data_path + r"\summarydata_final.csv"
    df_summary_expected = pd.read_csv(path_to_survey_result, engine='python')
    df_summary_expected = convert_dataframe_to_sql_format(SUMMARY_TABLE_NAME, df_summary_expected)

    # Sort the dataframes for comparison
    df_summary_result = df_summary_result.sort_values(['MINS_PORT_GRP_PV','ARRIVEDEPART','MINS_CTRY_GRP_PV'])
    df_summary_result.index = range(0, len(df_summary_result))
    df_summary_expected = df_summary_expected.sort_values(['MINS_PORT_GRP_PV','ARRIVEDEPART','MINS_CTRY_GRP_PV'])
    df_summary_expected.index = range(0, len(df_summary_expected))

    assert_frame_equal(df_summary_result, df_summary_expected)

