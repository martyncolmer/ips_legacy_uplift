import pandas as pd
from pandas.util.testing import assert_frame_equal
from main.utils import import_data
from main.io import CommonFunctions as cf


def test_import_survey_data():
    # Read in the expected result
    result_data_path = r"../data/import/output/post_import_SURVEY_SUBSAMPLE.csv"
    df_result = pd.read_csv(result_data_path)

    # Read in the input data
    test_data_path = r"../data/import/input/input_data.pkl"

    # Execute the import script
    import_data.import_survey_data(test_data_path, 'TEST-RUN-ID', 1234)

    # Connect to the database and pull the generated data from oracle (it is added to the database during import)
    conn = cf.get_oracle_connection()
    cur = conn.cursor()
    df_test_result = cur.execute("SELECT distinct * from SURVEY_SUBSAMPLE where RUN_ID = 'TEST-RUN-ID'")

    # Output the data to csv and re-import to adopt the formatting of the target data
    df_test_result.to_csv("test_run.csv", index=False)
    df_test = pd.read_csv("test_run.csv")

    # Check only matching columns (this should not effect the tables anymore)
    test_columns = list(df_test.columns.values)
    df_expected_result = df_result[test_columns].copy()

    # Remove the RUN_ID column as they are unique per run.
    df_test = df_test.drop('RUN_ID', axis=1)
    df_expected_result = df_expected_result.drop('RUN_ID', axis =1)

    # Sort data by serial number
    df_test = df_test.sort_values(by='SERIAL')
    df_expected_result = df_expected_result.sort_values(by='SERIAL')

    # Reset index the data so indexes line up
    df_test.index = range(0, len(df_test))
    df_expected_result.index = range(0, len(df_expected_result))

    # Check if the test and result dataframes match
    assert_frame_equal(df_test, df_expected_result, check_dtype=False, check_like=True)
    print("DONE")


if __name__ == '__main__':
    test_import_survey_data()