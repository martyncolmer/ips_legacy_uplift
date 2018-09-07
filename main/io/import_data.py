import pandas as pd
from sas7bdat import SAS7BDAT
from main.io.CommonFunctions import insert_dataframe_into_table, get_sql_connection


def extract_data(df):
    """
    Author       : Thomas Mahoney
    Date         : 26 / 04 / 2018
    Purpose      : Extracts the required columns from the import data.
    Parameters   : df - the dataframe containing all of the import data.
    Returns      : The extracted dataframe.
    Requirements : NA
    Dependencies : NA
    """

    # Generate a list of the required columns
    columns = ['SERIAL', 'AM_PM_NIGHT', 'AGE', 'ANYUNDER16', 'APORTLATDEG',
               'APORTLATMIN', 'APORTLATSEC', 'APORTLATNS', 'APORTLONDEG',
               'APORTLONMIN', 'APORTLONDSEC', 'APORTLONEW', 'ARRIVEDEPART',
               'BABYFARE', 'BEFAF','CHILDFARE', 'CHANGECODE', 'COUNTRYVISIT',
               'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC', 'CPORTLATNS',
               'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONDSEC', 'CPORTLONEW',
               'INTDATE', 'DAYTYPE', 'DIRECTLEG', 'DVEXPEND', 'DVFARE',
               'DVLINECODE', 'DVPACKAGE', 'DVPACKCOST', 'DVPERSONS', 'DVPORTCODE',
               'EXPENDCODE', 'EXPENDITURE', 'FARE','FAREK', 'FLOW', 'HAULKEY',
               'INTENDLOS', 'INTMONTH', 'KIDAGE', 'LOSKEY', 'MAINCONTRA', 'MIGSI',
               'NATIONALITY', 'NATIONNAME', 'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4',
               'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8', 'NUMADULTS', 'NUMDAYS',
               'NUMNIGHTS', 'NUMPEOPLE', 'PACKAGEHOL', 'PACKAGEHOLUK', 'PERSONS',
               'PORTROUTE', 'PACKAGE', 'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC',
               'PROUTELATNS', 'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC',
               'PROUTELONEW', 'PURPOSE', 'QUARTER', 'RESIDENCE', 'RESPNSE',
               'SEX', 'SHIFTNO','SHUTTLE', 'SINGLERETURN', 'TANDTSI', 'TICKETCOST',
               'TOWNCODE1', 'TOWNCODE2', 'TOWNCODE3', 'TOWNCODE4', 'TOWNCODE5',
               'TOWNCODE6', 'TOWNCODE7', 'TOWNCODE8', 'TRANSFER', 'UKFOREIGN',
               'VEHICLE', 'VISITBEGAN', 'WELSHNIGHTS', 'WELSHTOWN', 'FAREKEY',
               'TYPEINTERVIEW']

    # Set the imported columns to be uppercase
    df.columns = df.columns.str.upper()

    # Sort the data by serial number
    df_new = df.sort_values(by='SERIAL')

    # Recreate the dataframe using only the specified columns
    df_new = df_new.filter(columns, axis=1)

    return df_new


def import_survey_data(survey_data_path, run_id):
    """
    Author       : Thomas Mahoney
    Date         : 26 / 04 / 2018
    Purpose      : Loads the import data into a dataframe then appends the data to the 'SURVEY_SUBSAMPLE'
                   table on the connected database.
    Parameters   : survey_data_path - the dataframe containing all of the import data.
                   run_id - the generated run_id for the current run.
                   version_id - ID indicating the current version
    Returns      : NA
    Requirements : Datafile is of type '.csv', '.pkl' or '.sas7bdat'
    Dependencies : NA
    """

    # Check the survey_data_path's suffix to see what it matches then extract using the appropriate method.
    if survey_data_path[-3:] == "csv":
        df = pd.read_csv(survey_data_path, engine='python')

    # Call the extract data function to select only the needed columns.
    df_survey_data = extract_data(df)

    # Add the generated run id to the dataset.
    df_survey_data['RUN_ID'] = pd.Series(run_id, index=df_survey_data.index)

    # Insert the imported data into the survey_subsample table on the database.
    insert_dataframe_into_table('SURVEY_SUBSAMPLE', df_survey_data)


if __name__ == '__main__':
    survey_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\ips1712bv4_amtspnd.sas7bdat"
    version_id = 1891

    import_survey_data(survey_data_path, version_id)
