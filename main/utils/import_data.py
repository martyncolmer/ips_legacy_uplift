import pandas as pd
from sas7bdat import SAS7BDAT
from main.io.CommonFunctions import get_oracle_connection, insert_dataframe_into_table


def extract_data(df):
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

    df.columns = df.columns.str.upper()

    df_new = df.sort_values(by='SERIAL')

    df_new = df_new.filter(columns, axis=1)

    return df_new

def import_survey_data(survey_data_path, version_id):

    if survey_data_path[-3:] == "csv":
        df = pd.read_csv(survey_data_path)
    elif survey_data_path[-3:] == 'pkl':
        df = pd.read_pickle(survey_data_path)
    else:
        try:
            df = pd.read_sas(survey_data_path)
            print("pdimport")
        except:
            df = SAS7BDAT(survey_data_path).to_data_frame()
            print("sasimport")

    df_survey_data = extract_data(df)
    df_survey_data['RUN_ID'] = pd.Series('TEST-RUN-ID', index=df_survey_data.index)
    insert_dataframe_into_table('SURVEY_SUBSAMPLE', df_survey_data)





if __name__ == '__main__':
    survey_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\ips1712bv4_amtspnd.sas7bdat"
    version_id = 1891

    import_survey_data(survey_data_path, version_id)
