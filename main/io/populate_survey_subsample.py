import sys
import pandas as pd
from sas7bdat import SAS7BDAT
from main.io.CommonFunctions import get_oracle_connection, insert_list_into_table


def get_column_and_version_data(cur, version):
    
    sql = """SELECT 
                SC.COLUMN_NO, 
                SC.VERSION_ID, 
                SC.COLUMN_DESC, 
                SV.SERIAL_NO
            FROM
                SURVEY_COLUMN SC, SURVEY_VALUE SV
            WHERE
                SV.VERSION_ID = SC.VERSION_ID
            AND
                SV.VERSION_ID = """+version+""" 
            AND
                SV.COLUMN_NO = SC.COLUMN_NO
            AND
                UPPER(SC.COLUMN_DESC) != 'SERIAL'
            AND
                UPPER(SC.COLUMN_DESC) IN
                    ('AM_PM_NIGHT','AGE', 'ANYUNDER16','APORTLATDEG',
                    'APORTLATMIN','APORTLATSEC','APORTLATNS','APORTLONDEG',
                    'APORTLONMIN','APORTLONDSEC','APORTLONEW','ARRIVEDEPART',
                    'BABYFARE','BEFAF','CHILDFARE','CHANGECODE','COUNTRYVISIT',
                    'CPORTLATDEG','CPORTLATMIN','CPORTLATSEC','CPORTLATNS',
                    'CPORTLONDEG','CPORTLONMIN','CPORTLONDSEC','CPORTLONEW',
                    'INTDATE','DAYTYPE','DIRECTLEG', 'DVEXPEND','DVFARE',
                    'DVLINECODE','DVPACKAGE','DVPACKCOST', 'DVPERSONS','DVPORTCODE',
                    'EXPENDCODE','EXPENDITURE','FARE','FAREK', 'FLOW','HAULKEY',
                    'INTENDLOS','INTMONTH','KIDAGE','LOSKEY','MAINCONTRA', 'MIGSI',
                    'NATIONALITY','NATIONNAME','NIGHTS1','NIGHTS2','NIGHTS3','NIGHTS4',
                    'NIGHTS5','NIGHTS6','NIGHTS7','NIGHTS8', 'NUMADULTS','NUMDAYS',
                    'NUMNIGHTS','NUMPEOPLE','PACKAGEHOL', 'PACKAGEHOLUK','PERSONS',
                    'PORTROUTE','PACKAGE','PROUTELATDEG', 'PROUTELATMIN','PROUTELATSEC',
                    'PROUTELATNS','PROUTELONDEG','PROUTELONMIN','PROUTELONSEC',
                    'PROUTELONEW','PURPOSE','QUARTER', 'RESIDENCE','RESPNSE',
                    'SEX','SHIFTNO','SHUTTLE','SINGLERETURN', 'TANDTSI','TICKETCOST',
                    'TOWNCODE1','TOWNCODE2','TOWNCODE3', 'TOWNCODE4','TOWNCODE5',
                    'TOWNCODE6','TOWNCODE7','TOWNCODE8','TRANSFER','UKFOREIGN',
                    'VEHICLE','VISITBEGAN','WELSHNIGHTS', 'WELSHTOWN','FAREKEY',
                    'TYPEINTERVIEW')
            ORDER BY SERIAL_NO,VERSION_ID
        """
    
    cur.execute(sql)
    return cur.fetchall()


def populate_survey_subsample(run_id,conn,version):

    serial_sav = 0
    version_sav = 0
    columns = []
    values = []
    
    cur = conn.cursor()
    
    
    # Ensure survey_subsample has no records for the current run before we start
    sql = "DELETE FROM SURVEY_SUBSAMPLE WHERE RUN_ID = '" + str(run_id) +"'"
    print(sql)
    cur.execute(sql)
    #conn.commit()
    
    # Commented out because we're using false details
    # Select the correct version_id for the current run, using the run_id
    #sql = "SELECT max(VERSION_ID) FROM RUN_DATA_MAP WHERE RUN_ID = '" + run_id + "' AND DATA_SOURCE = 'SURVEY DATA LOADING'"
    #version_id = cur.fetch()
        
    version_id = str(version)
    
    # Get sample data from SURVEY_COLUMN & SURVEY_VALUE tables using version_id
    # (((THIS WILL BE THE VERSION ID)))
    sample = get_column_and_version_data(cur, version_id)
        
    
    # Write the run information to the RUN_DATA_MAP table
    sql = "insert into RUN_DATA_MAP (RUN_ID,VERSION_ID,DATA_SOURCE) values ('" \
          + run_id + "'," + version_id + ",'SURVEY DATA LOADING')"
            
    cur.execute(sql)
    #conn.commit()
        
    # Initialise a row counter for added records
    recCount = 1
    
    # Loop through the extracted sample data and populate SURVEY_SUBSAMPLE
    for i in sample:
        if(i[3] != serial_sav or i[1] != version_sav):
            if(serial_sav != 0):
                # Make all columns upper case before the insert
                columns = [col.upper() for col in columns]
                
                # Remove the extra quotes from the extracted strings
                for x in range(0,len(values)):
                    if(type(values[x]) == type("")):
                        values[x] = values[x].replace("'",'') 
                                
                # Write the row of data to the SURVEY_SUBSAMPLE table
                insert_list_into_table('SURVEY_SUBSAMPLE', columns, values,conn)
                
                # Print and increment the record count to show progress
                print(recCount)
                recCount += 1
            
            # Set the current serial and version
            serial_sav = i[3]
            version_sav = i[1]
            
            # Set the initial columns and values for the row before appending
            columns = ['run_id','serial',i[2]]
            values = ["'" +run_id+"'",i[3],i[4]]
            
        else:
            # Append the current column and value to the record list
            columns.append(i[2])
            values.append(i[4])
    
    
    # Remove the extra quotes from the extracted strings
    for x in range(0,len(values)):
        if(type(values[x]) == type("")):
            values[x] = values[x].replace("'",'') 
    
    # Write the row of data to the SURVEY_SUBSAMPLE table
    insert_list_into_table('SURVEY_SUBSAMPLE', columns, values,conn)
    
    print(recCount)
    
    sql = "UPDATE RUN_DATA_MAP SET DATA_SOURCE = 'SURVEY_DATA' WHERE RUN_ID = '" \
          + run_id + "' AND VERSION_ID = '" + version_id + "'"
    cur.execute(sql)
    #conn.commit()
    print("populate_survey_subsample complete")


def extract_survey_data(survey_data_path,version_id):
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

    print(survey_data_path[-3:])
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

    df.columns = df.columns.str.upper()
    print("columns uppered")
    print(df.head)
    df_new = df.sort_values(by='SERIAL')
    df_new = df_new.filter(columns, axis=1)
    print("columns filtered")
    print(len(columns))


if __name__ == '__main__':
    survey_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\ips1712bv4_amtspnd.sas7bdat"
    version_id = 1891

    extract_survey_data(survey_data_path, version_id)
