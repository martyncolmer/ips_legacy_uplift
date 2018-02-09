import sys

from IPSTransformation.CommonFunctions import get_oracle_connection
from IPSTransformation.CommonFunctions import insert_list_into_table


def get_column_and_version_data(cur, version):
    
    sql = """SELECT 
                SC.COLUMN_NO, 
                SC.VERSION_ID, 
                SC.COLUMN_DESC, 
                SV.SERIAL_NO,
                case when SC.COLUMN_TYPE != 'varchar2'
                     then trim(SV.COLUMN_VALUE)
                     else ''''||trim(SV.COLUMN_VALUE)||'''' end VALUE
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


def populate_survey_subsample(conn, p_run_id):

    serial_sav = 0
    version_sav = 0
    columns = []
    values = []
    version_id = 0
    
    cur = conn.cursor()
    
    
    # Ensure survey_subsample has no records for the current run before we start
    sql = "DELETE FROM SURVEY_SUBSAMPLE WHERE RUN_ID = '" + str(p_run_id) +"'"
    print(sql)
    cur.execute(sql)
    conn.commit()
    
    # Commented out because we're using false details
    # Select the correct version_id for the current run, using the run_id
    #sql = "SELECT max(VERSION_ID) FROM RUN_DATA_MAP WHERE RUN_ID = '" + p_run_id + "' AND DATA_SOURCE = 'SURVEY DATA LOADING'"
    #version_id = cur.fetch()
        
    version_id = '9999'
    
    # Get sample data from SURVEY_COLUMN & SURVEY_VALUE tables using version_id
    # (((THIS WILL BE THE VERSION ID)))
    sample = get_column_and_version_data(cur,'16')
        
    
    # Write the run information to the RUN_DATA_MAP table
    sql = "insert into RUN_DATA_MAP (RUN_ID,VERSION_ID,DATA_SOURCE) values ('" \
            +p_run_id+"',"+version_id+",'SURVEY DATA LOADING')"
            
    cur.execute(sql)
    conn.commit()
        
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
            version_sav = i[1];
            
            # Set the initial columns and values for the row before appending
            columns = ['run_id','serial',i[2]]
            values = ["'" +p_run_id+"'",i[3],i[4]]
            
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
            + p_run_id + "' AND VERSION_ID = '" + version_id +"'"
    cur.execute(sql)
    conn.commit()
    
    
connection = get_oracle_connection()


# Hard Coded for now, this will be generated
run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

populate_survey_subsample(connection, run_id)