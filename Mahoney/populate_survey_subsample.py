'''
Created on 1 Dec 2017

@author: mahont1
'''

import sys

from CommonFunctions import IPSCommonFunctions


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
    col_string = ""
    val_string = ""
    v_version_id = 0
    
    cur = conn.cursor()
    
    # make sure that survey_subsample has no records for run before we start
    
    #sql = "DELETE FROM SURVEY_SUBSAMPLE WHERE RUN_ID = " + str(p_run_id)

    #cur.execute(sql)
    #conn.commit()
    
    
    #sql = "SELECT max(VERSION_ID) FROM RUN_DATA_MAP WHERE RUN_ID = '" + p_run_id + "' AND DATA_SOURCE = 'SURVEY DATA LOADING'"
    #v_version_id = cur.fetch()
    v_version_id = '9999'

    #Get sample data from SURVEY_COLUMN & SURVEY_VALUE tables
    sample = get_column_and_version_data(cur,'9999')
        
    print(sample[0][0])#ColumnNumber
    print(sample[0][1])#VersionID
    print(sample[0][2])#ColumnDesc
    print(sample[0][3])#SerialNumber
    print(sample[0][4])#Value
    
    
    #sql = "insert into RUN_DATA_MAP (RUN_ID,VERSION_ID,DATA_SOURCE) values ('"+p_run_id+"',"+v_version_id+",'SURVEY DATA LOADING')"
    #print(sql)
    #cur.execute(sql)
    #conn.commit()
    word = 0
    for i in sample:
        
        if(i[3] != serial_sav or i[1] != version_sav):
            
            if(serial_sav != 0):
                sql = 'INSERT INTO SURVEY_SUBSAMPLE (' + col_string + ') VALUES (' + val_string + ')'
                print(sql)
                cur.execute(sql)
                conn.commit()
            
            serial_sav = i[3]
            version_sav = i[1];
            col_string ='run_id,serial,' + i[2]
            val_string = '"' + p_run_id + '",' + str(i[3]) + ',' + str(i[4])
        else:
            col_string  = col_string + ',' + i[2]
            val_string  = val_string + ',' + str(i[4])
    
    
    cur.execute('INSERT INTO SURVEY_SUBSAMPLE (' + col_string + ') VALUES (' + val_string + ')')
    conn.commit()
    
    
    sys.exit()
    
    sql = "UPDATE RUN_DATA_MAP SET DATA_SOURCE = 'SURVEY_DATA' WHERE RUN_ID = " + p_run_id + " AND VERSION_ID = " + v_version_id
    cur.execute(sql)
    conn.commit()
    



cf = IPSCommonFunctions()
connection = cf.get_oracle_connection()

populate_survey_subsample(connection, 'IPSSeedRun')