from main.io import CommonFunctions as cf
import pandas as pd
import sys
import random

pv_name = 'weekday_end_pv'

val = """
'
weekday = float(''nan'')                    
if dataset == ''survey'': 
    
    from datetime import datetime

    day = int(row[''INTDATE''][:2])
    month = int(row[''INTDATE''][2:4])
    year = int(row[''INTDATE''][4:8])
    
    d = datetime(year,month,day)
    
    dayweek = (d.isoweekday() + 1) % 7

    if (row[''PORTROUTE''] == 811):
        if (dayweek >= 2 and dayweek <= 5):
            weekday = 1    
        else:
            weekday = 2  
    else:  
        if (dayweek >= 2 and dayweek <= 6):
            weekday = 1   
        else:
            weekday = 2
            
if (row[''PORTROUTE''] == 811):
    row[''WEEKDAY_END_PV''] = weekday
elif (row[''PORTROUTE''] >= 600):
    row[''WEEKDAY_END_PV''] = 1
else:
    row[''WEEKDAY_END_PV''] = weekday
'
"""


def write_pv_to_table(pv_name,value,conn = None):
        
    if(conn == None):
        conn = cf.get_oracle_connection()
    
    
    sql = "update PROCESS_VARIABLE_PY set PV_DEF = " + val + " where (PV_NAME = '" + pv_name + "')"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def read_pv_table(pv_name = None,conn = None):
    
    if(conn == None):
        conn = cf.get_oracle_connection()
    
    if(pv_name == None):
        sql = "select PV_NAME, PROCESS_VARIABLE_ID, PV_DEF from PROCESS_VARIABLE_PY ORDER BY PROCESS_VARIABLE_ID"   
        cur = conn.cursor()
        cur.execute(sql)
        process_variables = cur.fetchall()
        for rec in process_variables:
            # Output process variable name and definition
            print(rec[0])
            print("")
            print(rec[2])
            print("")
            print("")
        
    else:        
        sql = "select PV_NAME, PV_DEF from PROCESS_VARIABLE_PY where (PV_NAME = '" + pv_name + "')"   
        cur = conn.cursor()
        cur.execute(sql)
        process_variables = cur.fetchall()
        # Output process variable name and definition
        print('--Process Variable--')
        print(process_variables[0][0])
        print("")
        print('--Statement--')
        print(process_variables[0][1])
        print("")
        
    print len(process_variables)

""""""

#write_pv_to_table(pv_name, val)
read_pv_table()