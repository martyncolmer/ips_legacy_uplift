from IPSTransformation import CommonFunctions as cf
import pandas as pd
import sys
import random

pv_name = 'apd_pv'

val = """
'
if row[''OSPORT2_PV''] in (210,500,600,700,800):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (1000,1100,1200,1700,2000):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (2100,2200,2300,2390,2500):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (2590,2800,2830,2840,150,160):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (310,320,340,2760,3020,3030):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (3040,3050,3060,3130,3170,3180):
    APDBAND = 1
elif row[''OSPORT2_PV''] in (3000,3010):
    APDBAND = 1
else:
    APDBAND = 2

if row[''FLOW''] > 4:
    row[''APD_PV''] = 0
elif APDBAND == 1:
    row[''APD_PV''] = 10/2
else:
    row[''APD_PV''] = 40/2
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

write_pv_to_table(pv_name, val)
read_pv_table(pv_name = pv_name)