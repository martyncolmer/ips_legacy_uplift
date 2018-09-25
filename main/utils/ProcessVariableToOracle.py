from main.io import CommonFunctions as cf
import pandas as pd
import sys
import random

pv_name = 'SPEND_IMP_ELIGIBLE_PV'

val = """
'
if (row[''FLOW''] in (1,4,5,8) and row[''PURPOSE''] < 80 and row[''PURPOSE''] != 23 and row[''PURPOSE''] != 24 and row[''MINS_FLAG_PV''] == 0) or (row[''FLOW''] in (1,4,5,8) and str(row[''PURPOSE'']) == ''nan'' and row[''MINS_FLAG_PV''] == 0):        
    row[''SPEND_IMP_ELIGIBLE_PV''] = 1
else:
    row[''SPEND_IMP_ELIGIBLE_PV''] = 0
'
"""


def write_pv_to_table(pv_name,value,conn = None):

    if(conn == None):
        conn = cf.get_sql_connection()

    sql = "update PROCESS_VARIABLE_PY set PV_DEF = " + val + " where (PV_NAME = '" + pv_name + "')"
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def read_pv_table(pv_name = None,conn = None):

    if(conn == None):
        conn = cf.get_sql_connection()

    if(pv_name == None):
        sql = "select PV_NAME, PROCESS_VARIABLE_ID, PV_DEF from PROCESS_VARIABLE_PY ORDER BY PROCESS_VARIABLE_ID"
        print(sql)
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

    print(len(process_variables))

# write_pv_to_table(pv_name, val)
read_pv_table(pv_name)