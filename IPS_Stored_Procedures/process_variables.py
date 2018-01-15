from IPSTransformation import CommonFunctions as cf
import pandas as pd
import sys
import random

random.seed(123456)

def modify_values(row):
    
    
    code = "row['pur3_pv'] = 1 if row['PURPOSE'] in (20,21,22) else None if row['PURPOSE'] == None else 2"
        
    exec(code)
    
    print(row['OPERA_PV'])
    
    return row


conn = cf.get_oracle_connection()
cur = conn.cursor()


# Import Survey Sub-Sample data
sql = "select * from SURVEY_SUBSAMPLE"
df_survey_subsample = pd.read_sql(sql,conn)

# Import Process Variables data
sql = """select PV_NAME, PV_DEF 
        from 
            PROCESS_VARIABLE_PY 
        where
            PV_NAME = 'apd_pv'
      """
cur.execute(sql)
process_variables = cur.fetchall()


# Output process variable name and definition
print(process_variables[0][0])
print(process_variables[0][1])

sys.exit()

df_survey_subsample = df_survey_subsample.head(50)
df_survey_subsample =  df_survey_subsample.apply(modify_values, axis = 1)


sys.exit()
# Going to use this to write the new if sets to oracle

val = """
'if row[''stay''] == 0:
    row[''dur1_pv''] = 0
elif  row[''stay''] >= 1 and row[''stay''] <= 7:
    row[''dur1_pv''] = 1
elif  row[''stay''] >= 8 and row[''stay''] <= 21:
    row[''dur1_pv''] = 2
elif  row[''stay''] >= 22 and row[''stay''] <= 35:
    row[''dur1_pv''] = 3
elif row[''stay''] >= 36 and row[''stay''] <= 91:
    row[''dur1_pv''] = 4
elif row[''stay''] >= 92:
    row[''dur1_pv''] = 5'
"""

sql = "update PROCESS_VARIABLE_PY set PV_DEF = " + val + " where (PV_NAME = 'pur3_pv')"
cur = conn.cursor()
cur.execute(sql)
conn.commit()
