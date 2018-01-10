import pandas as pd

from IPSTransformation import CommonFunctions as cf

conn = cf.get_oracle_connection()

sql = "select PV_NAME, PV_DEF from PROCESS_VARIABLE"

df = pd.read_sql(sql,conn)

print(df.head(1).values[0][1])

Purpose = 0

if Purpose in (20,21,22): 
    pur3_pv = 1; 
elif Purpose == None: 
    pur3_pv = None; 
else: 
    pur3_pv = 2



