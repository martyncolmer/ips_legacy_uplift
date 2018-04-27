from main.io import CommonFunctions as cf
import cx_Oracle
import pandas as pd


user = "IPS_11GST1_DATA"
password = "IPS_11GST1"
database = "SYSCON"
conn = cx_Oracle.connect(user, password, database)

# get survey data
sql = "SELECT * from SURVEY_SUBSAMPLE WHERE RUN_ID = '220ce5ee-bdcc-4142-a310-71268cd507ea'"
df = pd.read_sql(sql, conn)


sql = "SELECT * from TRAFFIC_DATA WHERE RUN_ID = '220ce5ee-bdcc-4142-a310-71268cd507ea'"
df_traffic = pd.read_sql(sql, conn)

sql = "SELECT * from SHIFT_DATA WHERE RUN_ID = '220ce5ee-bdcc-4142-a310-71268cd507ea'"
df_shift = pd.read_sql(sql, conn)

sql = "SELECT * from NON_RESPONSE_DATA WHERE RUN_ID = '220ce5ee-bdcc-4142-a310-71268cd507ea'"
df_nr = pd.read_sql(sql, conn)

sql = "SELECT * from UNSAMPLED_OOH_DATA WHERE RUN_ID = '220ce5ee-bdcc-4142-a310-71268cd507ea'"
df_ooh = pd.read_sql(sql, conn)


df.to_csv("post_import_SURVEY_SUBSAMPLE.csv", index=False)
df_traffic.to_csv("post_import_TRAFFIC_DATA.csv", index=False)
df_shift.to_csv("post_import_SHIFT_DATA.csv", index=False)
df_nr.to_csv("post_import_NON_RESPONSE_DATA.csv", index=False)
df_ooh.to_csv("post_import_UNSAMPLED_OOH_DATAcsv", index=False)



print("GOOD JOB TOM!")
#220ce5ee-bdcc-4142-a310-71268cd507ea