'''
Created on 29 Nov 2017

@author: mahont1
'''
from CommonFunctions import IPSCommonFunctions as commonfunctions
func = commonfunctions()
conn = func.get_oracle_connection()
cur = conn.cursor()

print(cur)

cur.execute("""
        SELECT *
        FROM SURVEY_COLUMN
        """
        )
print "-"
print(cur.fetchone())

vals = (1234,999,'TestCol','TestType',8)

print (vals)

# cur.execute("""INSERT into SURVEY_COLUMN
#         (VERSION_ID,COLUMN_NO,COLUMN_DESC,COLUMN_TYPE,COLUMN_LENGTH)
#         VALUES (:uVID,:uCN,:uCD,:uCT,:uCL)""",
#         uVID=vals[0],
#         uCN=vals[1],
#         uCD=vals[2],
#         uCT=vals[3],
#         uCL=vals[4]
#         )

cur.execute("""delete from SURVEY_COLUMN 
        where VERSION_ID = :id""", 
        id = 1234
        )
conn.commit()








