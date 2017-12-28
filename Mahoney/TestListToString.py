'''
Created on 27 Dec 2017

@author: mahont1
'''
from IPSTransformation.CommonFunctions import get_oracle_connection
from IPSTransformation import CommonFunctions as cf
import pandas as pd

import sys
# def insert_into_table(table_name, column_list, value_list):
#     """
#     Author     : mahont1
#     Date       : 20 Dec 2017
#     Purpose    : Uses SQL query to insert into table
#     Params     : table_name = Name of table to insert
#                : column_list = List the names of as many columns as required
#                : value_list = List the values required to insert
#                       CODE EXAMPLE:       insert_into_table("TABLE_DATA", ("date_and_time", "message_result"), ("20/12/2017", "Hello World!"))
#                                           OR
#                                           column_list = ("date_and_time", "message_result")
#                                           values = ("20/12/2017", "Hello World!")
#                                           insert_into_table(table_name, column_list, values)                      
#     Returns    : True/False  
#     """
#      
#     # Oracle connection variables
#     conn = get_oracle_connection()
#     cur = conn.cursor()     
#      
#     
#     # Re-format column_list and value_lists as strings    
#     columns_string = str(column_list)
#     columns_string = columns_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
#     
#     
#     value_string = str(value_list)
#     value_string = value_string.replace(']', "").replace('[', "")#.replace("'","").replace(',', "")
#      
#      
#     # table_name = 'response' 
#     sql = "INSERT INTO " + table_name + " (" + columns_string + ") VALUES (" + value_string + ")"
#     
#     print(sql)
#     
#     cur.execute(sql)
#     conn.commit()
#      
#     # VALIDATE IT DID IT


table_name = "SAS_SHIFT_WT"
columns = ['SERIAL', 'SHIFT_WT']
values = [4,123]


d = {'SERIAL': [1111, 2222,3333,4444,5555,6666], 'SHIFT_WT': [1131.0, 1141.0,1234.0,2354.0,3456.0,4567.0]}

df = pd.DataFrame(data=d)


#cf.insert_dataframe_into_table(table_name, df)

cf.insert_into_table_many(table_name, df)

print("Done")