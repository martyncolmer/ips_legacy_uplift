from IPSTransformation import CommonFunctions as cf
import pandas as pd
import sys
import random
import numpy as np
import math
from sas7bdat import SAS7BDAT
random.seed(123456)

dataset = ''                  #-
intabname = ''                      #-
outtabname = ''                     #-
id = ''                             #-

input_file = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\data_00.sas7bdat'
output_file1 = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\TestOutput.csv'
output_file2 = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\TestOutput_modified.csv'

count = 0


def modify_values(row):
    if(dataset == 'shift'):
        print("SHIFT DATA")
        
    global count
    print(dataset.upper() + ": Applying Pv's to record - " + str(count))
    count+= 1
    
    global process_varaiables
    
    for pv in process_varaiables:        
        code = str(pv[1])
        try:
            exec(code) 
        except KeyError:
            print("Key Not Found")
    
    row['SHIFT_PORT_GRP_PV'] = row['SHIFT_PORT_GRP_PV'][:10]        
    
    return row


def test_modify_single_pv(row):
    
    if row['FLOW'] < 5:
        if not math.isnan(row['DVLINECODE']):
            carrier = int(row['DVLINECODE'])
        else:
            carrier = 0
            
        if carrier >= 1000 and carrier <= 1999:
            row['OPERA_PV'] = 1
        elif carrier >= 2000 and carrier <= 88880:
            row['OPERA_PV'] = 2
            
    elif row['FLOW'] > 4:
        row['OPERA_PV'] = 3
    
    if math.isnan(row['OPERA_PV']):
        row['OPERA_PV'] = round(random.random(),0) + 1

    
    return row


def get_pvs(conn = None):
    
    if(conn == None):
        conn = cf.get_oracle_connection()
    
    cur = conn.cursor()
    
    if(dataset == 'survey'):
        sql = """SELECT 
                    PV_NAME,PV_DEF,PROCESS_VARIABLE_ID 
                 FROM 
                    PROCESS_VARIABLE_PY 
                 WHERE 
                    PV_NAME IN ('shift_port_grp_pv','weekday_end_pv'
                                  ,'am_pm_night_pv','shift_flag_pv'
                                  ,'crossings_flag_pv') 
                 ORDER BY 
                    PROCESS_VARIABLE_ID""" 
    
    elif(dataset == 'shift'):
                sql = """SELECT 
                    PV_NAME,PV_DEF,PROCESS_VARIABLE_ID 
                 FROM 
                    PROCESS_VARIABLE_PY 
                 WHERE 
                    PV_NAME IN ('shift_port_grp_pv','weekday_end_pv'
                                  ,'am_pm_night_pv') 
                 ORDER BY 
                    PROCESS_VARIABLE_ID"""
    
    else:
        # Import all process variables data
        sql = "select PV_NAME,PV_DEF,PROCESS_VARIABLE_ID from PROCESS_VARIABLE_PY ORDER BY PROCESS_VARIABLE_ID"
        
    cur.execute(sql)
    return cur.fetchall()


def file_to_dataframe(file_name):
    
    if(file_name[-3:] == 'csv'):
        print("CSV IMPORT FOUND")
        df = pd.read_csv(file_name)
        print("File read successful.")
        return df
    elif(file_name[-8:] == 'sas7bdat'):
        try:
            print("SAS IMPORT FOUND")
            df = pd.read_sas(file_name)
            print("File read successful.")
            return df
        except:
            print("ERROR READING SAS: pd.read_sas() unable to read file. Attempting SAS7BDAT.to_data_frame()")
            df = SAS7BDAT(file_name).to_data_frame()
            print("File read successful.")
            return df

    
def process(in_dataset, in_intabname, in_outtabname, in_id):
    
    global dataset 
    global intabname 
    global outtabname 
    global id 
    
    dataset = in_dataset
    intabname = in_intabname.upper()
    outtabname = in_outtabname
    id = in_id
    
    
    df_data = cf.get_table_values(intabname)
    
    #df_data.to_csv("SQL_Dataframe.csv")
    
    # Import SAS file
    #df_data = pd.read_sas(input_file)
    
    df_data.fillna(value=np.NaN, inplace=True)
    
    global process_varaiables
    process_varaiables = get_pvs()
    
    
    
    df_data.to_csv(output_file1)
    
    df_data =  df_data.apply(modify_values, axis = 1)
    
    
    df_data.to_csv(output_file2)
    
    if(dataset == 'survey'):
        df_out = df_data[['SERIAL','SHIFT_PORT_GRP_PV','WEEKDAY_END_PV','AM_PM_NIGHT_PV'
                               ,'SHIFT_FLAG_PV','CROSSINGS_FLAG_PV']]
        cf.insert_into_table_many(outtabname, df_out)
    elif(dataset == 'shift'):
        df_out = df_data[['REC_ID','SHIFT_PORT_GRP_PV','AM_PM_NIGHT_PV','WEEKDAY_END_PV']]
        
        cf.insert_into_table_many(outtabname, df_out)
        
    else:
        print("ERROR: dataset not set to 'survey'/'shift'")
    
    
    

if __name__ == '__main__':
    process()
