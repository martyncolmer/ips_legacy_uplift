from IPSTransformation import CommonFunctions as cf
import pandas as pd
import sys
import random
import numpy as np
import math
from sas7bdat import SAS7BDAT
random.seed(123456)

dataset = 'survey'
input_file = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\data_00.sas7bdat'
output_file1 = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\TestOutput.csv'
output_file2 = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\TestOutput_modified.csv'

def modify_values(row):
    print(row['SERIAL'])
    global process_varaiables
    
    for pv in process_varaiables:        
        code = str(pv[1])
        exec(code) 
    
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
    
    # Import Process Variables data
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

    
def process():
    
    df_data = cf.get_table_values('SAS_SURVEY_SUBSAMPLE')
    
    #df_data.to_csv("SQL_Dataframe.csv")
    
    # Import SAS file
    #df_data = pd.read_sas(input_file)
    
    df_data.fillna(value=np.NaN, inplace=True)
    
    global process_varaiables
    process_varaiables = get_pvs()
    
    df_data.to_csv(output_file1)
    
    df_data =  df_data.apply(modify_values, axis = 1)
    
    df_data.to_csv(output_file2)
    
    #cf.insert_into_table_many('SAS_SHIFT_SPV', df_data)
    
    
    

if __name__ == '__main__':
    process()
