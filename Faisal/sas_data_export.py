from IPSTransformation import CommonFunctions
import numpy as np
from sas7bdat import SAS7BDAT
import os
from collections import OrderedDict
import datetime
import cx_Oracle
import filecmp
 

def sas_data_export(id_number):
    
    """
    Author     : Faisal
    Date       : 18 Dec 2017
    Purpose    : Writes SAS or .CSV file to Oracle table
    Parameters : Expects id number 
    Returns    : None        
    """
    
    path_to_export_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data\export_data_out.sas7bdat"
    
    filename, ext = os.path.splitext(path_to_export_data)
    # check file format
    if (ext.lower() != ".sas7bdat" and ext.lower() != ".csv"):
        raise Exception("Abort: File format not supported")
     
    # open and read file as bytes
    content = ''
    with open (path_to_export_data,'rb') as file:
        content = file.read()

    conn = CommonFunctions.get_oracle_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredens.json")
    db = conn.cursor()
    db.setinputsizes (blobData = cx_Oracle.BLOB)
    
    sqlStr = "insert into  sas_data_export VALUES(:id,'subsample', :blobData)"
    db.execute (sqlStr, id = id_number,blobData= content)
    db.execute ('commit') 
    # # close the Oracle database connection
    db.close()
    
    
def sas_data_import(file_id):
    
    """
    Author     : Faisal
    Date       : 19 Dec 2017
    Purpose    : Reads .SAS or .CSV file from Oracle table and writes it to a file on local disk
    Parameters : Expects id number 
    Returns    : None        
    """
    
    conn = CommonFunctions.get_oracle_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredens.json")
    db = conn.cursor()
    sqlStr = "SELECT SDE_DATA FROM SAS_DATA_EXPORT WHERE SAS_PROCESS_ID =" + str(file_id)
    result  = db.execute (sqlStr)
    content = result.fetchall()[0][0]
    
    with open ('importedFile.sas7bdat', 'wb') as file:
        file.write(str(content))
        
    db.close()
    
    
    #for comparison to check if file retrieved was of correct
    file1 =  r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data\export_data_out.sas7bdat"
    file2 = 'importedFile.sas7bdat'
    
#     with open (file1,'rb') as file:
#         content1 = file.read()
#         
#     with open ('importedFile.sas7bdat', 'rb') as file:
#         content2 = file.read()
    
    #Compares two files and returns true if same file
        
    print(filecmp.cmp(file1, file2, shallow = False) )
        
#test run
sas_data_export(1111)    
sas_data_import(999)    
    
        
    
    