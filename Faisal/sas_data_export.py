from IPSTransformation import CommonFunctions
import numpy as np
from sas7bdat import SAS7BDAT
import os
from collections import OrderedDict
import datetime
import cx_Oracle
import filecmp
 

def sas_data_export(file_id):
    
    """
    Author     : Faisal
    Date       : 18 Dec 2017
    Purpose    : Writes SAS or .CSV file to Oracle table
    Parameters : id number to be assigned to the file being loaded into database.  
    Returns    : Returns nothing        
    """
    
    #Hard coded file path to write to database. 
    path_to_export_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data\export_data_out.sas7bdat"
    
    filename, extension = os.path.splitext(path_to_export_data)
    
    # check file format i.e if not .SAS or .CSV then raise exception
    if (extension.lower() != ".sas7bdat" and extension.lower() != ".csv"):
        raise Exception("Abort: File format not supported")
    
     
    # open and read file in binary mode
    content = ''
    
    if os.path.exists(path_to_export_data):
        with open (path_to_export_data,'rb') as file:
            content = file.read()
    else:
        raise Exception('Error: File not found')
                
    conn = CommonFunctions.get_oracle_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredens.json")
    db_connection = conn.cursor()
    
    
    sqlString = "SELECT  SAS_PROCESS_ID FROM SAS_DATA_EXPORT WHERE sas_process_id = " + str(file_id)
    result  = db_connection.execute (sqlString)
    column_values = result.fetchall()
    
    # if record exists with provided file id then it notifies to provide other id otherwise it adds record to the database
    
    if column_values:

            print('This id number already used. Please provide different file id')
    
    else:
        
        file_to_copy = filename.split('\\')[-1] + extension
        
        sqlStr = "insert into  sas_data_export VALUES(:id, :filecopy, :blobData)"
        db_connection.setinputsizes (blobData = cx_Oracle.BLOB)
        db_connection.execute (sqlStr, id = file_id,blobData= content, filecopy = file_to_copy)
        db_connection.execute ('commit')
        
        print('Record entered successfully with file id ' + str(file_id))
 
    # # close the Oracle database connection
    db_connection.close()
    
    
def sas_data_import(file_id):
    
    """
    Author     : Faisal
    Date       : 19 Dec 2017
    Purpose    : Reads .SAS or .CSV file from Oracle table and writes it to a file on local disk
    Parameters : id number of the file to be retrieved from database. In this case, value of the column SAS_PROCESS_ID from SAS_DATA_EXPORT table 
    Returns    : Returns nothing        
    """
    
    conn = CommonFunctions.get_oracle_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredens.json")
    db_connection = conn.cursor()
    sqlStr = "SELECT  SDE_LABEL, SDE_DATA FROM SAS_DATA_EXPORT WHERE SAS_PROCESS_ID =" + str(file_id)
    result  = db_connection.execute (sqlStr)
    content = result.fetchall()
     
    if content:
        
        file_name = content[0][0]
        try:
            
            # assign this variable to the desired location for saving the file otherwise file will be saved in current folder the functions executes from. 
            file_path = ''    
            with open (file_path+file_name, 'wb') as file:
                    file.write(str(content[0][1]))
            
        except IOError as io_err:
            
                print(io_err)
       
        # For comparison to check if file retrieved was same as file stored into database. True if files have same contents and False otherwise. 
        # If required, this needs to be done dynamically rather than with hard coded file paths
        
        file1 =  r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data\export_data_out.sas7bdat"
        print(filecmp.cmp(file1, file_name, shallow = False) )
    
    else:
        
        print('No file found with file id [ ' + str(file_id) + ' ]')   
             
    db_connection.close()
 
#comment out the line below for testing i.e if exporting then comment sas_data_import and vice versa...

#sas_data_export(1112)    
sas_data_import(1111)    
    
        
    
    