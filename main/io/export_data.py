import os
import cx_Oracle
from main.io import CommonFunctions as cf


""" Write BLOB (to oracle) from table"""
def sas_data_export(file_id):
    
    """
    Author       : Faisal/Nassir
    Date         : 18 Dec 2017
    Purpose      : Writes full SAS or .CSV file to Oracle table as a blob type.
    Parameters   : file_id - id number to be assigned to the file being loaded 
                 : into the database.  
    Returns      : NA        
    Requirements : NA        
    Dependencies : NA        
    """
    
    # Hard coded file path to write to database. 
    export_data_dir = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data"
    export_data_filename = "export_data_out.sas7bdat"
    path_to_export_data = export_data_dir + "\\" + export_data_filename
    
    filename, extension = os.path.splitext(path_to_export_data)
    
    # Check file format i.e if not .SAS or .CSV then raise exception
    if (extension.lower() != ".sas7bdat" and extension.lower() != ".csv"):
        raise Exception("Abort: File format not supported")
    
     
    content = ''
    
    # Open and read imported file in binary mode
    if os.path.exists(path_to_export_data):
        with open (path_to_export_data,'rb') as file:
            content = file.read()
    else:
        raise Exception('Error: File not found')
    
    
    conn = cf.get_sql_connection()
    db_connection = conn.cursor()
    
    
    sqlString = "SELECT SAS_PROCESS_ID FROM SAS_DATA_EXPORT WHERE sas_process_id = " + str(file_id)
    result  = db_connection.execute (sqlString)
    column_values = result.fetchall()
    
    # If a record exists with the provided file id then an error is thrown.    
    if column_values:
            print('This id number already used. Please provide different file id')
    
    else:
        file_to_copy = filename.split('\\')[-1] + extension
        
        sqlStr = "insert into  sas_data_export VALUES(:id, :filecopy, :blobData)"
        db_connection.setinputsizes (blobData = cx_Oracle.BLOB)
        db_connection.execute (sqlStr, id = file_id,blobData= content, filecopy = file_to_copy)
        db_connection.execute ('commit')
        
        print('Record entered successfully with file id ' + str(file_id))
 
    # Close the Oracle database connection
    db_connection.close()



""" Write to file from BLOB"""
def sas_data_import(file_id):
    
    """
    Author       : Faisal/Nassir
    Date         : 19 Dec 2017
    Purpose      : Reads .SAS or .CSV file from Oracle table and writes it to a 
                 : file on local disk.
    Parameters   : file_id -id number of the file to be retrieved from database. 
                 : In this case, value of the column SAS_PROCESS_ID from 
                 : SAS_DATA_EXPORT table.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    conn = cf.get_sql_connection()
    db_connection = conn.cursor()
    sqlStr = "SELECT  SDE_LABEL, SDE_DATA FROM SAS_DATA_EXPORT WHERE SAS_PROCESS_ID =" + str(file_id)
    result  = db_connection.execute (sqlStr)
    content = result.fetchall()
     
    if content:
        
        file_name = content[0][0]
        try:
            
            # If file_path is unassigned, the file will be saved in the 
            # function's local directory.
            file_path = ''    
            
            with open (file_path+file_name, 'wb') as file:
                    file.write(str(content[0][1]))
            
        except IOError as io_err:
            
                print(io_err)
       
        # For comparison to check if file retrieved was same as file stored into 
        # database. Returns True if files have same contents and False otherwise. 
        file1 =  r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data\export_data_out.sas7bdat"
        print(filecmp.cmp(file1, file_name, shallow = False) )
    
    else:
        
        print('No file found with file id [ ' + str(file_id) + ' ]')   
             
    db_connection.close()