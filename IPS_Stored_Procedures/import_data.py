import filecmp
from IPSTransformation import CommonFunctions as cf

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
    
    conn = cf.get_oracle_connection()
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