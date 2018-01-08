'''
Created on 24 Nov 2017

@author: thorne1
'''
import os
import zipfile
import cx_Oracle            # pip install this
import pandas as pandas     # pip install this
import json
import logging
import inspect

from sas7bdat import SAS7BDAT   # pip install this

import survey_support as ss


def database_logger():
    """
    Author        : thorne1
    Date          : 5 Jan 2018
    Purpose       : Sets up and returns database logger object   
    Parameters    : None
    Returns       : Database logger object  
    Requirements  : None
    Dependencies  : social_surveys.setup_logging
    """
    # Database logger setup
    ss.setup_logging(os.path.dirname(os.getcwd()) 
                     + "\\IPS_Logger\\IPS_logging_config_debug.json")   
    return logging.getLogger(__name__)


def standard_log_message(err_msg, func_name):
    """
    Author        : thorne1
    Date          : 5 Jan 2018
    Purpose       : Creates a standard log message which includes the user's 
                  : error message, the filename and function name
    Parameters    : err_msg - user's custom error message
                  : func_name - source function of failure
    Returns       : String  
    Requirements  : None
    Dependencies  : None
    """
    # 0 = frame object, 1 = filename. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    filename = str(inspect.stack()[0][1])
    return err_msg + ' - File "' + filename + '", in ' + func_name + '()'


def validate_file(xfile):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic function to validate file to check existence and size.
               : Validation includes: empty string instead of filename,
               : checking file exists, and if file is empty
    Params     : xfile (file is reserved keyword) - file to validate
    Returns    : True/False (boolean)
    """
    
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
   
    if xfile == "":
        # If file name not given
        err_msg = "ERROR: File name not provided"
        database_logger().error(standard_log_message(err_msg, function_name))
        return False
    if not os.path.exists(xfile):
        # If file does not exist
        err_msg = "ERROR: File does not exist"
        database_logger().error(standard_log_message(err_msg, function_name))
        return False
    if os.path.getsize(xfile) == 0:
        # If file is empty 
        err_msg = "ERROR: File is empty"
        database_logger().error(standard_log_message(err_msg, function_name))
        return False
    else:
        return True         


def get_oracle_connection(credentials_file = 
                          r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.json"):
        """
        Author     : thorne1
        Date       : 27 Nov 2017
        Purpose    : Generic function to connect to Oracle 
                   : database and return connection object
        Returns    : Connection (Object) 
                     (cannot return cursor object as DDL 
                     statements are implicitly committed
                     whereas DML statements are not)
        Params     : credentials_file is set to default location 
                   : unless user needs to point elsewhere
        REQS       : pip install cx_Oracle 
                     32-bit Oracle Client required
        DEPS       : get_credentials()
        """
        
        # Validate file
        if validate_file(credentials_file) == False:
            return False
        
        # Get credentials and decrypt              
        user = ss.get_keyvalue_from_json("User", credentials_file, True)
        password = ss.get_keyvalue_from_json("Password", credentials_file, True)
        database = ss.get_keyvalue_from_json('Database', credentials_file)
    
        try:
            # Connect
            conn = cx_Oracle.connect(user, password, database)
        except Exception as err:
            database_logger().error(err, exc_info = True)
            return False        
        else:
            return conn
        
    
def get_credentials(credentials_file):
    """
    Author        : Elinor Thorne
    Date          : 27 Nov 2017
    Purpose       : Retrieves credentials from local text file   
    Parameters    : Full dir path of credentials file 
    Returns       : Dictionary / False   
    Requirements  : None
    Dependencies  : None
    """
    
    # Validate file
    if validate_file(credentials_file):
        credentials_dict = {}
        
        try:
            with open(credentials_file) as json_file:
                credentials_dict = json.load(json_file)
        except Exception as err:
            # Log error in database
            database_logger().error(err, exc_info = True)
            return False
        else:            
            return credentials_dict
    

def extract_zip(dir_name, zip_file):
    """
    Author       : thorne1
    Date         : 8 Jan 2018
    Purpose      : Extract file from zip folder   
    Parameters   : dir_name - directory path EXCLUDING filename 
                 : zip_file - name of zipped filename
    Returns      : True/False   
    Requirements : None
    Dependencies : None
    """    

    # Validate existence of file
    if validate_file(dir_name):
        os.chdir(dir_name)
    
        file_found = False
        for item in os.listdir(dir_name):
            if item == zip_file:
                file_name = os.path.abspath(item)
                zip_ref = zipfile.ZipFile(file_name)
                zip_ref.extractall(dir_name)
                zip_ref.close()
                file_found = True
                break
        
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        
        return file_found


def import_csv(filename):
    """
    Author        : thorne1
    Date          : 8 Jan 2018
    Purpose       : Generic function to open a CSV   
    Parameters    : filename - full CSV path
    Returns       : Dataset (Object)  
    Requirements  : None
    Dependencies  : inspect,
                    validate_file()
                    pandas,
                    database_logger()
    """
    
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])

    # Validate file    
    if validate_file(filename):
        try:
            dataframe = pandas.read_csv(filename)
        except Exception as err:
            # Log error in database
            database_logger().error(err, exc_info = True)
            return False
        else:
            if dataframe.empty:
                err_msg = "ERROR: Dataframe is empty"
                # Log error in database
                database_logger().error(standard_log_message(err_msg
                                                             , function_name))
                return False
            else:
                return dataframe      


def import_sas(filename):
    """
    Author        : thorne1
    Date          : 8 Jan 2018
    Purpose       : Generic function to open a CSV  
    Parameters    : filename - full CSV path
    Returns       : Dataset (Object)  
    Requirements  : None
    Dependencies  : inspect,
                    validate_file()
                    pandas,
                    database_logger()
    """
    
    if validate_file(filename):
        try:
            # Create and return sas7bdat dataframe:
            with SAS7BDAT(filename) as file_object:
                return file_object
        except Exception as err:
            database_logger().error(err, exc_info = True)
            return False


def create_table(table_name, column_list):
    """
    Author        : thorne1
    Date          : 20 Dec 2017
    Purpose       : Uses SQL query to create a table  
    Parameters    : table_name - name of table to create
                  : column_list - List of as many column details as 
                  : required in the following format:
    FORMAT EXAMPLE: "COLUMN_NAME type(size)"
    CODE EXAMPLE  : create_table("TABLE_DATA", ("RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)"))
                    OR
                    cols = ("RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)")
                    create_table("TABLE_DATA", cols)               
    Returns       : True/False  
    Requirements  : None
    Dependencies  : check_table(),
                    get_oracle_connection(),
                    database_logger()
    """
    
    # Confirm table does not exist
    if check_table(table_name) == True:
        return False
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Re-format column_list as string
    columns = str(column_list).replace("'","") 
    
    # Create and execute SQL query 
    sql = "CREATE TABLE " + table_name + " " + columns 
    
    try:
        cur.execute(sql)
    except Exception as err:
        database_logger().error(err, exc_info = True)
        return False
    else:
        conn.commit()

    # Confirm table was created
    if check_table(table_name) == True:
        return True
    else:
        return False


def check_table(table_name):
    """
    Author        : thorne1
    Date          : 7 Dec 2017
    Purpose       : Generic SQL query to check if table exists   
    Parameters    : table_name - name of table to check if exists
    Returns       : True - Table exists / False - Table does not exist (bool)  
    Requirements  : None
    Dependencies  : get_oracle_connection()
                  : database_logger()
    """
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
     
    # Create and execute SQL query
    sql = "SELECT * from USER_TABLES where table_name = '" + table_name + "'"
    
    try:
        cur.execute(sql)
        result = cur.fetchone()
    except Exception as err:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        database_logger().error(err, exc_info = True)
        return False
    else:
        if result != None:
            # return True to indicate table exists
            return True
        else:
            return False


def drop_table(table_name):
    """
    Author        : thorne1
    Date          : 7 Dec 2017
    Purpose       : Generic SQL query to drop table  
    Parameters    : table_name - name of table to drop
    Returns       : True/False (bool)  
    Requirements  : None
    Dependencies  : check_table()
                  : get_oracle_connection()
                  : database_logger()
    """
    
    # Confirm table exists
    if check_table(table_name) == False:
        return False
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Create and execute SQL query
    sql = "DROP TABLE " + table_name
    
    try:
        cur.execute(sql)
    except Exception as err:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        database_logger().error(err, exc_info = True)
        return False
    else:
        if check_table(table_name) == True:
            # return False to indicate table still exists
            return False
        else:
            conn.commit()
            return True
    
    
def delete_from_table(table_name, condition1 = None, operator = None, condition2 = None, condition3 = None):
    """
    Author         : thorne1
    Date           : 7 Dec 2017
    Purpose        : Generic SQL query to delete contents of table   
    Parameters     : table_name - name of table
                     condition1 - first condition / value
                     operator - comparison operator i.e    
                     '=' Equal
                     '!=' Not Equal
                     '>' Greater than
                     '>=' Greater than or equal, etc
                     https://www.techonthenet.com/oracle/comparison_operators.php
                     condition2 - second condition / value
                     condition3 - third condition / value used for BETWEEN 
                     ranges, i.e: "DELETE FROM table_name WHERE condition1 
                     BETWEEN condition2 AND condition3"
    Returns         : True/False (bool)   
    Requirements    : None
    Dependencies    : check_table(),
                      get_oracle_connection,
    """
    
    # Confirm table exists
    if check_table(table_name) == False:
        return False    
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    # Create and execute SQL query
    if condition1 == None:
        # DELETE FROM table_name
        sql = ("DELETE FROM " + table_name)
    elif condition3 == None:
        # DELETE FROM table_name WHERE condition1 <operator> condition2
        sql = ("DELETE FROM " + table_name 
               + " WHERE " + condition1 
               + " " + operator 
               + " '" + condition2 + "'")
    else:
        # DELETE FROM table_name WHERE condition1 BETWEEN condition2 AND condition3
        sql = ("DELETE FROM " + table_name 
               + " WHERE " + condition1 
               + " " + operator 
               + " '" + condition2 + "'" 
               + " AND " + condition3)
        
    try:
        cur.execute(sql)
    except Exception as err:
        database_logger().error(err, exc_info = True)
        return False
    else:   
        conn.commit()
        return True
        

def select_data(column_name, table_name, condition1, condition2):
    """
        Author        : thorne1
        Date          : 21 Dec 2017
        Purpose       : Uses SQL query to retrieve value from Oracle table  
        Parameters    : column_name, table_name, condition1, condition2, i.e:
                      : "SELECT column_name FROM table_name WHERE condition1 = condition2" (no 'AND'/'OR' clause)
        Returns       : Result (String)  
        Requirements  : None
        Dependencies  : get_oracle_connection(),
                        database_logger()
        """
    
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Create SQL statement
    sql = ("SELECT " + column_name 
           + " FROM " + table_name 
           + " WHERE " + condition1 
           + " = '" + condition2 + "'")

    # Execute
    try:
        cur.execute(sql)
    except Exception as err:
        # Return False to indicate error
        database_logger().error(err, exc_info = True)
        return False
    else:
        val = cur.fetchone()
        result = str(val).strip("(,)")
        
    if result == 'None':
        # Query failed to return result.  Return False to indicate failure
        return False
    else:
        return result


def unload_parameters(parameter_id = False):    
    """
    Author     : Thomas Mahoney
    Date       : 19 Dec 2017
    Purpose    : Extracts a list of parameters from oracle to be used in the parent process.      
    Params     : parameter_id - the identifier used to extract specific parameter sets.
    Returns    : A dictionary of parameters
    """
   
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # If no ID provided, fetch latest ID from SAS_PARAMETERS 
    if parameter_id == False:
        cur.execute("select max(PARAMETER_SET_ID) from SAS_PARAMETERS")
        parameter_id = cur.fetchone()[0]
        
    print(parameter_id)
    
    # Create SQL query and execute
    sql = "select PARAMETER_NAME, PARAMETER_VALUE from SAS_PARAMETERS where PARAMETER_SET_ID = " + str(parameter_id)
    
    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError:
        # Return False to indicate error
        raise
        return False
    else: 
        # Execute SQL query and return parameters   
        results = cur.fetchall()
    
    
    # If no results, return False to indicate failure
    if results == []:
        return False
    
    # Create dictionary of parameters and return
    tempDict = {}
    for result in results:
        value = result[1].upper()
        
        if " " in value:
            value = value.split(" ")
            
        tempDict[result[0]] = value
    
    return tempDict


def insert_dataframe_into_table(table_name, dataframe):
    column_list = list(dataframe.columns.values)
    print(column_list)
    
    
    for val in dataframe.values:
        v_list = list(val)
        print(v_list)
        
        insert_into_table(table_name,column_list,v_list)


def insert_into_table(table_name, column_list, value_list):
    """
    Author     : mahont1
    Date       : 20 Dec 2017
    Purpose    : Uses SQL query to insert into table
    Params     : table_name = Name of table to insert
               : column_list = List the names of as many columns as required
               : value_list = List the values required to insert
                      CODE EXAMPLE:       insert_into_table("TABLE_DATA", ("date_and_time", "message_result"), ("20/12/2017", "Hello World!"))
                                          OR
                                          column_list = ("date_and_time", "message_result")
                                          values = ("20/12/2017", "Hello World!")
                                          insert_into_table(table_name, column_list, values)                      
    Returns    : True/False  
    """
     
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()     
     
    
    # Re-format column_list and value_lists as strings    
    columns_string = str(column_list)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
    
    value_string = str(value_list)
    value_string = value_string.replace(']', "").replace('[', "")#.replace("'","").replace(',', "")
     
    # table_name = 'response' 
    sql = "INSERT INTO " + table_name + " (" + columns_string + ") VALUES (" + value_string + ")"
    
    print(sql)
    
    cur.execute(sql)
    conn.commit()


def insert_into_table_many(table_name,dataframe):
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Inserts a full dataframe into an SQL table 
    Params       : table_name - the name of the target table in the sql database.
                   dataframe - the dataframe to be added to the selected table.
    Returns      : The number of rows added to the database.
    Requirements : NA
    Dependencies : NA
    """
    
    print(table_name)
    rows = [tuple(x) for x in dataframe.values]
    dataframe.columns = dataframe.columns.astype(str)#.str.split(',')
    columns_list = dataframe.columns.tolist()

    # Create column header string for SQL
    columns_string = str(columns_list)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
    
    parameter_holder = []
    # Create parameter holder string for SQL
    for x in range(0,len(dataframe.columns.tolist())):
        parameter_holder.append(":p"+str(x))
    
    parameter_string = str(parameter_holder)
    parameter_string = parameter_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
    print(parameter_string)
    
    
    connection = get_oracle_connection()
    cur = connection.cursor()

    sql = "INSERT into " + table_name + \
    "(" \
    + columns_string + \
    ") VALUES (" \
    + parameter_string +\
    ")"
    print(sql)

    cur.executemany("INSERT into " + table_name + 
         "(" 
         + columns_string + 
         ") VALUES (" 
         + parameter_string +
         ")",
         rows
         )
    

    print("Records added to "+table_name+" table - " + str(len(rows)))     
    connection.commit()
    
    # Returns number of rows added to table for validation
    return len(rows)