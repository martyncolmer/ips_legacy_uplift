'''
Created on 24 Nov 2017

@author: Elinor Thorne
'''
import datetime
import getpass
import inspect
import json
import logging
import numpy as np
import os
import pandas as pandas     # pip install this
import pyodbc
import time
import winsound
import zipfile

from pandas.util.testing import assert_frame_equal
from sas7bdat import SAS7BDAT

# import survey_support as ss


def database_logger():
    """
    Author        : Elinor Thorne
    Date          : 5 Jan 2018
    Purpose       : Sets up and returns database logger object   
    Parameters    : None
    Returns       : Database logger object  
    Requirements  : None
    Dependencies  : social_surveys.setup_logging
    """
    # Database logger setup
    # ss.setup_logging(os.path.dirname(os.getcwd())
    #                  + "\\IPS_Logger\\IPS_logging_config_debug.json")
    return logging.getLogger(__name__)


def standard_log_message(err_msg, current_working_file, func_name):
    """
    Author        : Elinor Thorne
    Date          : 5 Jan 2018
    Purpose       : Creates a standard log message which includes the user's 
                  : error message, the filename and function name
    Parameters    : err_msg - user's custom error message
                  : current_working_file - source dir path of failure
                  : func_name - source function of failure
    Returns       : String  
    Requirements  : None
    Dependencies  : None
    """

    # 0 = frame object, 1 = filename. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    filename = str(inspect.stack()[0][1])
    return (err_msg 
            + ' - File "' + current_working_file 
            + '", in ' + func_name + '()')


def validate_file(xfile, current_working_file, function_name):
    """
    Author        : Elinor Thorne
    Date          : 7 Dec 2017
    Purpose       : Generic function to validate file. Validation includes: 
                  : empty string instead of filename,
                  : checking file exists, and if file is empty  
    Parameters    : xfile (file is reserved keyword) - file to validate
                  : function_name - source function of failed validation 
    Returns       : True/False (boolean)  
    Requirements  : None
    Dependencies  : inspect,
                    database_logger(),
    """
   
    if xfile == "":
        # If file name not given
        err_msg = "ERROR: File name not provided"
        database_logger().error(standard_log_message(err_msg
                                                     , current_working_file
                                                     , function_name))
        return False
    if not os.path.exists(xfile):
        # If file does not exist
        err_msg = "ERROR: File does not exist"
        database_logger().error(standard_log_message(err_msg
                                                     , current_working_file
                                                     , function_name))
        return False
    if os.path.getsize(xfile) == 0:
        # If file is empty 
        err_msg = "ERROR: File is empty"
        database_logger().error(standard_log_message(err_msg
                                                     , current_working_file
                                                     , function_name))
        return False
    else:
        return True         


# def get_sql_connection(credentials_file =
#                           r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.json"):
#     """
#     Author     : thorne1
#     Date       : 27 Nov 2017
#     Purpose    : Generic function to connect to Oracle
#                : database and return connection object
#     Returns    : Connection (Object)
#                  (cannot return cursor object as DDL
#                  statements are implicitly committed
#                  whereas DML statements are not)
#     Params     : credentials_file is set to default location
#                : unless user needs to point elsewhere
#     REQS       : pip install cx_Oracle
#                  32-bit Oracle Client required
#     DEPS       : get_credentials()
#     """
#
#     # 0 = frame object, 1 = filename, 3 = function name.
#     # See 28.13.4. in https://docs.python.org/2/library/inspect.html
#     current_working_file = str(inspect.stack()[0][1])
#     function_name = str(inspect.stack()[0][3])
#
#     # Validate file
#     if validate_file(credentials_file
#                      , current_working_file
#                      , function_name) == False:
#         return False
#
#     # Get credentials and decrypt
#     user = ss.get_keyvalue_from_json("User", credentials_file, True)
#     password = ss.get_keyvalue_from_json("Password", credentials_file, True)
#     database = ss.get_keyvalue_from_json('Database', credentials_file)
#
#     try:
#         # Connect
#         conn = cx_Oracle.connect(user, password, database)
#     except Exception as err:
#         database_logger().error(err, exc_info = True)
#         return False
#     else:
#         return conn
#
#
# def get_sql_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials_SQLServer.json"):
#     """
#     Author       : Thomas Mahoney
#     Date         : 03 / 04 / 2018
#     Purpose      : Establishes a connection to the SQL Server database and returns the connection object.
#     Parameters   : in_table_name - the IPS survey records for the period.
#                    credentials_file  - file containing the server and login credentials used for connection.
#     Returns      : a pyodbc connection object.
#     Requirements : NA
#     Dependencies : NA
#     """
#
#     # Check if file exists or is empty
#     if not validate_file(credentials_file, str(inspect.stack()[0][1]), str(inspect.stack()[0][3])):
#         return False
#
#     # Get credentials and decrypt
#     username = ss.get_keyvalue_from_json("User", credentials_file)
#     password = ss.get_keyvalue_from_json("Password", credentials_file)
#     database = ss.get_keyvalue_from_json('Database', credentials_file)
#     server = ss.get_keyvalue_from_json('Server', credentials_file)
#
#     # Attempt to connect to the database
#     try:
#         conn = pyodbc.connect(driver="{SQL Server}", server=server, database=database, uid=username, pwd=password, autocommit=True)
#     except Exception as err:
#         database_logger().error(err, exc_info = True)
#         return False
#     else:
#         return conn

def get_sql_connection():
    """
    Author       : Thomas Mahoney / Nassir Mohammad (edits)
    Date         : 11 / 07 / 2018
    Purpose      : Establishes a connection to the SQL Server database and returns the connection object.
    Parameters   : in_table_name - the IPS survey records for the period.
                   credentials_file  - file containing the server and login credentials used for connection.
    Returns      : a pyodbc connection object.
    Requirements : NA
    Dependencies : NA
    """

    # Get credentials and decrypt
    username = os.getenv("DB_USER_NAME")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")
    server = os.getenv("DB_SERVER")

    # Attempt to connect to the database
    try:
        conn = pyodbc.connect(driver="{SQL Server}", server=server, database=database, uid=username, pwd=password, autocommit=True)
    except Exception as err:
        # print("computer says no")
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
    # 0 = frame object, 1 = filename, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3])
   
    if validate_file(credentials_file, current_working_file, function_name):
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
    else:
        return False
    

def extract_zip(dir_name, zip_file):
    """
    Author       : Elinor Thorne
    Date         : 8 Jan 2018
    Purpose      : Extract file from zip folder   
    Parameters   : dir_name - directory path EXCLUDING filename 
                 : zip_file - name of zipped filename
    Returns      : True/False   
    Requirements : None
    Dependencies : None
    """    
   
    # 0 = frame object, 1 = filename, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3])
    
    # Validate existence of file
    if validate_file(dir_name, current_working_file, function_name):
    
        file_found = False
        for item in os.listdir(dir_name):
            if item == zip_file:
                file_name = os.path.join(dir_name, zip_file)
                zip_ref = zipfile.ZipFile(file_name)
                zip_ref.extractall(dir_name)
                zip_ref.close()
                file_found = True
                break
        
        return file_found


def import_csv(filename):
    """
    Author        : Elinor Thorne
    Date          : 8 Jan 2018
    Purpose       : Generic function to open a CSV   
    Parameters    : filename - full CSV path
    Returns       : Dataframe (Object)  
    Requirements  : None
    Dependencies  : inspect,
                    validate_file()
                    pandas,
                    database_logger()
    """
    
    # 0 = frame object, 1 = filename, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3])

    # Validate file    
    if validate_file(filename, current_working_file, function_name):
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
                                                             , current_working_file
                                                             , function_name))
                return False
            else:
                return dataframe      


def import_sas(filename):
    """
    Author        : Elinor Thorne
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
    
    # 0 = frame object, 1 = filename, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3])
    
    if validate_file(filename, current_working_file, function_name):
        try:
            # Create and return sas7bdat dataframe:
            with SAS7BDAT(filename) as file_object:
                return file_object
        except Exception as err:
            database_logger().error(err, exc_info = True)
            return False
    else:
        return False


def create_table(table_name, column_list):
    """
    Author        : Elinor Thorne
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
                    get_sql_connection(),
                    database_logger()
    """
    
    # Oracle connection variables
    conn = get_sql_connection()
    cur = conn.cursor()
    
    # Re-format column_list as string
    columns = str(column_list).replace("'","") 

    #table_name = 'dbo.' + table_name

    # Create and execute SQL query 
    sql = "CREATE TABLE " + table_name + " " + columns

    try:
        cur.execute(sql)
    except Exception as err:
        #database_logger().error(err, exc_info = True)
        return False
    else:
        conn.commit()

    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3])    
    
    # Confirm table was created
    if check_table(table_name) == True:
        conn.commit()
        return True
    else:
        err_msg = "ERROR: %s was not created" %table_name
        #database_logger().error(standard_log_message(err_msg, current_working_file, function_name))
        return False


def check_table(table_name):
    """
    Author        : Elinor Thorne
    Date          : 7 Dec 2017
    Purpose       : Generic SQL query to check if table exists   
    Parameters    : table_name - name of table to check if exists
    Returns       : True - Table exists / False - Table does not exist (bool)  
    Requirements  : None
    Dependencies  : get_sql_connection()
                  : database_logger()
    """
    
    # Oracle connection variables
    conn = get_sql_connection()
    cur = conn.cursor()
     
    # Create and execute SQL query
    sql = "SELECT * from INFORMATION_SCHEMA.TABLES where table_name = '" + table_name + "'"

    try:
        cur.execute(sql)
        result = cur.fetchone()
    except Exception as err:
        print("9")
        # Raise (unit testing purposes) and return False to indicate table does not exist
        #database_logger().error(err, exc_info = True)
        return False
    else:
        if result != None:
            # return True to indicate table exists
            conn.commit()
            return True
        else:
            return False


def drop_table(table_name):
    """
    Author        : Elinor Thorne
    Date          : 7 Dec 2017
    Purpose       : Generic SQL query to drop table  
    Parameters    : table_name - name of table to drop
    Returns       : True/False (bool)  
    Requirements  : None
    Dependencies  : check_table()
                  : get_sql_connection()
                  : database_logger()
    """

    # 0 = frame object, 1 = filename, 3 = function name.
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3])

    # Oracle connection variables
    conn = get_sql_connection()
    cur = conn.cursor()
    
    # Create and execute SQL query
    sql = "DROP TABLE " + table_name
    
    try:
        cur.execute(sql)
    except Exception as err:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        #database_logger().error(err, exc_info = True)
        return False
    else:
        if check_table(table_name) == True:
            # return False to indicate table still exists
            err_msg = "ERROR: %s was not dropped" %table_name
            #database_logger().error(standard_log_message(err_msg, current_working_file, function_name))
            return False
        else:
            conn.commit()
            return True
    
    
def delete_from_table(table_name, condition1=None, operator=None
                      , condition2=None, condition3=None):
    """
    Author         : Elinor Thorne
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
                      get_sql_connection,
    """
    
    # Oracle connection variables
    conn = get_sql_connection()
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
        print("bla!")
        #database_logger().error(err, exc_info = True)
        return False
    else:
        return sql
        

def select_data(column_name, table_name, condition1, condition2):
    """
    Author        : Elinor Thorne
    Date          : 21 Dec 2017
    Purpose       : Uses SQL query to retrieve values from database
    Parameters    : column_name, table_name, condition1, condition2, i.e:
                  : "SELECT column_name FROM table_name WHERE condition1 = condition2" (no 'AND'/'OR' clause)
    Returns       : Data Frame for multiple values, scalar/string for single values
    Requirements  : None
    """

    # Connection variables
    conn = get_sql_connection()
    # cur = conn.cursor()

    # Create SQL statement
    sql = """
        SELECT {} 
        FROM {}
        WHERE {} = '{}'
        """.format(column_name, table_name, condition1, condition2)

    try:
        print(sql)
        result = pandas.read_sql(sql, conn)
        print("Result: {}".format(result))
    except Exception as err:
        print(err)
        # Return False to indicate error
        # database_logger().error(err, exc_info = True)
        return False

    # if an empty data frame is returned
    if len(result) == 0:
        # err_msg = "ERROR: SQL query failed to return result."
        result = False
    elif len(result) == 1:
        # if a single value is returned we don't want it to be a data frame
        result = result.loc[0, column_name]

    return result


def unload_parameters(parameter_id = False):
    """
    Author        : Thomas Mahoney
    Date          : 19 Dec 2017
    Purpose       : Extracts a list of parameters from oracle 
                  : to be used in the parent process.  
    Parameters    : parameter_id - the identifier used to 
                    extract specific parameter sets.
    Returns       : A dictionary of parameters  
    Requirements  : None
    Dependencies  : get_sql_connection(),
                    cx_Oracle,
    """
   
    # Connection variables
    conn = get_sql_connection()
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
    except Exception as err:
        # Return False to indicate error
        database_logger().error(err, exc_info = True)
        return False
    else: 
        # Execute SQL query and return parameters   
        results = cur.fetchall()
    
    # 0 = frame object, 1 = filename, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    current_working_file = str(inspect.stack()[0][1])
    function_name = str(inspect.stack()[0][3]) 

    # If no results, return False to indicate failure
    if results == []:
        err_msg = "ERROR: SQL query failed to return result."
        database_logger().error(standard_log_message(err_msg
                                                     , current_working_file
                                                     , function_name))
        return False
    
    # Create dictionary of parameters and return
    tempDict = {}
    for result in results:
        if result[1] != None:
            value = result[1].upper()
            
            if " " in value:
                value = value.split(" ")
        else:
            value = ''
        tempDict[result[0]] = value
    
    return tempDict


def get_table_values(table_name):
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Extracts a full table into a pandas dataframe
    Params       : table_name - the name of the target table in the sql database.
    Returns      : Dataframe containing the extracted table data.
    Requirements : NA
    Dependencies : NA
    """

    # Connection to the database
    conn = get_sql_connection()
    
    # Create SQL statement
    sql = "SELECT * from " + table_name

    # Execute the sql statement using the pandas.read_sql function and return
    # the result.
    return pandas.read_sql(sql, conn)


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
    conn = get_sql_connection()
    cur = conn.cursor()     
    
    # Re-format column_list and value_lists as strings    
    columns_string = str(column_list)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
    
    value_string = str(value_list)
    value_string = value_string.replace(']', "").replace('[', "").replace('"', '')#.replace("'","")#.replace(',', "")
     
    # table_name = 'response' 
    sql = "INSERT INTO " + table_name + " (" + columns_string + ") VALUES (" + value_string + ")"
    
    print(sql)
    
    print("executing")
    cur.execute(sql)
    print("commiting")
    conn.commit()


def insert_into_table_many(table_name,dataframe,connection = False):
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

    if(connection == False):
        print("Getting Connection")
        connection = get_sql_connection()
    
    cur = connection.cursor()

    rows = [tuple(x) for x in dataframe.values]
    dataframe.columns = dataframe.columns.astype(str)#.str.split(',')
    columns_list = dataframe.columns.tolist()

    # Create column header string for SQL
    columns_string = str(columns_list)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
    
    parameter_holder = []
    # Create parameter holder string for SQL
    for x in range(0,len(dataframe.columns.tolist())):
        parameter_holder.append("?")
    
    parameter_string = str(parameter_holder)
    parameter_string = parameter_string.replace(']', "").replace('[', "").replace("'","")#.replace(',', "")
    #print(parameter_string)
    sql = "INSERT into " + table_name + \
    "(" \
    + columns_string + \
    ") VALUES (" \
    + parameter_string +\
    ")"
    print(sql)
    
    #Debugging
    #for rec in rows:
    #    print (rec)
    
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


def insert_list_into_table(table_name,columns,values,connection = False):
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Inserts a single row dataframe into an SQL table 
    Params       : table_name - the name of the target table in the sql database.
                   columns - the column headers for the fields being added.
                   values - the fields being added to the database.
                   connection - the database connection object
    Returns      : true or false depending on success.
    Requirements : NA
    Dependencies : NA
    """
    
    if(connection == False):
        print("Getting Connection")
        connection = get_sql_connection()
    cur = connection.cursor()

    # Create column header string for SQL
    columns_string = str(columns)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'","")
    #print(columns_string)
    # Create parameter holder string for SQL    
    parameter_string = str(values)
    parameter_string = parameter_string.replace(']', "").replace('[', "")#.replace("'","")
    
    # Removes .0's from numerics - This has been added due to a constraint issue in populate_survey_sub
    parameter_string = parameter_string.replace(".0'","'")
    #print(parameter_string)

    sql = "INSERT into " + table_name + \
    "(" \
    + columns_string + \
    ") VALUES (" \
    + parameter_string +\
    ")"
    print(sql)
   
    cur.execute("INSERT into " + table_name + 
         "(" 
         + columns_string + 
         ") VALUES (" 
         + parameter_string +
         ")"
         )
    
    connection.commit()
    print("Record added to "+table_name+" table.")
    
    # Returns True if no errors
    return True


def commit_to_audit_log(action, process_object, audit_msg):
    """
    Author        : thorne1
    Date          : 9 Jan 2018
    Purpose       : Commits log to audit_log   
    Parameters    : action - populates the 'action' column within AUDIT_LOG
                  :     i.e 'Create', 'Run', 'Upload', etc 
                  : process_object - populates the  
                  : 'object' column within AUDIT_LOG
                  :     i.e 'ExternalTrafficData', 'Surveydata', 
                  :     'SASProcessTask', etc
                  : audit_msg - populates the 'audit_log_details' 
                  : column within AUDIT_LOG
                  :     i.e 'Uploaded new "Unsampled" data', 'Removed existing
                  :        "Unsampled" data', etc
    Returns       : None  
    Requirements  : None
    Dependencies  : None
    """
    # Create dictionary to hold table parameters 
    params = {}
    
    # Oracle connection variables
    conn = get_sql_connection()
    cur = conn.cursor()       
    
    # Assign 'audit_id' by returning max audit_id and incrementing by 1
    # HARD-CODED AS PER DP 
    sql = "SELECT MAX(AUDIT_ID) FROM AUDIT_LOG"
    audit_id = cur.execute(sql).fetchone()
    params['audit_id'] = audit_id[0] + 1
    
    # Assign 'actioned_by' 
    params['actioned_by'] = getpass.getuser()
    
    # Assign 'action'
    params['action'] = action
    
    # Assign 'process_object'
    params['object'] = process_object
    
    # Assign 'log_date and time'
    # Add date and time details to instance params dict
    py_now = datetime.datetime.now()

    params['log_date'] = datetime.datetime.now()

    # params['log_date'] = cx_Oracle.Timestamp(py_now.year
    #                                          , py_now.month
    #                                          , py_now.day
    #                                          , int(py_now.hour)
    #                                          , int(py_now.minute)
    #                                          , int(py_now.second))
    
    # Assign 'audit_log_details' 
    params['audit_log_details'] = audit_msg
    
    # Prepare SQL statement
    table_name = "AUDIT_LOG "
    params = (params['audit_id']
              , params['actioned_by']
              , params['action']    
              , params['object']
              , params['log_date']
              , params['audit_log_details'])
    sql = ("INSERT INTO " 
           + table_name 
           + """(AUDIT_ID
           , ACTIONED_BY
           , ACTION
           , OBJECT
           , LOG_DATE
           , AUDIT_LOG_DETAILS) 
           VALUES(?, ?, ?, ?, ?, ?)""")
    
    # Execute SQL
    cur.execute(sql, params)
    conn.commit()


def compare_datasets(test_name, sas_file, py_df):
    """
    Author        : thorne1
    Date          : 22 Feb 2018
    Purpose       : Compare SAS datasets with Python dataframes   
    Parameters    : test_name - this name will be printed to console
                    , sas_file - full dir path to sas7bdat file
                    , py_df - your Python dataframe
    Returns       : Nothing but useful print statements to console  
    """
    sas_df = pandas.read_sas(sas_file)
    
    print("TESTING " + test_name)
    
    try:
        assert_frame_equal(sas_df, py_df, check_names = False, check_like = True)
    except Exception as err:
        print(test_name + " failed.  Details below..")
        print(err)
    else:
        print(test_name + " SUCCESS")


def compare_dfs(test_name, sas_file, df, col_list = False):
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Oct Data\Town and Stay Imputation"
    print(sas_root + "\\" + sas_file)

    try:
        csv = pandas.read_sas(sas_root + "\\" + sas_file)
    except Exception as err:
        csv = SAS7BDAT(sas_root + "\\" + sas_file).to_data_frame()

    csv.columns = csv.columns.str.upper()
    
    fdir = r"H:\My Documents\Documents\Git Repo\Misc and Admin\Legacy Uplift\Compare"
    sas = "_sas.csv"
    py = "_py.csv"
    
    print("TESTING " + test_name)
    
    if not col_list:
        csv.to_csv(fdir+"\\"+test_name+sas)
        df.to_csv(fdir+"\\"+test_name+py)
    else:
        csv[col_list].to_csv(fdir+"\\"+test_name+sas)
        df[col_list].to_csv(fdir+"\\"+test_name+py)
    
    print(test_name + " COMPLETE")
    print("")


def insert_dataframe_into_table_rbr(table_name, dataframe, connection=False):
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

    # Check if connection to database exists and creates one if necessary.
    if not connection:
        print("Getting Connection")
        connection = get_sql_connection()

    cur = connection.cursor()

    dataframe = dataframe.where((pandas.notnull(dataframe)), None)
    dataframe = dataframe.replace(r'\s+', 'None', regex=True)

    # Extract the dataframe values into a collection of rows
    rows = [tuple(x) for x in dataframe.values]

    # Force the dataframe columns to be uppercase
    dataframe.columns = dataframe.columns.astype(str)

    # Generate a list of columns from the dataframe column collection
    columns_list = dataframe.columns.tolist()

    # Create the column header string by stripping the unneeded syntax from the column list+63

    columns_string = str(columns_list)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'", "")

    # Create a value string to hold the SQL query's parameter placeholders.
    value_string = ""

    # Populate the string for each column in the dataframe.
    for x in range(0, len(dataframe.columns.tolist())):
        if x is 0:
            value_string += "?"
        else:
            value_string += ", ?"

    # Use the strings created above to build the sql query.
    sql = "INSERT into " + table_name + \
          "(" + columns_string + ") VALUES (" + value_string + ")"

    print(sql)
    print("Rows to insert - " + str(len(rows)))

    # Debugging
    # for rec in rows:
    #    print (rec)

    for row in rows:
        print(row)
        cur.execute(sql, row)

    print("Records added to " + table_name + " table - " + str(len(rows)))
    #connection.commit()

    # Returns number of rows added to table for validation
    return len(rows)


def insert_dataframe_into_table(table_name, dataframe, connection=False):
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

    # Check if connection to database exists and creates one if necessary.
    if not connection:
        print("Getting Connection")
        connection = get_sql_connection()

    cur = connection.cursor()

    dataframe = dataframe.where((pandas.notnull(dataframe)), None)
    # print(dataframe)
    # print(list(dataframe.columns.values))

    # Extract the dataframe values into a collection of rows
    rows = [tuple(x) for x in dataframe.values]

    # Force the dataframe columns to be uppercase
    dataframe.columns = dataframe.columns.astype(str)

    # Generate a list of columns from the dataframe column collection
    columns_list = dataframe.columns.tolist()

    # Create the column header string by stripping the unneeded syntax from the column list+63

    columns_string = str(columns_list)
    columns_string = columns_string.replace(']', "").replace('[', "").replace("'", "")

    # Create a value string to hold the SQL query's parameter placeholders.
    value_string = ""

    # Populate the string for each column in the dataframe.
    for x in range(0, len(dataframe.columns.tolist())):
        if x is 0:
            value_string += "?"
        else:
            value_string += ", ?"

    # Use the strings created above to build the sql query.
    sql = "INSERT into " + table_name + \
          "(" + columns_string + ") VALUES (" + value_string + ")"

    print(sql)
    print("Rows to insert - " + str(len(rows)))

    start_time = time.time()
    print("Start - " + str(start_time))

    cur.executemany(sql, rows)

    end_time = time.time()
    print("End - " + str(end_time))
    print("Elapsed - " + str(end_time - start_time))

    print("Records added to " + table_name + " table - " + str(len(rows)))
    #connection.commit()

    # Returns number of rows added to table for validation
    return len(rows)


def round_half_up(f):
    import math
    return math.floor(f + 0.5)


def round_series_half_up(dataframe):
    import math


def beep():
    winsound.Beep(440, 250) # frequency, duration
    time.sleep(0.25)        # in seconds (0.25 is 250ms)
    
    winsound.Beep(600, 250)
    time.sleep(0.25)

    print("boop-beep!")


def execute_sql_query(connection, sql_statement):
    cur = connection.cursor()
    cur.execute(sql_statement)
    connection.commit()


def unpickle_rick(file):
    # File location
    path = r"C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\generic_xml_steps"

    # Pickle in and CSV out
    in_file = "\{}.pkl".format(file)
    out_file = "{}.csv".format(file)

    # Read pickle in as df
    df = pandas.read_pickle(path+in_file)

    # Send to CSV
    df.to_csv(r"{}\{}".format(path, out_file))
    beep()