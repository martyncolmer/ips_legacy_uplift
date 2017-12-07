'''
Created on 24 Nov 2017

@author: thorne1
'''

import os
import zipfile
import cx_Oracle    # pip install this
import pandas as pandas     # pip install this
import datetime

from sas7bdat import SAS7BDAT   # pip install this


def get_oracle_connection():
    """
    Author     : mahont1 & thorne1
    Date       : 27 Nov 2017
    Purpose    : Generic function to connect to Oracle database and return connection object
    Returns    : Connection (Object) 
                 (cannot return cursor object as DDL statements are implicitly committed
                 whereas DML statements are not)
    REQS       : pip install cx_Oracle 
                 32-bit Oracle Client required
    DEPS       : get_credentials()
    """
    
    if get_credentials() != False:
        # Retrieve credentials dictionary
        creds = get_credentials()
    else:
        return False       
    
    try:
        # Connect
        conn = cx_Oracle.connect(creds['User']
                                 , creds['Password']
                                 , creds['Database'])
    except cx_Oracle.DatabaseError:
        raise
        return False
    else:
        return conn


def get_credentials():
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Retrieves credentials from local text file
    Returns    : Dictionary        
    """

    # IPSCredentials file location
    credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
    
    # Create dictionary
    credentials_dict = {}
    
    # Validate file
    if os.path.getsize(credentials_file) == 0:
        # File is empty, return False to indicate failure 
        return False
    
    # Open and read file, and assign to string variable 
    try:
        file_object = open(credentials_file, "r")
    except IOError:
        raise
        return False            
    else:
        credentials_string = file_object.read()  
            
    # Parse string to dictionary
    for line in credentials_string.split('\n'):
        # if not line: break
        pair = line.split(":")
        credentials_dict[pair[0].strip()] = pair[1].strip()

    return credentials_dict


def get_password():
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Retrieves user password for database (Oracle)
                 Data currently retrieved from .txt file.  Process to be determined.
    Returns    : Password (String)
    DEPS       : get_credentials()
    """
    
    pwd = get_credentials()
    return pwd['Password']


def extract_zip(dir_name, file_extension=""):
    """
    Author     : thorne1
    Date       : 24 Nov 2017
    Purpose    : Generic function to extract either a specific file from zip, or entire file
    Params     : dir_name - directory containing .zip file (is this unclear?)
                 file_extension - Specify a file type to extract one file 
                                  (assuming there is only one file type in zip)
                                  or leave empty to extract all
    Returns    : True/False
    """
    
    # Change directory from working directory to directory with files
    try:
        os.chdir(dir_name)
    except WindowsError:
        raise
        return False
                 
    # Find and create zipfile object
    for item in os.listdir(dir_name):
        if item.endswith(".zip"):
            filename = os.path.abspath(item)
            zip_file = zipfile.ZipFile(filename)
    # If file_extension not specified, extract everything
    if file_extension == "":
        zip_file.extractall(dir_name)
        # Return True to indicate success
        return True
    else:
        # Find and extract a specified file
        file_found = False
        for each_file in zip_file.namelist():
            
            if each_file.endswith(file_extension):
                zip_file.extract(each_file, dir_name)
                file_found = True
                # Return True to indicate success
        return file_found
    
    # Clean up
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    zip_file.close()  


def import_csv(filename):
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Generic function to open a CSV   
    Params     : filename - full CSV path 
    Returns    : Dataset (Object)   
    """
    try:
        dataframe = pandas.read_csv(filename)
    except IOError:
        # Raise (unit testing purposes) and return False to indicate failure 
        raise
        return False
    else:
        return dataframe      


def import_SAS(filename):
    """
    Author     : thorne1
    Date       : 23 Nov 2017        
    Purpose    : Generic function to open and read a SAS dataset
    Params     : filename - full path to SAS file
    Returns    : SAS File (object) 
                (Does not return dataframe - dataframes do not include column metadata, i.e Label, Type, Format, etc)   
    https://pypi.python.org/pypi/sas7bdat    
    """
    
    try:
        # Create and return sas7bdat dataframe:
        with SAS7BDAT(filename) as file_object:
            return file_object
    except TypeError:
        # Incorrect file type, return False to indicate failure
        raise
        return False
    except IOError:
        # File not found, return False to indicate failure
        raise
        return False


def check_table(table_name):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to check if table exists   
    Params     : table_name 
    Returns    : True/False (bool)   
    """
    conn = get_oracle_connection()
    cur = conn.cursor()   
     
    sql_query = "SELECT COUNT(*) FROM " + table_name    
            
    try:
        cur.execute(sql_query).fetchone()
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:
        # return True to indicate table does exist
        return True


def drop_table(table_name):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to delete table   
    Params     : table_name 
    Returns    : True/False (bool)   
    """
    conn = get_oracle_connection()
    cur = conn.cursor()       
    sql = "DROP TABLE " + table_name
    cur.execute(sql)
    
    return True
    
    
def delete_from_table(table_name):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to drop contents of table   
    Params     : table_name 
    Returns    : True/False (bool)   
    """
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    sql = "DELETE FROM " + table_name    
    
    cur.execute(sql)
    conn.commit()
    
    return True


def import_traffic_data(filename):
    """
    Author    : thorne1
    Date      : 27 Nov 2017
    Purpose   : Specific function to open a Traffic Data CSV and inserts to Oracle   
    Params    : filename - directory path to CSV
    Returns   : True or False
    REQS      : pip install pandas
    DEPS      : get_oracle_connection()
                get_survey_type()
    """
    try:
        # Attempt to open CSV and convert to dataframe
        dataframe = pandas.read_csv(filename)
    except KeyError:
        raise
        return False
    except IOError:
        # File not found, return False to indicate failure
        raise
        if filename == "":
            print "IOError: Filename not provided."
        else:
            print "IOError: %s does not exist." % (filename)
        return False 
        
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    table_name = "TRAFFIC_DATA"
   
    # Hard-coded variables
    run_id = "IPSSeedRunFR02"       # Primary-key constraint on TRAFFIC_DATA. See RUN table
    data_source_id = {"Sea": 1
                      , "Air": 2
                      , "Tunnel": 3
                      , "Shift": 4
                      , "Non Response": 5
                      , "Unsampled": 6}     # These have been copied from DATA_SOURCE table
    vehicle = {"Sea": 7
                      , "Air": 8
                      , "Tunnel": 9
                      , "Shift": 10
                      , "Non Response": 11
                      , "Unsampled": 12}    # These are made up
    
    survey_type = get_survey_type(filename)    #i.e, "Sea", "Air", "Tunnel", etc
    
    # Create collection of rows
    rows = [list(x) for x in dataframe.values]
    for row in rows:
        row.insert(0,run_id)        # Insert row_id value as first column
        row.append(vehicle[survey_type])    # Insert vehicle value as last column
        row[row.index(survey_type)] = data_source_id[survey_type]   # Using survey_type, replace DATASOURCE values with data_source_id           
        
    # SQL statement to insert collection to table
    sql = ("INSERT INTO " 
           + table_name 
           + """(RUN_ID
           , YEAR, MONTH, DATA_SOURCE_ID
           , PORTROUTE, ARRIVEDEPART, TRAFFICTOTAL
           , PERIODSTART, PERIODEND, AM_PM_NIGHT
           , HAUL, VEHICLE) 
           VALUES(:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)""")
    
    try:
        # Execute SQL statement
        cur.executemany(sql, rows)            
    except cx_Oracle.DatabaseError:
        # Return False to indicate error
        raise
        return False
    else:
        conn.commit()
        return True


def get_data_source_values():
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Specific function to return data_source_names from data_source table  
    Params     : 
    Returns    : data_source_names (Tuple)    
    """
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Execute SQL
    cur.execute("SELECT data_source_name FROM data_source")
    rows = cur.fetchall()
    
    # Create tuple of data source names
    values = []
    for row in rows:
        values.append(row)
    
    return values


def get_survey_type(filename):
    """
    Author    : thorne1
    Date      : 4 Dec 2017
    Purpose   : Specific function to return the correct value for data_source_id from the filename
                i.e     "C:\foo\bar\Sea Traffic Q1 2017.csv"     =    "Sea"
                        "C:\foo\bar\Tunnel Traffic Q1 2017.csv"  =    "Tunnel"
                        "C:\foo\bar\Non Response Q1 2017.csv"     =    "Non Response", etc
    Params    : filename - full path to CSV 
    Returns   : Survey type (string)  
    """

    # 1st - Retrieve data source names
    values = get_data_source_values()
    
    # 2nd - Get survey type from filename
    full_path = filename.split("\\")
    full_filename = full_path[-1].split(" ")
    if full_filename[0] == "Non" and full_filename[1] == "Response":
        survey_type = full_filename[0] + " " + full_filename[1]
    else:
        survey_type = full_filename[0]
    
    # 3rd - Validate survey type from filename against tuple from table 
    if any(survey_type in value for value in values):
        return survey_type
    else:
        return False
    

def create_traffic_data_table():
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Specific SQL query to create traffic_date table
    Params     : 
    Returns    :   
    """
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    table_name = "TRAFFIC_DATA_2"       

    sql = "CREATE TABLE " + table_name + " (RUN_ID varchar2(40), YEAR number(4), MONTH number(2), DATA_SOURCE_ID varchar2(10), PORTROUTE number(4), ARRIVEDEPART number(1), TRAFFICTOTAL number(12,3), PERIODSTART varchar2(10), PERIODEND varchar2(10), AM_PM_NIGHT number(1), HAUL varchar2(2), VEHICLE varchar2(10))"
    
    cur.execute(sql)
    conn.commit()
    
    print "Table should have been created"


def insert_resposne_table(level, err):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Specific SQL query to insert values into response table
    Params     : 
    Returns    :   
    """
    conn = get_oracle_connection()
    cur = conn.cursor()     
    table_name = "response"  
    py_now = datetime.datetime.now()
    now = cx_Oracle.Timestamp(py_now.year, py_now.month, py_now.day, int(py_now.hour), int(py_now.minute), int(py_now.second))
    level_dict = {40: "ERROR", 30: "WARNING", 5:  "SUCCESS", 0: "NOTSET"}
    response_message = ("%s: %s" %(level_dict[level], err)) 
    
    # table_name = 'response' 
    sql = "INSERT INTO " + table_name + "(date_and_time, message_result) VALUES(" + now + ", " + response_message + ")"
    cur.execute(sql)
    conn.commit()
    
    print "Table should have been created"