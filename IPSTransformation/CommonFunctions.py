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
# from LogDBHandler import IPS_Log_Handler

#import LogDBHandler

def get_credentials():
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Retrieves credentials from local text file
    Returns    : Dictionary        
    """

    # IPSCredentials file location
    credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
    
    # Validate file
    if os.path.getsize(credentials_file) == 0:
        print "File is empty."
        return False
    
    # Open and read file, and assign to string variable 
    try:
        file_object = open(credentials_file, "r")
    except IOError as err:
        raise err
        return False            
    else:
        credentials_string = file_object.read()   
        
    # Create dictionary
    credentials_dict = {}
    
    # Parse string to dictionary
    for line in credentials_string.split('\n'):
        # if not line: break
        pair = line.split(":")
        credentials_dict[pair[0].strip()] = pair[1].strip()

    return credentials_dict


def get_oracle_connection():
    """
    Author     : mahont1 & thorne1
    Date       : 27 Nov 2017
    Purpose    : Connect to Oracle database and return cursor object
    Returns    : Connection (Object) 
                 (cannot return cursor object as DDL statements are implicitly committed
                 whereas DML statements are not)
    REQS       : pip install cx_Oracle 
                 32-bit Oracle Client required
    DEPS       : get_credentials()
    """
    
    # Retrieve credentials dictionary 
    creds = get_credentials()
    
    try:
        # Connect
        conn = cx_Oracle.connect(creds['User']
                                 , creds['Password']
                                 , creds['Database'])
    except cx_Oracle.DatabaseError as err:
        print err
        return False
    else:
        return conn 
        
        
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
    Purpose    : Extracts either a specific file from zip, or entire file
    Params     : dir_name - directory containing .zip file (is this unclear?)
                 file_extension - Specify a file type to extract one file 
                                  (assuming there is only one file type in zip)
                                  or leave empty to extract all
    Returns    : True/False
    """
    
    # Change directory from working directory to directory with files
    try:
        os.chdir(dir_name)
    except WindowsError as err:
        raise
        print err
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


def import_traffic_data(filename):
    """
    Author    : thorne1
    Date      : 27 Nov 2017
    Purpose   : Opens a Traffic Data CSV and inserts to Oracle   
    Params    : filename - directory path to CSV
    Returns   : True or False
    REQS      : pip install pandas
    DEPS      : get_oracle_connection()
                get_survey_type()
    """
    try:
        # Attempt to open CSV and convert to dataframe
        dataframe = pandas.read_csv(filename)
    except IOError:
        # File not found, return False to indicate failure
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
    except cx_Oracle.DatabaseError as err:
        # Return False to indicate error
        print err
        return False
    else:
        conn.commit()
        return True
    
    
def get_survey_type(filename):
    """
    Author    : thorne1
    Date      : 4 Dec 2017
    Purpose   : Returns the correct value for data_source_id from the filename
                i.e     "C:\foo\bar\Sea Traffic Q1 2017.csv"     =    "Sea"
                        "C:\foo\bar\Tunnel Traffic Q1 2017.csv"  =    "Tunnel", etc
    Params    : filename - directory path to CSV 
    Returns   : Survey type (string)  
    """

    full_path = filename.split("\\")
    full_filename = full_path[-1].split(" ")
    if full_filename[0] == "Non" and full_filename[1] == "Response":
        survey_type = full_filename[0] + " " + full_filename[1]
    else:
        survey_type = full_filename[0]
    
    return survey_type


def import_SAS(filename):
    """
    Author     : thorne1
    Date       : 23 Nov 2017        
    Purpose    : Opens and reads a SAS dataset
    Params     : filename - directory path to SAS file
    Returns    : SAS File (object) or False   
    https://pypi.python.org/pypi/sas7bdat    
    """
    
    try:
        # Create and return sas7bdat dataframe:
        with SAS7BDAT(filename) as file_object:
            return file_object
    except TypeError:
        # File not found, return False to indicate failure
        raise
        print "%s is not a SAS file" % (filename)
    return False





def check_table(table_name):
    conn = get_oracle_connection()
    cur = conn.cursor()   
     
    sql_query = "SELECT COUNT(*) FROM " + table_name    
            
    try:
        cur.execute(sql_query).fetchone()
    except cx_Oracle.DatabaseError:
        print "Table does not exist"
    else:
        print "Table exists"


def create_TRAFFIC_DATA_table():
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    table_name = "TRAFFIC_DATA_2"       

    sql = "CREATE TABLE " + table_name + " (RUN_ID varchar2(40), YEAR number(4), MONTH number(2), DATA_SOURCE_ID varchar2(10), PORTROUTE number(4), ARRIVEDEPART number(1), TRAFFICTOTAL number(12,3), PERIODSTART varchar2(10), PERIODEND varchar2(10), AM_PM_NIGHT number(1), HAUL varchar2(2), VEHICLE varchar2(10))"
    
    cur.execute(sql)
    conn.commit()
    
    print "Table should have been created"


def drop_table(table_name):
    conn = get_oracle_connection()
    cur = conn.cursor()       
    sql = "DROP TABLE " + table_name
    cur.execute(sql)
    
    print "Table should have been deleted"
    
    
def insert_RESPONSE_table(level, err):
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
    

def delete_from_table(table_name):
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    sql = "DELETE FROM " + table_name    
    
    cur.execute(sql)
    conn.commit()