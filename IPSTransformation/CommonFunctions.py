'''
Created on 24 Nov 2017

@author: thorne1
'''

import os
import zipfile
import cx_Oracle    # pip install this
import pandas as pandas     # pip install this
import datetime
import numpy as np
import sys
import json

from sas7bdat import SAS7BDAT   # pip install this

def validate_file(xfile):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic function to validate file to check existence and size     
    Params     : xfile (file is reserved keyword) - file to validate
    Returns    : True/False (boolean)
    """
   
    if xfile == "":
        # If file name not given
        return False
    if not os.path.exists(xfile):
        # If file does not exist
        return False
    if os.path.getsize(xfile) == 0:
        # If file is empty 
        return False
    else:
        return True         


def get_oracle_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"):
        """
        Author     : mahont1 & thorne1
        Date       : 27 Nov 2017
        Purpose    : Generic function to connect to Oracle database and return connection object
        Returns    : Connection (Object) 
                     (cannot return cursor object as DDL statements are implicitly committed
                     whereas DML statements are not)
        Params     : credentials_file is set to default location unless user needs to point elsewhere
        REQS       : pip install cx_Oracle 
                     32-bit Oracle Client required
        DEPS       : get_credentials()
        """
        
        creds = get_credentials(credentials_file)
    
        try:
            # Connect
            conn = cx_Oracle.connect(creds['User']
                                     , creds['Password']
                                     , creds['Database'])
        except TypeError:
            raise
            return False        
        except cx_Oracle.DatabaseError:
            raise
            return False
        else:
            return conn
        
    
def get_credentials(credentials_file):
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Retrieves credentials from local text file
    Returns    : Dictionary        
    """
    
    if validate_file(credentials_file):
        credentials_dict = {}
        
        try:
            file_object = open(credentials_file, "r")
        except IOError:
            raise 
            return False
        else:
            credentials_string = file_object.read()
            
            for line in credentials_string.split('\n'):
                pair = line.split(":")
                credentials_dict[pair[0].strip()] = pair[1].strip()
            
            return credentials_dict
    

def extract_zip(dir_name, zip_file):    

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
        
        return file_found
                
        os.chdir(os.path.dirname(os.path.realpath(__file__)))


def import_csv(filename):
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Generic function to open a CSV   
    Params     : filename - full CSV path 
    Returns    : Dataset (Object)   
    """
    
    if validate_file(filename):
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
    
    
    if validate_file(filename):
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


def create_table(table_name, column_list):
    """
    Author     : thorne1
    Date       : 20 Dec 2017
    Purpose    : Uses SQL query to create a table
    Params     : table_name - name of table to create
               : column_list - List of as many column details as required in the following format:
                      FORMAT EXAMPLE:    "COLUMN_NAME type(size)"
                      CODE EXAMPLE:       create_table("TABLE_DATA", ("RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)"))
                                          OR
                                          cols = ("RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)")
                                          create_table("TABLE_DATA", cols)                      
    Returns    : True/False  
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
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
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
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to check if table exists   
    Params     : table_name 
    Returns    : True - Table exists / False - Table does not exist (bool)   
    """
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()   
     
    # Create and execute SQL query
    sql = "SELECT * from USER_TABLES where table_name = '" + table_name + "'"
    
    try:
        cur.execute(sql)
        result = cur.fetchone()
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:
        if result != None:
            # return True to indicate table exists
            return True
        else:
            return False


def drop_table(table_name):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to delete table   
    Params     : table_name 
    Returns    : True/False (bool)   
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
        # print result
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:
        if check_table(table_name) == True:
            # return False to indicate table still exists
            return False
        else:
            conn.commit()
            return True
    
    
def delete_from_table(table_name):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to drop contents of table   
    Params     : table_name 
    Returns    : True/False (bool)   
    """
    # Confirm table exists
    if check_table(table_name) == False:
        return False    
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    # Create and execute SQL query
    sql = "DELETE FROM " + table_name    
    
    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:   
        conn.commit()
        
    sql = "SELECT * FROM " + table_name
    result = cur.execute(sql).fetchall()
    
    if result != []:
        return False
    else:
        return True


def select_data(column_name, table_name, condition1, condition2):
    """
    Author     : thorne1
    Date       : 21 Dec 2017
    Purpose    : Uses SQL query to retrieve value from Oracle table  
    Params     : column_name, table_name, condition1, condition2, i.e:
               : "SELECT column_name FROM table_name WHERE condition1 = condition2" (no 'AND'/'OR' clause)
    Returns    : Result (String)
    """
    
    # Validation
    dataframe = get_table_to_dataframe(table_name)
    
    if column_name not in dataframe.columns.values:
        return False    
    
    if check_table(table_name) == False:
        return False
    
    if condition1 not in dataframe.values:
        return False
    
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
    except cx_Oracle.DatabaseError:
        # Return False to indicate error
        raise
        return False
    else:
        val = cur.fetchone()
        result = str(val).strip("(,)")
        
    if result == 'None':
        print "Query failed to return result. Please review and try again."
        return False
    else:
        return result


def ips_error_check():
    pass    
    

def commit_ips_response():
    pass
    
    
def unload_parameters(id = False):
    
    """
    Author     : Thomas Mahoney
    Date       : 19 Dec 2017
    Purpose    : Extracts a list of parameters from oracle to be used in the parent process.      
    Params     : id - the identifier used to extract specific parameter sets.
    Returns    : A dictionary of parameters
    """
   
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    if id == False:
        cur.execute("select max(PARAMETER_SET_ID) from SAS_PARAMETERS")
        id = cur.fetchone()[0]
        
    print(id)
    
    cur.execute("select PARAMETER_NAME, PARAMETER_VALUE from SAS_PARAMETERS where PARAMETER_SET_ID = " + str(id))
    results = cur.fetchall()
    tempDict = {}
    for set in results:
        tempDict[set[0].upper()] = set[1]
    
    return tempDict


def get_column_names(table_name):
    """
    Author     : thorne1
    Date       : 27 Dec 2017
    Purpose    : Retrieve column names from Oracle table    
    Params     : table_name - Name of table 
    Returns    : list of column names  
    """
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Create and execute SQL query
    sql = "SELECT * FROM " + table_name
    cur.execute(sql)
    column_descriptions = cur.description
    
    # Create list of column names
    column_names = []
    for every_item in column_descriptions:
        column_names.append(every_item[0]) 
        
    return column_names


def get_table_to_dataframe(table_name):
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Create and execute SQL query to dataframe
    sql = "SELECT * FROM " + table_name
    cur.execute(sql)
    dataframe = pandas.read_sql(sql, conn)
    
    return dataframe



if __name__ == "__main__":
    pass
#    table_name = "TRAFFIC_DATA"
#    print drop_table(table_name)
#    column_list = ("RUN_ID varchar2(40) NOT NULL"
#                   ,"YEAR number(4) NOT NULL"
#                   , "MONTH number(2) NOT NULL"
#                   , "DATA_SOURCE_ID number(10) NOT NULL"
#                   , "PORTROUTE number(4)"
#                   , "ARRIVEDEPART number(1)"
#                   , "TRAFFICTOTAL number(12,3) NOT NULL"
#                   , "PERIODSTART varchar2(10)"
#                   , "PERIODEND varchar2(10)"
#                   , "AM_PM_NIGHT number(1)"
#                   , "HAUL varchar2(2)"
#                   , "VEHICLE number(1)")
#    print create_table(table_name, column_list)