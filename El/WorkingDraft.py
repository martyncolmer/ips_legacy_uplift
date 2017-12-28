'''
Created on 24 Nov 2017

El's CommonFunctions working draft - no need to use

@author: thorne1
'''

import os
import zipfile
import cx_Oracle    # pip install this
import pandas as pandas     # pip install this
import json
import logging

import survey_support

from sas7bdat import SAS7BDAT   # pip install this

#set up null logger.  logger.debug()

def validate_file(xfile):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic function to validate file to check existence and size     
    Params     : xfile (file is reserved keyword) - file to validate
    Returns    : True/False (boolean)
    """
   
    if xfile == "":
#        print "IOError: Filename not provided."# If file does not exist OR file is empty return False
        return False
    if not os.path.exists(xfile):
#        print ("%s does not exist. Please check file location." %xfile)
        return False
    if os.path.getsize(xfile) == 0:
#        print ("%s is empty. Please check file." %xfile) 
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
        
    
def get_json_credentials(credentials_file):
    """
    Author     : thorne1
    Date       : 27 Nov 2017
    Purpose    : Retrieves credentials from local text file
    Returns    : Dictionary        
    """
    
    # Create dictionary
    credentials_dict = {}
    
    # Validate file
    if os.path.getsize(credentials_file) == 0:
        # File is empty, return False to indicate failure 
        return False
    
    # Open and read file, and assign to string variable 
    try:
        with open(credentials_file) as json_file:
            credentials_dict = json.load(json_file)
    except IOError:
        raise
            
    return credentials_dict
    

def extract_zip(dir_name, zip_file):    

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


def validate_nan_values(filename):
    df = import_csv(filename)
    
    return df.isnull().values.any()
        
        
def create_table(table_name, column_list):
    """
    Author     : thorne1
    Date       : 20 Dec 2017
    Purpose    : Generic SQL query to create a table
    Params     : table_name = Name of table to create
               : column_list = List of as many column details as required in the following format:
                      FORMAT EXAMPLE:    "COLUMN_NAME type(size)"
                      CODE EXAMPLE:       create_table("TABLE_DATA", ("RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)"))
                                          OR
                                          cols = ("RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)")
                                          create_table("TABLE_DATA", cols)                      
    Returns    : True/False  
    """
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    columns = str(column_list).replace("'","") 
    
    sql = "CREATE TABLE " + table_name + " " + columns    
    cur.execute(sql)
    conn.commit()

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
    Returns    : True/False (bool)   
    """
    conn = get_oracle_connection()
    cur = conn.cursor()   
         
    sql = "SELECT * from USER_TABLES where table_name = '" + table_name + "'"
    
    try:
        result = cur.execute(sql)
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
    conn = get_oracle_connection()
    cur = conn.cursor()       
    sql = "DROP TABLE " + table_name
    
    try:
        result = cur.execute(sql)
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:
        if result == None:
            # return True to indicate table exists
            return True
        else:
            return False
    
    
def delete_from_table(table_name):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to drop entire contents of table   
    Params     : table_name 
    Returns    : True/False (bool)   
    """
    conn = get_oracle_connection()
    cur = conn.cursor() 
    
    sql = "DELETE FROM " + table_name    
    
    cur.execute(sql)
    conn.commit()
    
    return True


def get_values(table_name):
    """
    Author      : thorne1
    Date        : 14 Dec 2017
    Purpose     : Retrieves values from Oracle table   
    Params      : table_name 
    Returns     : list   
    """
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    sql = "SELECT * from " + table_name
    cur.execute(sql)          
    result = cur.fetchall()
    
    return result   


def insert_into_table2(table_name, value_list):
    """
    Author     : thorne1
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
    
    # Confirm table exists
    if check_table(table_name) == False:
        print "Table does not exist"
        return False
    
    # Oracle connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Re-format column_list and value_lists as strings
#    columns = str(column_list).replace("'","")
    vals = str(value_list).replace("'","")
    
    # Retrieve row count prior to inserting
    sql = "SELECT * FROM " + table_name
    original_row_count = cur.execute(sql).fetchall()
    print "original_row_count: %s" %original_row_count
    
    # Create and execute SQL insert query
    sql = "INSERT INTO " + table_name + " VALUES " + vals
    print sql
    
    # sys.exit()

    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:
        conn.commit()
    
    # Validate rows were inserted
    sql = "SELECT * FROM " + table_name
    final_row_count = cur.execute(sql).fetchall()
    print "final_row_count: %s" %final_row_count
    if final_row_count > original_row_count:
        print "Table inserted successfully"
        return True
    else:
        print "Table not inserted"
        return False



def insert_into_table(table_name, column_list, value_list):
    """
    Author     : thorne1
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
    columns = str(column_list).replace("'","")
    values = str(value_list).replace("'","")
    
    # table_name = 'response' 
    sql = "INSERT INTO " + table_name + columns + " VALUES" + values
    cur.execute(sql)
    conn.commit()
    
    # VALIDATE IT DID IT

#if __name__ == '__main__':
##    datasource = "Unsampled"
##    print get_datasource_id(datasource)
#    caa_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017.csv"
#    tun_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"    
#    nr_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response Q1 2017.csv"
#    ps_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible Shifts Q1 2017.csv"
#    sea_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"    
#    uns_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv"    
#
##    Traffic_Data
##    print delete_from_table("TRAFFIC_DATA")    
#    import_traffic_data(sea_filename)
#    #print import_traffic_data(caa_filename)
##    print import_traffic_data(tun_filename)
##
###    Shift_Data
##    print delete_from_table("SHIFT_DATA")
#    import_traffic_data(ps_filename)
##
###    Non_Response_Data
##    print delete_from_table("NON_RESPONSE_DATA")
#    import_traffic_data(nr_filename)
#
##    Unsampled_OOH_Data
##    print delete_from_table("UNSAMPLED_OOH_DATA")
#    import_traffic_data(uns_filename)
#
##    print drop_table("Els_Test_Table2")
    #print drop_table("Els_Test_Table")
    #print create_table("Els_Test_Table","RUN_ID varchar2(40)", "YEAR number(4)", "MONTH number(2)")
    #print check_table("Els_Test_Table")
    

def select_data(column_name, table_name, condition1, condition2):
    """
    Author     : thorne1
    Date       : 21 Dec 2017
    Purpose    : Uses SQL query to retrieve value from Oracle table  
    Params     : column_name, table_name, condition1, condition2, i.e:
               : "SELECT column_name FROM table_name WHERE condition1 = condition2" (no 'AND'/'OR' clause)
    Returns    : Result (String)
    """
    
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    # Create SQL statement
    sql = "SELECT " + column_name + " FROM " + table_name + " WHERE " + condition1 + " = '" + condition2 + "'"

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
        
        return result
    

def get_row_count(table_name):
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    sql = "SELECT * FROM " + table_name
    cur.execute(sql).fetchall()
    print cur.rowcount
    

def get_column_names(table_name):
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    sql = "SELECT * FROM " + table_name
    cur.execute(sql)
    column_descriptions = cur.description
    
    column_names = []
    for every_item in column_descriptions:
        column_names.append(every_item[0]) 
        
    print type(column_names)


def get_table_to_dataframe(table_name):
    # Connection variables
    conn = get_oracle_connection()
    cur = conn.cursor()
    
    sql = "SELECT * FROM " + table_name
    cur.execute(sql)
    dataframe = pandas.read_sql(sql, conn)
    
    print dataframe
    

table_name = "DATA_SOURCE"
get_table_to_dataframe(table_name)