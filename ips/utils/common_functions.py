"""
Created on 24 Nov 2017

@author: Elinor Thorne
"""

import logging
from typing import Optional
import pandas
import os
import pyodbc
import traceback


def database_logger() -> logging.Logger:
    """
    Author        : Elinor Thorne
    Date          : 5 Jan 2018
    Purpose       : Sets up and returns database logger object
    Parameters    : None
    Returns       : Database logger object
    Requirements  : None
    Dependencies  : social_surveys.setup_logging
    """
    return logging.getLogger(__name__)


def standard_log_message(err_msg: str, current_working_file: str, func_name: str) -> str:
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

    return (err_msg
            + ' - File "' + current_working_file
            + '", in ' + func_name + '()')


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

    try:
        conn = pyodbc.connect(driver="{ODBC Driver 17 for SQL Server}", server=server, database=database,
                              uid=username, pwd=password, autocommit=True)
        return conn
    except Exception as err:
        database_logger().error(err, exc_info = True)
        return False


def drop_table(table_name: str) -> None:
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

    conn = get_sql_connection()

    if conn is None:
        raise ConnectionError("drop_table: Cannot get database connection")

    cur = conn.cursor()

    # Create and execute SQL query
    sql = "DROP TABLE IF EXISTS " + table_name

    try:
        cur.execute(sql)
    except Exception as err:
        database_logger().error(err, exc_info = True)
        raise err


def delete_from_table(table_name: str, condition1: str = None, operator: str = None,
                      condition2: str = None, condition3: str = None) -> None:
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

    conn = get_sql_connection()

    if conn is None:
        raise ConnectionError("delete_from_table: Cannot get database connection")

    cur = conn.cursor()

    # Create and execute SQL query
    if condition1 is None:
        # DELETE FROM table_name
        sql = ("DELETE FROM " + table_name)
    elif condition3 is None:
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
        traceback.print_exc()
        print(err)
    finally:
        conn.close()


def select_data(column_name: str, table_name: str, condition1: str, condition2: str) -> Optional[pandas.DataFrame]:
    """
    Author        : Elinor Thorne
    Date          : 21 Dec 2017
    Purpose       : Uses SQL query to retrieve values from database
    Parameters    : column_name, table_name, condition1, condition2, i.e:
                  : "SELECT column_name FROM table_name WHERE condition1 = condition2" (no 'AND'/'OR' clause)
    Returns       : Data Frame for multiple values, scalar/string for single values
    Requirements  : None
    """

    conn = get_sql_connection()

    if conn is None:
        raise ConnectionError("select_data: Cannot get database connection")

    sql = f"""
        SELECT {column_name} 
        FROM {table_name}
        WHERE {condition1} = '{condition2}'
        """

    try:
        return pandas.read_sql_query(sql, con=conn)
    except Exception as err:
        print(err)
    finally:
        conn.close()

    return None


def get_table_values(table_name: str) -> pandas.DataFrame:
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Extracts a full table into a pandas dataframe
    Params       : table_name - the name of the target table in the sql database.
    Returns      : Dataframe containing the extracted table data.
    Requirements : NA
    Dependencies : NA
    """

    conn = get_sql_connection()

    if conn is None:
        raise ConnectionError("get_table_values: Cannot get database connection")

    try:
        return pandas.read_sql_table(table_name=table_name, con=conn)
        # df.fillna(value=pd.np.nan, inplace=True)
        # return df
    except Exception as err:
        print(err)
    finally:
        conn.close()


def insert_dataframe_into_table(table_name: str, dataframe: pandas.DataFrame) -> Optional[int]:
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Inserts a full dataframe into a SQL table
    Params       : table_name - the name of the target table in the sql database.
                   dataframe - the dataframe to be added to the selected table.
    Returns      : The number of rows added to the database.
    Requirements : NA
    Dependencies : NA
    """

    conn = get_sql_connection()

    if conn is None:
        print("insert_dataframe_into_table: Cannot get database connection")
        return None

    cur = conn.cursor()

    dataframe = dataframe.where((pandas.notnull(dataframe)), None)

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


    # Debugging
    # for rec in rows:
    #    print (rec)

    for row in rows:
        cur.execute(sql, row)

    # Returns number of rows added to table for validation
    return len(rows)


def execute_sql_statement(sql):
    conn = get_sql_connection()

    if conn is None:
        raise ConnectionError("execute_sql_statement: Cannot get database connection")

    cur = conn.cursor()

    try:
        cur.execute(sql)
    except Exception as err:
        print(err)
    finally:
        conn.close()
