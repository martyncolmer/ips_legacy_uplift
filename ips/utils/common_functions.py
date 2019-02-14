"""
Created on 24 Nov 2017

@author: Elinor Thorne
"""

import logging
from typing import Optional
import pandas
import pyodbc
import os
import sqlalchemy
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
    # Database logger setup
    # ss.setup_logging(os.path.dirname(os.getcwd())
    #                  + "\\IPS_Logger\\IPS_logging_config_debug.json")
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


def get_sql_connection() -> Optional[sqlalchemy.engine.base.Engine]:
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
        # conn = sqlalchemy.create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}")
        return sqlalchemy.create_engine\
            (f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

        # conn = pyodbc.connect(driver="{ODBC Driver 17 for SQL Server}", server=server,
        #                       database=database, uid=username, pwd=password,
        #                       autocommit=True, p_str=None)
        # conn: Connection = sqlite3.connect("../data/ips.db")
    except Exception as err:
        # print("computer says no")
        database_logger().error(err, exc_info=True)

    return None


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
        print("Cannot get database connection")
        return None

    # Create and execute SQL query
    sql = "DROP TABLE " + table_name

    try:
        conn.engine.execute(sql)
    except Exception as err:
        print(err)


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
        print("Cannot get database connection")
        return None

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
        conn.engine.execute(sql)
    except Exception as err:
        traceback.print_exc()
        print(err)


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
        print("Cannot get database connection")
        return None

    sql = f"""
        SELECT {column_name} 
        FROM {table_name}
        WHERE {condition1} = '{condition2}'
        """

    try:
        res = conn.engine.execute(sql)
        df = pandas.DataFrame(res.fetchall())
        df.columns = res.keys()
        return df
    except Exception as err:
        print(err)

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
        print("Cannot get database connection")
        return pandas.DataFrame()

    sql = "SELECT * from " + table_name

    try:
        res = conn.engine.execute(sql)
        df = pandas.DataFrame(res.fetchall())
        df.columns = res.keys()
        return df
    except Exception as err:
        print(err)


def insert_dataframe_into_table(table_name: str, dataframe: pandas.DataFrame) -> None:
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

    eng = get_sql_connection()

    if eng is None:
        print("Cannot get database connection")
        return None

    with eng.connect() as con:

        dataframe = dataframe.where((pandas.notnull(dataframe)), None)
        dataframe.columns = dataframe.columns.astype(str)
        dataframe.columns = dataframe.columns.str.upper()

        try:
            dataframe.to_sql(table_name, con=con, if_exists='append',
                             chunksize=1000, index=False)
        except Exception as err:
            print(err)
            return None

