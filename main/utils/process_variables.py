from main.io import CommonFunctions as cf
import pandas as pd
import sys
import random
import numpy as np
import math
from sas7bdat import SAS7BDAT
random.seed(123456)

input_file = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\data_00.sas7bdat'
output_file1 = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\TestOutput.csv'
output_file2 = r'Z:\CASPA\IPS\Testing\ProcessVariables\PythonOutputs\TestOutput_modified.csv'


def modify_values(row, pvs, dataset):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Applies the PV rules to the specified dataframe on a row by row basis.
    Parameters   : row - the row of a dataframe passed to the function through the 'apply' statement called
                   pvs - a collection of pv names and statements to be applied to the dataframe's rows.
                   dataset -  and identifier used in the executed pv statements.
    Returns      : a modified row to be reinserted into the dataframe.
    Requirements : this function must be called through a pandas apply statement.
    Dependencies : NA
    """

    for pv in pvs:
        code = str(pv[1])
        try:
            exec code
        except KeyError:
            print("Key Not Found")
    
    row['SHIFT_PORT_GRP_PV'] = row['SHIFT_PORT_GRP_PV'][:10]        
    
    return row


def get_pvs(conn=None):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Extracts the PV data from the process_variables table.
    Parameters   : conn - a connection object linking  the database.
    Returns      : a collection of pv names and statements
    Requirements : NA
    Dependencies : NA
    """

    # Connect to the database
    if conn is None:
        conn = cf.get_oracle_connection()

    # Create a cursor object from the connection
    cur = conn.cursor()

    # Specify the sql query for retrieving the process variable statements from the database
    sql = """SELECT 
                PROCVAR_NAME,PROCVAR_RULE 
             FROM 
                SAS_PROCESS_VARIABLE"""

    # Execute the sql query
    cur.execute(sql)

    # Return the pv statements obtained from the sql query
    return cur.fetchall()

    
def process(in_table_name, out_table_name, in_id, dataset):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Runs the process variables step of the IPS calculation process.
    Parameters   : in_table_name - the table where the data is coming from.
                   out_table_name - the destination table where the modified data will be sent.
                   in_id - the column id used in the output dataset (this is used when the data is merged into the main
                           table later.
                   dataset - an identifier for the dataset currently being processed.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Ensure the input table name is capitalised
    in_table_name = in_table_name.upper()

    # Extract the table's content into a local dataframe
    df_data = cf.get_table_values(in_table_name)

    # Fill nan values (Is this needed?)
    df_data.fillna(value=np.NaN, inplace=True)

    # Get the process variable statements
    process_variables = get_pvs()

    # TESTING - Output file before applying process variables for comparison
    df_data.to_csv(output_file1)

    # Apply process variables
    df_data = df_data.apply(modify_values, axis=1, args=(process_variables, dataset))

    # TESTING - Output file after applying process variables for comparison
    df_data.to_csv(output_file2)

    # Create a list to hold the PV column names
    updated_columns = []

    # Loop through the pv's
    for pv in process_variables:
        updated_columns.append(pv[0].upper())

    # Generate a column list from the in_id column and the pvs for the current run
    columns = [in_id] + updated_columns

    # Create a new dataframe from the modified data using the columns specified
    df_out = df_data[columns]

    # Insert the dataframe to the output table
    cf.insert_into_table_many(out_table_name, df_out)


if __name__ == '__main__':
    # Run the process variables step
    process(in_table_name='SAS_SURVEY_SUBSAMPLE',
            out_table_name='SAS_SHIFT_SPV',
            in_id='SERIAL',
            dataset='survey')
