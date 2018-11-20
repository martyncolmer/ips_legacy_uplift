import pandas as pd
import logging
import os
import inspect

# import survey_support as ss
from main.io import CommonFunctions as cf


def import_traffic_data(run_id, filename):
    """
    Author        : Elinor Thorne
    Date          : 27 Nov 2017
    Purpose       : Imports CSV (Sea, CAA, Tunnel Traffic, Possible Shifts,
                    Non Response or Unsampled) and inserts to Oracle
    Parameters    : filename - full directory path to CSV
    Returns       : True or False
    Requirements  : pip install pandas
    Dependencies  : CommonFunctions.import_csv()
                    CommonFunctions.validate_csv()
                    CommonFunctions.get_sql_connection()
                    CommonFunctions.select_data()
    """

    # Connection variables
    conn = cf.get_sql_connection()
    cur = conn.cursor()


    # Convert CSV to dataframe and stage
    dataframe = pd.read_csv(filename)
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)
    dataframe = dataframe.fillna('')

    # replace "REGION" values with 0 if not an expected value
    if "REGION" in dataframe.columns:
        dataframe['REGION'].replace(['None', "", ".", 'nan'], 0, inplace=True)

    # Get datasource values and replace with new datasource_id
    datasource = dataframe.at[1, 'DATA_SOURCE_ID']
    datasource_id = cf.select_data("DATA_SOURCE_ID"
                                   , "DATA_SOURCE"
                                   , "DATA_SOURCE_NAME"
                                   , datasource)
    dataframe['DATA_SOURCE_ID'].replace([datasource], datasource_id, inplace=True)

    # Select appropriate table name (value) based on data source (key)
    table_name_dict = {"Sea": "TRAFFIC_DATA",
                       "Air": "TRAFFIC_DATA",
                       "Tunnel": "TRAFFIC_DATA",
                       "Shift": "SHIFT_DATA",
                       "Non Response": "NON_RESPONSE_DATA",
                       "Unsampled": "UNSAMPLED_OOH_DATA"}
    table_name = table_name_dict[datasource]

    # Cleansing
    if table_name == 'TRAFFIC_DATA':
        dataframe["VEHICLE"] = 0
        dataframe['AM_PM_NIGHT'] = dataframe[['AM_PM_NIGHT']].convert_objects(convert_numeric=True).replace("", float('nan'))

        sql = """
        DELETE FROM [TRAFFIC_DATA]
        WHERE RUN_ID = '{}'
        AND DATA_SOURCE_ID = '{}'
        """.format(run_id, datasource_id)
    else:
        sql = """
        DELETE FROM [{}]
        WHERE RUN_ID = '{}'
        """.format(table_name, run_id)

    cur.execute(sql)

    # Insert dataframe to table
    cf.insert_dataframe_into_table(table_name, dataframe)



if __name__ == '__main__':
    run_id = 'el-test'

    dir = r'C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\main\Dec\import_data'
    shift_data_path = dir + r'\Poss shifts Dec 2017.csv'
    nr_data_path = dir + r'\Dec17_NR.csv'
    unsampled_data_path = dir + r'\Unsampled Traffic Dec 2017.csv'
    sea_data_path = dir + r'\Sea Traffic Dec 2017.csv'
    tunnel_data_path = dir + r'\Tunnel Traffic Dec 2017.csv'
    air_data = dir + r'\Air Sheet Dec 2017 VBA.csv'

    import_traffic_data(run_id, shift_data_path)
    import_traffic_data(run_id, nr_data_path)
    import_traffic_data(run_id, unsampled_data_path)
    import_traffic_data(run_id, sea_data_path)
    import_traffic_data(run_id, tunnel_data_path)
    import_traffic_data(run_id, air_data)