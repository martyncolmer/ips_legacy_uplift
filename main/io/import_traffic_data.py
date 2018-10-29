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



# if __name__ == '__main__':
#     run_id = 'el-test'
#
#     dir = r'C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\main\Dec\import_data'
#     shift_data_path = dir + r'\Poss shifts Dec 2017.csv'
#     nr_data_path = dir + r'\Dec17_NR.csv'
#     unsampled_data_path = dir + r'\Unsampled Traffic Dec 2017.csv'
#     sea_data_path = dir + r'\Sea Traffic Dec 2017.csv'
#     tunnel_data_path = dir + r'\Tunnel Traffic Dec 2017.csv'
#     air_data = dir + r'\Air Sheet Dec 2017 VBA.csv'
#
#     import_traffic_data(run_id, shift_data_path)
#     import_traffic_data(run_id, nr_data_path)
#     import_traffic_data(run_id, unsampled_data_path)
#     import_traffic_data(run_id, sea_data_path)
#     import_traffic_data(run_id, tunnel_data_path)
#     import_traffic_data(run_id, air_data)

# import pandas
# import logging
# import os
# import inspect
#
# # import survey_support as ss
# from main.io import CommonFunctions as cf
#
#
# def import_traffic_data(run_id, filename):
#     """
#     Author        : Elinor Thorne
#     Date          : 27 Nov 2017
#     Purpose       : Imports CSV (Sea, CAA, Tunnel Traffic, Possible Shifts,
#                     Non Response or Unsampled) and inserts to Oracle
#     Parameters    : filename - full directory path to CSV
#     Returns       : True or False
#     Requirements  : pip install pandas
#     Dependencies  : CommonFunctions.import_csv()
#                     CommonFunctions.validate_csv()
#                     CommonFunctions.get_sql_connection()
#                     CommonFunctions.select_data()
#     """
#
#     # run_id currently hard-coded due to
#     # primary-key constraint on TRAFFIC_DATA (see RUN table)
#     # THIS WILL NEED TO BE AMENDED ONCE run_id PROCESS IMPLEMENTED
#     if(run_id == ''):
#         run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'
#
#     # 0 = frame object, 1 = filename, 3 = function name.
#     # See 28.13.4. in https://docs.python.org/2/library/inspect.html
#     current_working_file = str(inspect.stack()[0][1])
#     function_name = str(inspect.stack()[0][3])
#
#     # Database logger setup
#     # ss.setup_logging(os.path.dirname(os.getcwd())
#     #                  + "\\IPS_Logger\\IPS_logging_config_debug.json")
#     logger = logging.getLogger(__name__)
#
#     # Import CSV and validate
#     if cf.validate_file(filename, current_working_file, function_name) == True:
#         try:
#             pandas.read_csv(filename)
#         except Exception as err:
#             # return False to indicate failure
#             logger.error(err, exc_info = True)
#             return False
#         else:
#             dataframe = cf.import_csv(filename)
#             if dataframe.empty:
#                 return False
#     else:
#         # File validation failed
#         return False
#
#     # Change column names to upper case
#     dataframe.columns = dataframe.columns.str.upper()
#
#     # remove whitespaces within column names
#     dataframe.columns = dataframe.columns.str.replace(' ', '')
#
#     # insert "ROW_ID" column to dataframe
#     dataframe["RUN_ID"] = pandas.Series(run_id, index = dataframe.index)
#
#     # replace "DATASOURCE" column name with "DATA_SOURCE_ID"
#     dataframe.rename(columns={"DATASOURCE":"DATA_SOURCE_ID"}, inplace = True)
#
#     # replace Nan values with empty string
#     dataframe = dataframe.fillna('')
#
#     # replace "REGION" values with 0 if not an expected value
#     if "REGION" in dataframe.columns:
#         dataframe['REGION'].replace(['None',"",".",'nan'],0,inplace=True)
#
#     # Get datasource values i.e, "Sea", "Air", "Tunnel", etc
#     records_to_return = 1
#     datasource = dataframe.at[records_to_return, 'DATA_SOURCE_ID']
#
#     # Get datasource id i.e 1, 2, 3, etc as per DATA_SOURCE table
#     # and replace current datasource values with new datasource_id
#     datasource_id = cf.select_data("DATA_SOURCE_ID"
#                                    , "DATA_SOURCE"
#                                    , "DATA_SOURCE_NAME"
#                                    , datasource)
#     dataframe['DATA_SOURCE_ID'].replace([datasource],datasource_id,inplace=True)
#
#     # Oracle connection variables
#     conn = cf.get_sql_connection()
#     cur = conn.cursor()
#
#     # Key = datasource / Value = table name
#     table_name_dict = {"Sea": "TRAFFIC_DATA"
#                   , "Air": "TRAFFIC_DATA"
#                   , "Tunnel": "TRAFFIC_DATA"
#                   , "Shift": "SHIFT_DATA"
#                   , "Non Response": "NON_RESPONSE_DATA"
#                   , "Unsampled": "UNSAMPLED_OOH_DATA"}
#     # Select appropriate table name based on datasource
#     table_name = table_name_dict[datasource]
#
#     # Get the row count from the table
#     sql = "SELECT * FROM [dbo].[" + table_name +"] where RUN_ID = '" + run_id + "'"
#     cur.execute(sql)
#     row_count = len(cur.fetchall())
#
#     if row_count > 0:
#         cf.delete_from_table(table_name, "RUN_ID", "=", run_id)
#
#     # If table_name == "TRAFFIC_DATA" insert "VEHICLE" column with empty values
#     if table_name == "TRAFFIC_DATA":
#         # Traffic data needs some columns set up and for nulls to be added as numeric values (else we get a conversion error in SQL)
#         dataframe["VEHICLE"] = 0
#         dataframe['AM_PM_NIGHT'] = dataframe[['AM_PM_NIGHT']].convert_objects(convert_numeric=True).replace("", float('nan'))
#
#
#     # Insert dataframe to table
#     cf.insert_dataframe_into_table(table_name, dataframe)
#
#     # Commit to audit log
#     action = "Upload"
#     process_object = "TrafficData"
#     audit_msg = "Upload complete for: %s()" %function_name
#     cf.commit_to_audit_log(action, process_object, audit_msg)