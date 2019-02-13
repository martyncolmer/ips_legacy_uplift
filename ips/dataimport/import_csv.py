import pandas as pd

import ips.dataimport.schemas.non_response_schema as non_response_schema
import ips.dataimport.schemas.shift_schema as shift_schema
import ips.dataimport.schemas.traffic_schema as traffic_schema
import ips.dataimport.schemas.unsampled_schema as unsampled_schema
from ips.dataimport import CSVType
from ips.utils import common_functions as cf


def import_csv(file_type, run_id, file_name):
    data_schema = None

    def import_traffic_file():

        conn = cf.get_sql_connection()
        if conn is None:
            print("Cannot get database connection")
            return

        # Convert CSV to dataframe and stage
        dataframe = pd.read_csv(file_name, engine="python", dtype=data_schema)

        dataframe.columns = dataframe.columns.str.upper()
        dataframe.columns = dataframe.columns.str.replace(' ', '')
        dataframe["RUN_ID"] = run_id
        dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

        datasource_type = file_type.name
        datasource_id = file_type.value

        datasource_id = datasource_id
        dataframe['DATA_SOURCE_ID'].replace([datasource_type], datasource_id, inplace=True)

        sql = f"""
                    DELETE FROM [TRAFFIC_DATA]
                    WHERE RUN_ID = '{run_id}'
                    AND DATA_SOURCE_ID = '{datasource_id}'"""

        try:
            conn.engine.execute(sql)
            cf.insert_dataframe_into_table('TRAFFIC_DATA', dataframe)
        except Exception as err:
            print(err)
            return None

    def import_shift_file():

        conn = cf.get_sql_connection()
        if conn is None:
            print("Cannot get database connection")
            return

        # Convert CSV to dataframe and stage
        dataframe = pd.read_csv(file_name, engine="python", dtype=data_schema)

        dataframe.columns = dataframe.columns.str.upper()
        dataframe.columns = dataframe.columns.str.replace(' ', '')
        dataframe["RUN_ID"] = run_id
        dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

        datasource_type = file_type.name
        datasource_id = file_type.value

        datasource_id = datasource_id
        dataframe['DATA_SOURCE_ID'].replace([datasource_type], datasource_id, inplace=True)

        sql = f"""
        DELETE FROM SHIFT_DATA
        WHERE RUN_ID = '{run_id}'
        """

        try:
            conn.engine.execute(sql)
            cf.insert_dataframe_into_table('SHIFT_DATA', dataframe)
        except Exception as err:
            print(err)
            return None

    def import_nonresponse_file():

        conn = cf.get_sql_connection()
        if conn is None:
            print("Cannot get database connection")
            return

        # Convert CSV to dataframe and stage
        dataframe = pd.read_csv(file_name, engine="python", dtype=data_schema)

        dataframe.columns = dataframe.columns.str.upper()
        dataframe.columns = dataframe.columns.str.replace(' ', '')
        dataframe["RUN_ID"] = run_id
        dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

        datasource_id = file_type.value

        datasource_id = datasource_id
        dataframe['DATA_SOURCE_ID'].replace(['Non Response'], datasource_id, inplace=True)

        sql = f"""
            DELETE FROM NON_RESPONSE_DATA
            WHERE RUN_ID = '{run_id}'
            """

        try:
            conn.engine.execute(sql)
            cf.insert_dataframe_into_table('NON_RESPONSE_DATA', dataframe)
        except Exception as err:
            print(err)
            return None

    def import_unsampled_file():

        conn = cf.get_sql_connection()
        if conn is None:
            print("Cannot get database connection")
            return

        # Convert CSV to dataframe and stage
        dataframe = pd.read_csv(file_name, engine="python", dtype=data_schema)

        dataframe.columns = dataframe.columns.str.upper()
        dataframe.columns = dataframe.columns.str.replace(' ', '')
        dataframe["RUN_ID"] = run_id
        dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

        # replace "REGION" values with 0 if not an expected value
        dataframe['REGION'].replace(['None', "", ".", 'nan'], 0, inplace=True)

        datasource_id = file_type.value

        datasource_id = datasource_id
        dataframe['DATA_SOURCE_ID'].replace(['Unsampled'], datasource_id, inplace=True)

        sql = f"""
                DELETE FROM NON_RESPONSE_DATA
                WHERE RUN_ID = '{run_id}'
                """

        try:
            conn.engine.execute(sql)
            cf.insert_dataframe_into_table('UNSAMPLED_OOH_DATA', dataframe)
        except Exception as err:
            print(err)
            return None

    if file_type == CSVType.Sea:
        data_schema = traffic_schema.get_schema()
        return import_traffic_file

    if file_type == CSVType.Air:
        data_schema = traffic_schema.get_schema()
        return import_traffic_file

    if file_type == CSVType.Tunnel:
        data_schema = traffic_schema.get_schema()
        return import_traffic_file

    if file_type == CSVType.Shift:
        data_schema = shift_schema.get_schema()
        return import_shift_file

    if file_type == CSVType.NonResponse:
        data_schema = non_response_schema.get_schema()
        return import_nonresponse_file

    if file_type == CSVType.Unsampled:
        data_schema = unsampled_schema.get_schema()
        return import_unsampled_file

    return None
