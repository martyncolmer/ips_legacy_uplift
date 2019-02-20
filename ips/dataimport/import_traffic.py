import pandas as pd

import ips.dataimport.schemas.traffic_schema as traffic_schema
from ips.utils import common_functions as cf


def import_traffic(file_name, file_type, run_id):

    data_schema = traffic_schema.get_schema()

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
                    DELETE FROM TRAFFIC_DATA
                    WHERE RUN_ID = '{run_id}'
                    AND DATA_SOURCE_ID = '{datasource_id}'"""

    try:
        cf.execute_sql_statement(sql)
        cf.insert_dataframe_into_table('TRAFFIC_DATA', dataframe)
    except Exception as err:
        print(err)
        return None
