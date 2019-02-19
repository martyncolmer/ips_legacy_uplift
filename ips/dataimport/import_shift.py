import pandas as pd

import ips.dataimport.schemas.shift_schema as shift_schema
from ips.utils import common_functions as cf


def import_shift(file_name, file_type, run_id):
    conn = cf.get_sql_connection()
    if conn is None:
        print("import_shift: Cannot get database connection")
        return

    data_schema = shift_schema.get_schema()

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
