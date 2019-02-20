import pandas as pd

import ips.dataimport.schemas.non_response_schema as non_response_schema
from ips.utils import common_functions as cf


def import_non_response(file_name, file_type, run_id):

    data_schema = non_response_schema.get_schema()
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
        cf.execute_sql_statement(sql)
        cf.insert_dataframe_into_table('NON_RESPONSE_DATA', dataframe)
    except Exception as err:
        print(err)
        return None
