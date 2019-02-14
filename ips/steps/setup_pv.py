
import pandas as pd

import ips.dataimport.schemas.traffic_schema as traffic_schema
from ips.utils import common_functions as cf



def setup_pv(run_id):

    conn = cf.get_sql_connection()
    if conn is None:
        print("Cannot get database connection")
        return

    # Retrieve data from SQL
    survey_data = cf.get_table_values(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    sql3 = f"""
        INSERT INTO PROCESS_VARIABLE_PY
        SELECT * FROM PROCESS_VARIABLE_TESTING
        WHERE RUN_ID = '{run_id}'
    """

    conn.engine.execute(sql1)