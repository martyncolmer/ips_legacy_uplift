import inspect
import sys
from main.io import CommonFunctions as cf

SURVEY_SUBSAMPLE_TABLE = "[dbo].[SURVEY_SUBSAMPLE]"
SAS_SURVEY_SUBSAMPLE_TABLE = "[dbo].[SAS_SURVEY_SUBSAMPLE]"
SAS_PROCESS_VARIABLES_TABLE = "[dbo].[SAS_PROCESS_VARIABLE]"
COLUMNS_TO_MOVE = ['SERIAL', 'AGE', 'AM_PM_NIGHT', 'ANYUNDER16', 'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC',
                   'APORTLATNS', 'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC', 'APORTLONEW', 'ARRIVEDEPART',
                   'BABYFARE', 'BEFAF', 'CHANGECODE', 'CHILDFARE', 'COUNTRYVISIT', 'CPORTLATDEG',
                   'CPORTLATMIN', 'CPORTLATSEC', 'CPORTLATNS', 'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC',
                   'CPORTLONEW', 'INTDATE', 'DAYTYPE', 'DIRECTLEG', 'DVEXPEND', 'DVFARE', 'DVLINECODE',
                   'DVPACKAGE', 'DVPACKCOST', 'DVPERSONS', 'DVPORTCODE', 'EXPENDCODE', 'EXPENDITURE', 'FARE',
                   'FAREK', 'FLOW', 'HAULKEY', 'INTENDLOS', 'KIDAGE', 'LOSKEY', 'MAINCONTRA', 'MIGSI',
                   'INTMONTH', 'NATIONALITY', 'NATIONNAME', 'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4',
                   'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8', 'NUMADULTS', 'NUMDAYS', 'NUMNIGHTS',
                   'NUMPEOPLE', 'PACKAGEHOL', 'PACKAGEHOLUK', 'PERSONS', 'PORTROUTE', 'PACKAGE',
                   'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC', 'PROUTELATNS', 'PROUTELONDEG',
                   'PROUTELONMIN', 'PROUTELONSEC', 'PROUTELONEW', 'PURPOSE', 'QUARTER', 'RESIDENCE',
                   'RESPNSE', 'SEX', 'SHIFTNO', 'SHUTTLE', 'SINGLERETURN', 'TANDTSI', 'TICKETCOST',
                   'TOWNCODE1', 'TOWNCODE2', 'TOWNCODE3', 'TOWNCODE4', 'TOWNCODE5', 'TOWNCODE6', 'TOWNCODE7',
                   'TOWNCODE8', 'TRANSFER', 'UKFOREIGN', 'VEHICLE', 'VISITBEGAN', 'WELSHNIGHTS', 'WELSHTOWN',
                   'AM_PM_NIGHT_PV', 'APD_PV', 'ARRIVEDEPART_PV', 'CROSSINGS_FLAG_PV', 'STAYIMPCTRYLEVEL1_PV',
                   'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV', 'STAYIMPCTRYLEVEL4_PV', 'DAY_PV',
                   'DISCNT_F1_PV', 'DISCNT_F2_PV', 'DISCNT_PACKAGE_COST_PV', 'DUR1_PV', 'DUR2_PV',
                   'DUTY_FREE_PV', 'FAGE_PV', 'FARES_IMP_ELIGIBLE_PV', 'FARES_IMP_FLAG_PV', 'FLOW_PV',
                   'FOOT_OR_VEHICLE_PV', 'HAUL_PV', 'IMBAL_CTRY_FACT_PV', 'IMBAL_CTRY_GRP_PV',
                   'IMBAL_ELIGIBLE_PV', 'IMBAL_PORT_FACT_PV', 'IMBAL_PORT_GRP_PV', 'IMBAL_PORT_SUBGRP_PV',
                   'LOS_PV', 'LOSDAYS_PV', 'MIG_FLAG_PV', 'MINS_CTRY_GRP_PV', 'MINS_CTRY_PORT_GRP_PV',
                   'MINS_FLAG_PV', 'MINS_NAT_GRP_PV', 'MINS_PORT_GRP_PV', 'MINS_QUALITY_PV', 'NR_FLAG_PV',
                   'NR_PORT_GRP_PV', 'OPERA_PV', 'OSPORT1_PV', 'OSPORT2_PV', 'OSPORT3_PV', 'OSPORT4_PV',
                   'PUR1_PV', 'PUR2_PV', 'PUR3_PV', 'PURPOSE_PV', 'QMFARE_PV', 'RAIL_EXERCISE_PV',
                   'RAIL_IMP_ELIGIBLE_PV', 'SAMP_PORT_GRP_PV', 'SHIFT_FLAG_PV', 'SHIFT_PORT_GRP_PV',
                   'STAY_IMP_ELIGIBLE_PV', 'STAY_IMP_FLAG_PV', 'STAY_PURPOSE_GRP_PV', 'TOWNCODE_PV',
                   'TYPE_PV', 'UK_OS_PV', 'UKPORT1_PV', 'UKPORT2_PV', 'UKPORT3_PV', 'UKPORT4_PV',
                   'UNSAMP_PORT_GRP_PV', 'UNSAMP_REGION_GRP_PV', 'WEEKDAY_END_PV', 'DIRECT', 'EXPENDITURE_WT',
                   'EXPENDITURE_WTK', 'OVLEG', 'SPEND', 'SPEND1', 'SPEND2', 'SPEND3', 'SPEND4', 'SPEND5',
                   'SPEND6', 'SPEND7', 'SPEND8', 'SPEND9', 'SPENDIMPREASON', 'SPENDK', 'STAY', 'STAYK',
                   'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K', 'STAY9K',
                   'STAYTLY', 'STAY_WT', 'STAY_WTK', 'UKLEG', 'VISIT_WT', 'VISIT_WTK', 'SHIFT_WT',
                   'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT',
                   'FAREKEY', 'TYPEINTERVIEW']


def nullify_survey_subsample_pv_values(run_id, conn, pv_values):
    """
    Author       : Elinor Thorne
    Date         : Apr 2018
    Purpose      : Updates required columns to null
    Parameters   : NA
    Returns      : NA
    """

    # Construct string for SQL statement
    columns_to_null = []
    for item in pv_values:
        columns_to_null.append(item + " = null")
    columns_to_null = ", ".join(map(str, columns_to_null))

    # Create SQL Statement
    sql = """
    UPDATE {} 
        SET {} 
        WHERE RUN_ID = '{}'
    """.format(SURVEY_SUBSAMPLE_TABLE, columns_to_null, run_id)

    print(sql)
    print("")

    # Execute and commits the SQL command
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def move_survey_subsample_to_sas_table(run_id, conn, step_name):
    """
    Author       : Elinor Thorne
    Date         : Apr 2018
    Purpose      : Moves data to temporary location
    Parameters   : NA
    Returns      : NA
    """

    columns = ["["+col+"]" for col in COLUMNS_TO_MOVE]
    columns = ','.join(columns)

    # Assign RESPNSE condition to step
    if step_name == "TRAFFIC_WEIGHT" or step_name == "UNSAMPLED_WEIGHT":
        respnse = "BETWEEN 1 and 2"
    else:
        respnse = "BETWEEN 1 and 6"

    # Create and execute SQL Statement
    sql = """
    INSERT INTO {} 
        ({})
            (SELECT {} 
            FROM {}
            WHERE RUN_ID = '{}'
            AND SERIAL NOT LIKE '9999%'
            AND RESPNSE {})
    """.format(SAS_SURVEY_SUBSAMPLE_TABLE, columns, columns, SURVEY_SUBSAMPLE_TABLE, run_id, respnse)

    print("{}: {}".format(str(inspect.stack()[0][3]).upper(), sql))
    print("")

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def populate_survey_data_for_step(run_id, conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : 13 Apr 2018
    Purpose      : Populates survey_data in preparation for step
    Parameters   : conn - connection object pointing at the database
                 : run_id -
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Cleanse tables as applicable
    delete_statement = cf.delete_from_table(SAS_SURVEY_SUBSAMPLE_TABLE)
    print(delete_statement)
    print("")

    for table in step_configuration["delete_tables"]:
        delete_statement = cf.delete_from_table(table)
        print("{}: {}".format(str(inspect.stack()[0][3]).upper(), delete_statement))
        print("")

    nullify_survey_subsample_pv_values(run_id, conn, step_configuration["nullify_pvs"])
    move_survey_subsample_to_sas_table(run_id, conn, step_configuration["name"])


def populate_step_data(run_id, conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Populate step
    Parameters   : run_id -
                 : conn -
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Assign variables
    table = step_configuration["table_name"]
    data_table = step_configuration["data_table"]
    columns = step_configuration["insert_to_populate"]
    cols = ", ".join(map(str, columns))

    # Construct string for SQL statement
    calc_cols = ["CALC." + col for col in columns]
    calc_columns = ", ".join(map(str, calc_cols))

    # Cleanse temp table
    delete_statement = cf.delete_from_table(data_table)
    print(delete_statement)
    print("")

    # Create and execute SQL statement
    sql = """
    INSERT INTO {}
        ({})
    SELECT {}
    FROM {} AS CALC
    WHERE RUN_ID = '{}'
    """.format(data_table, cols, calc_columns, table, run_id)
    print("{}: {}".format(str(inspect.stack()[0][3]).upper(), sql))
    print("")

    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Exception as err:
        # TODO: log and handle error
        pass


def copy_step_pvs_for_survey_data(run_id, conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Copy step process variable data
    Parameters   : run_id -
                 : conn -
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Assign variables
    basic_insert = ["SHIFT_WEIGHT", "NON_RESPONSE", "MINIMUMS_WEIGHT", "TRAFFIC_WEIGHT"]
    multiple_inserts = ["UNSAMPLED_WEIGHT", "IMBALANCE_WEIGHT", "FARES_IMPUTATION", "SPEND_IMPUTATION", "RAIL_IMPUTATION", "REGIONAL_WEIGHTS", "TOWN_AND_STAY_EXPENDITURE"]
    spv_table = step_configuration["spv_table"]
    cur = conn.cursor()

    # Cleanse tables
    delete_statement = cf.delete_from_table(SAS_PROCESS_VARIABLES_TABLE)
    print(delete_statement)
    print("")
    delete_statement = cf.delete_from_table(spv_table)
    print(delete_statement)
    print("")

    step = step_configuration["name"]

    # Construct SQL statement as applicable and execute
    if step in basic_insert:
        columns = step_configuration["pv_columns"]
        str_input = ", ".join(map(str, columns))
        sql = """
        INSERT INTO {}
            (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0
            FROM PROCESS_VARIABLE_PY AS PV WHERE PV.RUN_ID = '{}' 
            AND UPPER(PV.PV_NAME) IN ({}))
        """.format(SAS_PROCESS_VARIABLES_TABLE, run_id, str_input)
        print(sql)
        print("")
        cur.execute(sql)
        conn.commit()

    if step in multiple_inserts:
        count = 0
        for item in step_configuration["pv_columns"]:
            count = count + 1
            sql = """
            INSERT INTO {}
                (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, {}
                FROM PROCESS_VARIABLE_PY AS PV WHERE PV.RUN_ID = '{}' 
                AND UPPER(PV.PV_NAME) IN ({}))
            """.format(SAS_PROCESS_VARIABLES_TABLE, count, run_id, item)
            print(sql)
            print("")
            cur.execute(sql)
            conn.commit()

    if step == "STAY_IMPUTATION":
        columns = [col.replace(']', '').replace('[', '') for col in step_configuration['copy_pvs']]
        str_input = "', '".join(map(str, columns))
        sql = """
        INSERT INTO {}
            (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0
            FROM PROCESS_VARIABLE_PY AS PV WHERE PV.RUN_ID = '{}' 
            AND UPPER(PV.PV_NAME) IN ('{}'))
            """.format(SAS_PROCESS_VARIABLES_TABLE, run_id, str_input)
        print(sql)
        print("")
        cur.execute(sql)
        conn.commit()
        count = 0

        for item in step_configuration["copy_pvs2"]:
            # item = item.replace(']', '').replace('[', '')
            count = count + 1
            sql = """
            INSERT INTO {}
                (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, {}
                FROM PROCESS_VARIABLE_PY AS PV WHERE PV.RUN_ID = '{}' 
                AND UPPER(PV.PV_NAME) IN ('{}'))
            """.format(SAS_PROCESS_VARIABLES_TABLE, count, run_id, item)
            print(sql)
            print("")
            cur.execute(sql)
            conn.commit()

# Nassir - Done
def update_survey_data_with_step_pv_output(conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : Apr 2018
    Purpose      : Updates survey_data with the process variable outputs
    Parameters   : conn - connection object pointing at the database
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Assign variables
    spv_table = step_configuration["spv_table"]

    # Construct string for SQL statement
    cols = [item.replace("'", "") for item in step_configuration["pv_columns"]]
    cols = [item + " = CALC." + item for item in cols]
    set_statement = ", ".join(map(str, cols))

    # Construct and execute SQL statement
    sql = """
        UPDATE {}
            SET {}
            FROM {} as SSS
            JOIN {} as CALC
            ON SSS.SERIAL = CALC.SERIAL
        """.format(SAS_SURVEY_SUBSAMPLE_TABLE, set_statement, SAS_SURVEY_SUBSAMPLE_TABLE, spv_table)
    print(sql)
    print("")

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    # Cleanse temp tables
    delete_statement = cf.delete_from_table(SAS_PROCESS_VARIABLES_TABLE)
    print(delete_statement)
    print("")

    delete_statement = cf.delete_from_table(spv_table)
    print(delete_statement)
    print("")

    # code specific to minimums weight function/step
    # TODO: consider moving this out to another function called by minimum weight
    if step_configuration["name"] == "MINIMUMS_WEIGHT":
        delete_statement = cf.delete_from_table(step_configuration["weight_table"])
        print(delete_statement)
        print("")
        delete_statement = cf.delete_from_table(step_configuration["sas_ps_table"])
        print(delete_statement)
        print("")

# Nassir
def copy_step_pvs_for_step_data(run_id, conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Copies the process variables for the step.
    Parameters   : run_id -
                 : conn - connection object pointing at the database.
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Cleanse temp tables
    delete_statement = cf.delete_from_table(SAS_PROCESS_VARIABLES_TABLE)
    print(delete_statement)
    print("")
    delete_statement = cf.delete_from_table(step_configuration["pv_table"])
    print(delete_statement)
    print("")

    # Construct and execute SQL statements as applicable
    if step_configuration["name"] == "UNSAMPLED_WEIGHT":
        order = step_configuration["order"]
        for item in step_configuration["pv_columns"]:
            sql = ("""
                 INSERT INTO {}
                 ([PROCVAR_NAME], [PROCVAR_RULE], [PROCVAR_ORDER])
                     (SELECT pv.[PV_NAME], pv.[PV_DEF], {}
                     FROM [dbo].[PROCESS_VARIABLE_PY] AS pv
                     WHERE pv.[RUN_ID] = '{}'
                     AND UPPER(pv.[PV_NAME]) in ({}))
                 """.format(SAS_PROCESS_VARIABLES_TABLE, order, run_id, item))
            print(sql)
            print("")
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            order = order + 1
    else:
        cols = []
        for item in step_configuration["pv_columns"]:
            cols.append(item)
        pv_columns = ", ".join(map(str, cols))

        sql = """
            INSERT INTO {}
            ([PROCVAR_NAME], [PROCVAR_RULE], [PROCVAR_ORDER])
                (SELECT pv.[PV_NAME], pv.[PV_DEF], {}
                FROM [dbo].[PROCESS_VARIABLE_PY] AS pv
                WHERE pv.[RUN_ID] = '{}'
                AND UPPER(pv.[PV_NAME]) in ({})) 
        """.format(SAS_PROCESS_VARIABLES_TABLE, step_configuration["order"], run_id, pv_columns)
        print(sql)
        print("")
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

# Nassir
def update_step_data_with_step_pv_output(conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Updates data with the process variable output.
    Parameters   : conn - connection object pointing at the database.
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Construct string for SQL statement
    cols = [item.replace("'", "") for item in step_configuration["pv_columns2"]]
    cols = [item + " = CALC." + item for item in cols]
    set_statement = ", ".join(map(str, cols))

    # Construct and execute SQL statement
    pv_table = step_configuration["pv_table"]
    data_table = step_configuration["data_table"]
    sql = """
            UPDATE {}
                SET {}
                FROM {} as SSS
                JOIN {} as CALC
                ON SSS.REC_ID = CALC.REC_ID
            """.format(data_table, set_statement, data_table, pv_table)
    print(sql)
    print("")

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    # Cleanse temporary tables
    delete_statement = cf.delete_from_table(step_configuration["pv_table"])
    print(delete_statement)
    delete_statement = cf.delete_from_table(step_configuration["weight_table"])
    print(delete_statement)
    delete_statement = cf.delete_from_table(SAS_PROCESS_VARIABLES_TABLE)
    print(delete_statement)
    delete_statement = cf.delete_from_table(step_configuration["sas_ps_table"])
    print(delete_statement)

def sql_update_statement(table_to_update_from, columns_to_update):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Constructs SQL update statement
    Parameters   : step -
    Returns      : String - SQL update statement
    """
    # Construct SET string
    cols = [item + ' = temp.' + item for item in columns_to_update]
    columns = " , ".join(cols)

    # Construct SQL statement and execute
    sql = """
            UPDATE {}
            SET {}
            FROM {} as SSS
            JOIN {} as temp
            ON SSS.SERIAL = temp.SERIAL            
            """.format(SAS_SURVEY_SUBSAMPLE_TABLE, columns, SAS_SURVEY_SUBSAMPLE_TABLE, table_to_update_from)

    print(sql)
    print("")

    return sql


def update_survey_data_with_step_results(conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Updates survey data with the results
    Parameters   : conn - connection object pointing at the database
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    step = step_configuration["name"]

    # Assign variables
    do_green = ["SHIFT_WEIGHT", "NON_RESPONSE", "MINIMUMS_WEIGHT", "TRAFFIC_WEIGHT", "UNSAMPLED_WEIGHT", "FINAL_WEIGHT",
                "FARES_IMPUTATION", "REGIONAL_WEIGHTS", "TOWN_AND_STAY_EXPENDITURE", "AIR_MILES"]
    weights = ["SHIFT_WEIGHT", "NON_RESPONSE", "MINIMUMS_WEIGHT", "TRAFFIC_WEIGHT", "UNSAMPLED_WEIGHT", "FINAL_WEIGHT",
               "IMBALANCE_WEIGHT"]
    imputations = ["FARES_IMPUTATION", "REGIONAL_WEIGHTS", "TOWN_AND_STAY_EXPENDITURE", "RAIL_IMPUTATION",
                   "STAY_IMPUTATION", "SPEND_IMPUTATION", "AIR_MILES"]

    if step in weights:
        table = step_configuration["weight_table"]
    elif step in imputations:
        table = step_configuration["temp_table"]
    else:
        # TODO throw error - invalid step
        return None

    sql2 = ""

    # Construct and execute SQL statement/s as applicable
    results_columns = step_configuration["results_columns"]
    if step in do_green:
        sql1 = sql_update_statement(table, results_columns)
    elif step == "IMBALANCE_WEIGHT":
        sql1 = sql_update_statement(table, results_columns)
        sql2 = """
                UPDATE {}
                SET [IMBAL_WT] = 1.00
                WHERE [IMBAL_WT] IS NULL
                """.format(SAS_SURVEY_SUBSAMPLE_TABLE)
    elif step == "STAY_IMPUTATION":
        sql1 = sql_update_statement(table, results_columns)
        sql2 = """
                UPDATE {}
                SET [STAY] = (SELECT temp.[NUMNIGHTS]
                    FROM {} AS temp
                    WHERE [SERIAL] = temp.[SERIAL])
                    WHERE [SERIAL] NOT IN (SELECT imp.[SERIAL]
                        FROM {} AS imp)
                """.format(SAS_SURVEY_SUBSAMPLE_TABLE, SAS_SURVEY_SUBSAMPLE_TABLE, table)
    elif step == "SPEND_IMPUTATION":
        sql1 = """
                    UPDATE {}
                    SET [SPEND] = (SELECT temp.[NEWSPEND]
                        FROM {} as temp
                        WHERE [SERIAL] = temp.[SERIAL])
                    WHERE [SERIAL] IN (SELECT temp2.[SERIAL]
                        FROM {} AS temp2
                        WHERE temp2.[NEWSPEND] >= 0)
                    """.format(SAS_SURVEY_SUBSAMPLE_TABLE, table, table)
        sql2 = sql_update_statement(table, results_columns)
    else:
        sql1 = """
                   UPDATE {}
                   SET [SPEND] = (SELECT temp.[SPEND] FROM {} AS temp WHERE [SERIAL] = temp.[SERIAL])
                   WHERE [SERIAL] IN (SELECT temp2.[SERIAL] FROM {} AS temp2 WHERE temp2.[SPEND] >= 0)
                   """.format(SAS_SURVEY_SUBSAMPLE_TABLE, table, table)
        sql2 = ""

    print(sql1)
    print("")
    cur = conn.cursor()
    cur.execute(sql1)
    if sql2 != "":
        print(sql2)
        print("")
        cur.execute(sql2)
    conn.commit()

    # Cleanse temporary table
    delete_statement = cf.delete_from_table(table)
    print(delete_statement)
    print("")


def store_survey_data_with_step_results(run_id, conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Stores the survey data with the results
    Parameters   : run_id -
                 : conn - connection object pointing at the database.
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    step = step_configuration["name"]
    cols = step_configuration["nullify_pvs"]

    # Add additional column to two steps
    if (step == "SPEND_IMPUTATION") or (step == "RAIL_IMPUTATION"):
        cols.append("[SPEND]")

    cols = [item + " = SSS." + item for item in cols]
    set_statement = " , ".join(cols)

    # Create SQL statement and execute
    sql = """
    UPDATE {}
    SET {}
    FROM {} as SS
    JOIN {} as SSS
    ON SS.SERIAL = SSS.SERIAL
    AND SS.RUN_ID = '{}'
    """.format(SURVEY_SUBSAMPLE_TABLE, set_statement, SURVEY_SUBSAMPLE_TABLE, SAS_SURVEY_SUBSAMPLE_TABLE, run_id)

    print(sql)
    print("")

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    # Cleanse summary and subsample tables as applicable
    ps_tables_to_delete = ["SHIFT_WEIGHT"
        , "NON_RESPONSE"
        , "MINIMUMS_WEIGHT"
        , "TRAFFIC_WEIGHT"
        , "UNSAMPLED_WEIGHT"
        , "IMBALANCE_WEIGHT"
        , "FINAL_WEIGHT"]

    if step in ps_tables_to_delete:
        delete_statement = cf.delete_from_table(step_configuration["ps_table"], "RUN_ID", "=", run_id)
        print(delete_statement)
        print("")

    delete_statement = cf.delete_from_table(SAS_SURVEY_SUBSAMPLE_TABLE)
    print(delete_statement)
    print("")


def store_step_summary(run_id, conn, step_configuration):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Stores the summary data
    Parameters   : run_id
                 : conn - connection object pointing at the database
                 : step -
    Returns      : NA
    """

    print(str(inspect.stack()[0][3]).upper())
    print("")

    # Assign variables
    ps_table = step_configuration["ps_table"]
    sas_ps_table = step_configuration["sas_ps_table"]

    # Cleanse summary table as applicable
    delete_statement = cf.delete_from_table(ps_table, "RUN_ID", "=", run_id)
    print(delete_statement)
    print("")

    # Create selection string
    selection = [col for col in step_configuration["ps_columns"] if col != "[RUN_ID]"]
    columns = " , ".join(step_configuration["ps_columns"])
    selection = " , ".join(selection)

    # Create and execute SQL statement
    sql = """
    INSERT INTO {}
    ({})
    SELECT '{}', {} FROM {}
    """.format(ps_table, columns, run_id, selection, sas_ps_table)

    print(sql)
    print("")

    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Exception as err:
        print(err)

    # Cleanse temporary summary table
    delete_statement = cf.delete_from_table(sas_ps_table)
    print(delete_statement)
    print("")


if __name__ == "__main__":
    run_id = 'update-step-data-with-step-pv-output'
    conn = cf.get_sql_connection()
    step_config = {"table_name": "[dbo].[SHIFT_DATA]",
                   "data_table": "[dbo].[SAS_SHIFT_DATA]",
                   "insert_to_populate": ["[PORTROUTE]", "[WEEKDAY]", "[ARRIVEDEPART]", "[TOTAL]",
                                          "[AM_PM_NIGHT]"],
                   }

    # update_step_data_with_step_pv_output(conn, step_config)
    populate_step_data(run_id, conn, step_config)