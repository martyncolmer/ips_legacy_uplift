from main.io import CommonFunctions as cf
import pandas as pd
import json
import pyodbc
import sys
from pprint import pprint

with open("variables.json") as f:
    data = json.load(f)

survey_subsample = "[dbo].[SURVEY_SUBSAMPLE]"
sas_survey_subsample = "[dbo].[SAS_SURVEY_SUBSAMPLE]"
sas_process_variable = "[dbo].[SAS_PROCESS_VARIABLE]"
columns = ["SERIAL", "AGE", "AM_PM_NIGHT", "ANYUNDER16", "APORTLATDEG", "APORTLATMIN", "APORTLATSEC", "APORTLATNS"
    , "APORTLONDEG", "APORTLONMIN", "APORTLONSEC", "APORTLONEW", "ARRIVEDEPART", "BABYFARE", "BEFAF", "CHANGECODE"
    , "CHILDFARE", "COUNTRYVISIT", "CPORTLATDEG", "CPORTLATMIN", "CPORTLATSEC", "CPORTLATNS", "CPORTLONDEG"
    , "CPORTLONMIN", "CPORTLONSEC", "CPORTLONEW", "INTDATE", "DAYTYPE", "DIRECTLEG", "DVEXPEND", "DVFARE", "DVLINECODE"
    , "DVPACKAGE", "DVPACKCOST", "DVPERSONS", "DVPORTCODE", "EXPENDCODE", "EXPENDITURE", "FARE", "FAREK", "FLOW"
    , "HAULKEY", "INTENDLOS", "KIDAGE", "LOSKEY", "MAINCONTRA", "MIGSI", "INTMONTH", "NATIONALITY", "NATIONNAME"
    , "NIGHTS1", "NIGHTS2", "NIGHTS3", "NIGHTS4", "NIGHTS5", "NIGHTS6", "NIGHTS7", "NIGHTS8", "NUMADULTS", "NUMDAYS"
    , "NUMNIGHTS", "NUMPEOPLE", "PACKAGEHOL", "PACKAGEHOLUK", "PERSONS", "PORTROUTE", "PACKAGE", "PROUTELATDEG"
    , "PROUTELATMIN", "PROUTELATSEC", "PROUTELATNS", "PROUTELONDEG", "PROUTELONMIN", "PROUTELONSEC", "PROUTELONEW"
    , "PURPOSE", "QUARTER", "RESIDENCE", "RESPNSE", "SEX", "SHIFTNO", "SHUTTLE", "SINGLERETURN", "TANDTSI"
    , "TICKETCOST", "TOWNCODE1", "TOWNCODE2", "TOWNCODE3", "TOWNCODE4", "TOWNCODE5", "TOWNCODE6", "TOWNCODE7"
    , "TOWNCODE8", "TRANSFER", "UKFOREIGN", "VEHICLE", "VISITBEGAN", "WELSHNIGHTS", "WELSHTOWN", "AM_PM_NIGHT_PV"
    , "APD_PV", "ARRIVEDEPART_PV", "CROSSINGS_FLAG_PV", "STAYIMPCTRYLEVEL1_PV", "STAYIMPCTRYLEVEL2_PV"
    , "STAYIMPCTRYLEVEL3_PV", "STAYIMPCTRYLEVEL4_PV", "DAY_PV", "DISCNT_F1_PV", "DISCNT_F2_PV", "DISCNT_PACKAGE_COST_PV"
    , "DUR1_PV", "DUR2_PV", "DUTY_FREE_PV", "FAGE_PV", "FARES_IMP_ELIGIBLE_PV", "FARES_IMP_FLAG_PV", "FLOW_PV"
    , "FOOT_OR_VEHICLE_PV", "HAUL_PV", "IMBAL_CTRY_FACT_PV", "IMBAL_CTRY_GRP_PV", "IMBAL_ELIGIBLE_PV"
    , "IMBAL_PORT_FACT_PV", "IMBAL_PORT_GRP_PV", "IMBAL_PORT_SUBGRP_PV", "LOS_PV", "LOSDAYS_PV", "MIG_FLAG_PV"
    , "MINS_CTRY_GRP_PV", "MINS_CTRY_PORT_GRP_PV", "MINS_FLAG_PV", "MINS_NAT_GRP_PV", "MINS_PORT_GRP_PV"
    , "MINS_QUALITY_PV", "NR_FLAG_PV", "NR_PORT_GRP_PV", "OPERA_PV", "OSPORT1_PV", "OSPORT2_PV", "OSPORT3_PV"
    , "OSPORT4_PV", "PUR1_PV", "PUR2_PV", "PUR3_PV", "PURPOSE_PV", "QMFARE_PV", "RAIL_EXERCISE_PV"
    , "RAIL_IMP_ELIGIBLE_PV", "SAMP_PORT_GRP_PV", "SHIFT_FLAG_PV", "SHIFT_PORT_GRP_PV", "STAY_IMP_ELIGIBLE_PV"
    , "STAY_IMP_FLAG_PV", "STAY_PURPOSE_GRP_PV", "TOWNCODE_PV", "TYPE_PV", "UK_OS_PV", "UKPORT1_PV", "UKPORT2_PV"
    , "UKPORT3_PV", "UKPORT4_PV", "UNSAMP_PORT_GRP_PV", "UNSAMP_REGION_GRP_PV", "WEEKDAY_END_PV", "DIRECT"
    , "EXPENDITURE_WT", "EXPENDITURE_WTK", "OVLEG", "SPEND", "SPEND1", "SPEND2", "SPEND3", "SPEND4", "SPEND5", "SPEND6"
    , "SPEND7", "SPEND8", "SPEND9", "SPENDIMPREASON", "SPENDK", "STAY", "STAYK", "STAY1K", "STAY2K", "STAY3K", "STAY4K"
    , "STAY5K", "STAY6K", "STAY7K", "STAY8K", "STAY9K", "STAYTLY", "STAY_WT", "STAY_WTK", "UKLEG", "VISIT_WT"
    , "VISIT_WTK", "SHIFT_WT", "NON_RESPONSE_WT", "MINS_WT", "TRAFFIC_WT", "UNSAMP_TRAFFIC_WT", "IMBAL_WT", "FINAL_WT"
    , "FAREKEY", "TYPEINTERVIEW"]


def populate_survey_data(run_id, conn, calculation):
    """
    Author       : Elinor Thorne
    Date         : 13 Apr 2018
    Purpose      : Populates survey_data in preparation for calculation
    Parameters   : conn - connection object pointing at the database
                 : run_id -
                 : calculation -
    Returns      : NA
    """
    def nullify_survey_subsample_pv_values():
        # Retrieve variables from JSON and create string to input to SQL Statement
        input = []
        for item in data[calculation]["nullify_pvs"]:
            input.append(item + " = null")
        columns_to_null = ", ".join(map(str, input))

        # Create SQL Statement to set survey_subsample's PV values to null
        sql = """
        UPDATE {} 
            SET {} 
            WHERE RUN_ID = '{}'
        """.format(survey_subsample, columns_to_null, run_id)

        # Executes and commits the SQL command
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


    def move_survey_subsample_to_sas_table():
        # Convert columns list to string
        column_string = ", ".join(map(str, columns))

        # Assign RESPNSE condition to calculation
        if calculation == "TRAFFIC_WEIGHT" or calculation == "UNSAMPLED_WEIGHT":
            respnse = "BETWEEN 1 and 2"
        else:
            respnse = "BETWEEN 1 and 6"

        # Create SQL Statement to copy survey_subsample to sas_survey_subsample
        sql = """
        INSERT INTO {} 
            ({})
                (SELECT {} 
                FROM {}
                WHERE RUN_ID = '{}'
                AND SERIAL NOT LIKE '9999%'
                AND RESPNSE {})
        """.format(sas_survey_subsample, column_string, column_string, survey_subsample, run_id, respnse)

        # # Executes and commits the SQL command
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


    cf.delete_from_table(sas_survey_subsample)

    for table in data[calculation]["delete_tables"]:
        cf.delete_from_table(table)

    nullify_survey_subsample_pv_values()
    move_survey_subsample_to_sas_table()


def populate_data(run_id, conn, calculation):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Populate calculation
    Parameters   : run_id -
                 : conn -
    Returns      : NA
    """

    # Assign variables
    table = data[calculation]["table_name"]
    data_table = data[calculation]["data_table"]
    columns = data[calculation]["insert_to_populate"]
    cols = "], [".join(map(str, columns))

    calc_cols = []
    for col in data[calculation]["insert_to_populate"]:
        if col != "REC_ID":
            calc_cols.append("CALC.[" + col)

    calc_columns = "], ".join(map(str, calc_cols))

    # Delete temp table
    cf.delete_from_table(data_table)

    # Create SQL statement
    sql = """
    INSERT INTO {}
        ([{}])
    SELECT ([sys].[sp_sequence_get_range]), {}]
    FROM {} AS CALC
    WHERE RUN_ID = '{}'
    """.format(data_table, cols, calc_columns, table, run_id)

    # Execute statement
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def copy_pvs_for_survey_data(run_id, conn, calculation):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Copy calculation process variable data
    Parameters   : run_id -
                 : conn -
                 : calculation -
    Returns      : NA
    """

    cur = conn.cursor()
    cf.delete_from_table(sas_process_variable)

    try:
        # Retrieve name of SPV table for calculation
        spv_table = (data[calculation]["spv_table"])
    except KeyError:
        return
    else:
        cf.delete_from_table(spv_table)

    if data[calculation]["count"] == 0:
        input = data[calculation]["pv_columns"]
        str_input = "', '".join(map(str, input))
        sql = """
        INSERT INTO {}
            (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0
            FROM PROCESS_VARIABLE AS PV WHERE PV.RUN_ID = '{}' 
            AND UPPER(PV.PV_NAME) IN ('{}'))
        """.format(sas_process_variable, run_id, str_input)
        cur.execute(sql)
        conn.commit()
    else:
        if calculation == "STAY_IMPUTATION":
            input = data[calculation]["copy_pvs"]
            str_input = "', '".join(map(str, input))
            sql = """
            INSERT INTO {}
                (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0
                FROM PROCESS_VARIABLE AS PV WHERE PV.RUN_ID = '{}' 
                AND UPPER(PV.PV_NAME) IN ('{}'))
                """.format(sas_process_variable, run_id, str_input)
            cur.execute(sql)
            conn.commit()
            count = 0
            for item in data[calculation]["copy_pvs2"]:
                count = count + 1
                sql = """
                INSERT INTO {}
                    (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, {}
                    FROM PROCESS_VARIABLE AS PV WHERE PV.RUN_ID = '{}' 
                    AND UPPER(PV.PV_NAME) IN ('{}'))
                """.format(sas_process_variable, count, run_id, item)
                cur.execute(sql)
                conn.commit()
        else:
            count = 0
            for item in data[calculation]["pv_columns"]:
                count = count + 1
                sql = """
                INSERT INTO {}
                    (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, {}
                    FROM PROCESS_VARIABLE AS PV WHERE PV.RUN_ID = '{}' 
                    AND UPPER(PV.PV_NAME) IN ('{}'))
                """.format(sas_process_variable, count, run_id, item)
                cur.execute(sql)
                conn.commit()


def update_survey_data_with_pv_output(calculation):
        """
        Author       : Elinor Thorne
        Date         : Apr 2018
        Purpose      : Updates survey_data with the process variable outputs
        Parameters   : conn - connection object pointing at the database
                     : calculation -
        Returns      : NA
        """

        try:
            spv_table = data[calculation]["spv_table"]
        except KeyError:
            return

        set = []
        spv_table = data[calculation]["spv_table"]

        for item in data[calculation]["pv_columns"]:
            set.append(item + " = (SELECT " + item + " FROM " + spv_table + " AS CALC WHERE SERIAL = CALC.SERIAL)")
        set_statement = ", ".join(map(str, set))

        sql = """
        UPDATE {}
            SET {}
            WHERE SERIAL = (SELECT SERIAL 
                            FROM {})
        """.format(sas_survey_subsample, set_statement, spv_table)

        print(sql)
        sys.exit()

        # cur = conn.cursor()
        # cur.execute(sql)
        # conn.commit()

        cf.delete_from_table(sas_process_variable)
        cf.delete_from_table(spv_table)

        if calculation == "MINIMUMS_WEIGHT":
            cf.delete_from_table(data[calculation]["wt_table"])
            cf.delete_from_table(data[calculation]["sas_ps_table"])


def copy_pvs_for_data(run_id, conn, calculation):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Copies the process variables for the calculation.
    Parameters   : run_id -
                 : conn - connection object pointing at the database.
                 : calculation -
    Returns      : NA
    """

    cf.delete_from_table(sas_process_variable)
    cf.delete_from_table(data[calculation]["pv_table"])

    if calculation == "UNSAMPLED_WEIGHT":
        order = data[calculation]["order"]
        for item in data[calculation]["pv_columns2"]:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO {}
                ([PROCVAR_NAME], [PROCVAR_RULE], [PROCVAR_ORDER])
                    (SELECT pv.[PV_NAME], pv.[PV_DEF], {}
                    FROM [dbo].[PROCESS_VARIABLE] AS pv
                    WHERE pv.[RUN_ID] = '{}'
                    AND UPPER(pv.[PV_NAME]) in ({}))
                """.format(sas_process_variable, order, run_id, item))
            conn.commit()
            order = order + 1
    else:
        cols = []
        for item in data[calculation]["pv_columns2"]:
            cols.append(item)
        pv_columns = ", ".join(map(str, cols))

        sql = """
            INSERT INTO {}
            ([PROCVAR_NAME], [PROCVAR_RULE], [PROCVAR_ORDER])
                (SELECT pv.[PV_NAME], pv.[PV_DEF], {}
                FROM [dbo].[PROCESS_VARIABLE] AS pv
                WHERE pv.[RUN_ID] = '{}'
                AND UPPER(pv.[PV_NAME]) in ({})) 
        """.format(sas_process_variable, data[calculation]["order"], run_id, pv_columns)

        # print(sql)
        # sys.exit()

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def update_data_with_pv_output(calculation):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Updates data with the process variable output.
    Parameters   : conn - connection object pointing at the database.
                 : calculation -
    Returns      : NA
    """

    set = []
    for item in data[calculation]["pv_columns2"]:
        set.append(item + " = (SELECT " + item + " FROM " + data[calculation]["pv_table"] + " AS PV WHERE [REC_ID] = PV.[REC_ID])")
    set_statement = """, 
    """.join(map(str, set))

    sql = """
        UPDATE {}
        SET {}
        WHERE [REC_ID] = (SELECT [REC_ID] FROM {})
    """.format(data[calculation]["data_table"], set_statement, data[calculation]["pv_table"])

    # print(sql)

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    # print("cf.delete_from_table({})".format(data[calculation]["pv_table"]))
    # print("cf.delete_from_table({})".format(data[calculation]["weight_table"]))
    # print("cf.delete_from_table({})".format(sas_process_variable))
    # print("cf.delete_from_table({})".format(data[calculation]["sas_ps_table"]))

    cf.delete_from_table(data[calculation]["pv_table"])
    cf.delete_from_table(data[calculation]["weight_table"])
    cf.delete_from_table(sas_process_variable)
    cf.delete_from_table(data[calculation]["sas_ps_table"])


def update_survey_data_with_results(calculation):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Updates survey data with the results
    Parameters   : conn - connection object pointing at the database
                 : calculation -
    Returns      : NA
    """

    # Assign variables
    do_green = ["SHIFT_WEIGHT", "NON_RESPONSE", "MINIMUMS_WEIGHT", "TRAFFIC_WEIGHT", "UNSAMPLED_WEIGHT", "FINAL_WEIGHT",
                "FARES_IMPUTATION", "REGIONAL_WEIGHTS", "TOWN_AND_STAY_EXPENDITURE", "AIR_MILES"]
    weights = ["SHIFT_WEIGHT", "NON_RESPONSE", "MINIMUMS_WEIGHT", "TRAFFIC_WEIGHT", "UNSAMPLED_WEIGHT", "FINAL_WEIGHT",
               "IMBALANCE_WEIGHT"]
    imputations = ["FARES_IMPUTATION", "REGIONAL_WEIGHTS", "TOWN_AND_STAY_EXPENDITURE", "RAIL_IMPUTATION",
                   "STAY_IMPUTATION", "SPEND_IMPUTATION", "AIR_MILES"]

    if calculation in weights:
        table = data[calculation]["weight_table"]
    elif calculation in imputations:
        table = data[calculation]["temp_table"]
    else:
        table = sas_survey_subsample

    sql2 = ""

    def green(calculation):
        # Construct SET string
        cols = []
        for item in data[calculation]["results_columns"]:
            cols.append(
                item + " = (SELECT temp." + item + " FROM " + table + " AS temp WHERE [SERIAL] = temp.[SERIAL])")
        columns = """, 
                """.join(map(str, cols))

        # Construct SQL statement and execute
        sql = """
                UPDATE {}
                SET {}
                WHERE [SERIAL] = {}.SERIAL
                """.format(sas_survey_subsample, columns, table)

        return sql

    if calculation in do_green:
        sql1 = green(calculation)
    elif calculation == "IMBALANCE_WEIGHT":
        sql1 = green(calculation)
        sql2 = """
                UPDATE {}
                SET [IMBAL_WT] = 1.00
                WHERE [IMBAL_WT] IS NULL
                """.format(sas_survey_subsample)
    elif calculation == "STAY_IMPUTATION":
        sql1 = green(calculation)
        sql2 = """
                UPDATE {}
                SET [STAY] = (SELECT temp.NUMNIGHTS
                    FROM {} AS temp
                    WHERE SERIAL = temp.SERIAL)
                    WHERE SERIAL NOT IN (SELECT imp.SERIAL
                        FROM {})
                """.format(sas_survey_subsample, sas_survey_subsample, table)
    elif calculation == "SPEND_IMPUTATION":
        sql1 = """
                    UPDATE {}
                    SET [SPEND] = (SELECT temp.NEWSPEND
                        FROM {} as temp
                        WHERE SERIAL = temp.SERIAL)
                    WHERE [SERIAL] IN (SELECT temp2.SERIAL
                        FROM {} AS temp2
                        WHERE temp2[NEWSPEND] >= 0)
                    """.format(sas_survey_subsample, table, table)
        sql2 = green(calculation)
    else:
        sql1 = """
                   UPDATE {}
                   SET [SPEND] = (SELECT temp.SPEND FROM {} WHERE SERIAL = temp.SERIAL)
                   WHERE [SERIAL] IN (SELECT temp2.SERIAL FROM {} WHERE temp2.SPEND >= 0)
                   """.format(sas_survey_subsample, table, table)
        sql2 = ""

    # cur = conn.cursor()
    # cur.execute(sql1)
    print(sql1)
    print("")
    if sql2 != "":
        # cur.execute(sql2)
        print(sql2)
    # conn.commit()

    # if calculation != "RAIL_IMPUTATION":
    #     if calculation == "SPEND_IMPUTATION":
    #         sql = """
    #         UPDATE {}
    #         SET [SPEND] = (SELECT temp.NEWSPEND
    #             FROM {} as temp
    #             WHERE SERIAL = temp.SERIAL)
    #         WHERE [SERIAL] IN (SELECT temp2.SERIAL
    #             FROM {} AS temp2
    #             WHERE temp2[NEWSPEND] >= 0)
    #         """.format(sas_survey_subsample, table, table)
    #
    #         print(sql)
    #         # cur = conn.cursor()
    #         # cur.execute(sql)
    #         # conn.commit()
    #
    #     # Construct SET string
    #     cols = []
    #     for item in data[calculation]["results_columns"]:
    #         cols.append(item + " = (SELECT temp." + item + " FROM " + table + " AS temp WHERE [SERIAL] = temp.[SERIAL])")
    #     columns = """,
    #     """.join(map(str, cols))
    #
    #     # Construct SQL statement and execute
    #     sql = """
    #     UPDATE {}
    #     SET {}
    #     WHERE [SERIAL] = {}.SERIAL
    #     """.format(sas_survey_subsample, columns, table)
    #
    #     print(sql)
    #     # cur = conn.cursor()
    #     # cur.execute(sql)
    #     # conn.commit()
    # elif calculation == "IMBALANCE_WEIGHT":
    #     sql = """
    #     UPDATE {}
    #     SET [IMBAL_WT] = 1.00
    #     WHERE [IMBAL_WT] IS NULL
    #     """.format(sas_survey_subsample)
    #
    #     print(sql)
    #     # cur = conn.cursor()
    #     # cur.execute(sql)
    #     # conn.commit()
    # elif calculation == "STAY_IMPUTATION":
    #     sql2 = """
    #     UPDATE {}
    #     SET [STAY] = (SELECT temp.NUMNIGHTS
    #         FROM {} AS temp
    #         WHERE SERIAL = temp.SERIAL)
    #         WHERE SERIAL NOT IN (SELECT imp.SERIAL
    #             FROM {})
    #     """.format(sas_survey_subsample, sas_survey_subsample, table)
    #
    #     print(sql)
    #     # cur = conn.cursor()
    #     # cur.execute(sql)
    #     # conn.commit()
    #
    # else:   # RAIL_IMPUTATION
    #     sql = """
    #        UPDATE {}
    #        SET [SPEND] = (SELECT temp.SPEND FROM {} WHERE SERIAL = temp.SERIAL)
    #        WHERE [SERIAL] IN (SELECT temp2.SERIAL FROM {} WHERE temp2.SPEND >= 0)
    #        """.format(sas_survey_subsample, table, table)
    #
    #     print(sql)
    #     # cur = conn.cursor()
    #     # cur.execute(sql)
    #     # conn.commit()

    # Cleanse temporary table
    # cf.delete_from_table(table)
    print("cf.delete_from_table({})".format(table))


def store_survey_data_with_results(run_id, calculation):
    """
    Author       : Elinor Thorne
    Date         : April 2018
    Purpose      : Stores the survey data with the results
    Parameters   : run_id -
                 : conn - connection object pointing at the database.
    Returns      : NA
    """

    # Add additional column to two steps
    if (calculation == "SPEND_IMPUTATION") \
            or (calculation == "RAIL_IMPUTATION"):
        set = ["[SPEND] = (SELECT [SPEND] FROM [dbo].[SAS_SURVEY_SUBSAMPLE] AS SS WHERE [SERIAL] = SS.[SERIAL])"]
    else:
        set = []

    # Create SET and SELECT string
    for item in data[calculation]["nullify_pvs"]:
        set.append(item + " = (SELECT " + item + " FROM " + sas_survey_subsample + " AS SS WHERE [SERIAL] = SS.[SERIAL])")
    set_statement = """, 
        """.join(map(str, set))

    # Create SQL statement and execute
    sql = """
    UPDATE {}
    SET {}
    WHERE RUN_ID = '{}'
    """.format(survey_subsample, set_statement, run_id)

    print(sql)

    # cur = conn.cursor()
    # cur.execute(sql)
    # conn.commit()

    # Delete from summary and subsample tables as applicable
    ps_tables_to_delete = ["SHIFT_WEIGHT"
        , "NON_RESPONSE"
        , "MINIMUMS_WEIGHT"
        , "TRAFFIC_WEIGHT"
        , "UNSAMPLED_WEIGHT"
        , "IMBALANCE_WEIGHT"
        , "FINAL_WEIGHT"]

    if calculation in ps_tables_to_delete:
        # cf.delete_from_table(data[calculation]["ps_table"], "RUN_ID", "=", run_id)
        print("cf.delete_from_table({}, 'RUN_ID', '=',{})".format(data[calculation]["ps_table"], run_id))
    print("cf.delete_from_table({})".format(sas_survey_subsample))
    # cf.delete_from_table(sas_survey_subsample)


def store_summary(run_id, calculation):
    """
    Author       : Elinor Thorne
    Date         : May 2018
    Purpose      : Stores the summary data
    Parameters   : run_id
                 : conn - connection object pointing at the database
                 : calculation -
    Returns      : NA
    """

    # Assign variables
    ps_table = data[calculation]["ps_table"]
    sas_ps_table = data[calculation]["sas_ps_table"]
    cols = []
    sel = []

    # Create selection string
    for col in data[calculation]["ps_columns"]:
        cols.append(col)
        if col != "RUN_ID":
            sel.append("SELECT " + col + " FROM " + sas_ps_table)
    columns = """, 
    """.join(map(str, cols))
    selection = """),
     (""".join(map(str, sel))

    # Cleanse summary table
    # cf.delete_from_table(ps_table, 'RUN_ID', '=', run_id)
    print("cf.delete_from_table({}, 'RUN_ID', '=', {})".format(ps_table, run_id))

    # Create and execute SQL statement
    sql = """
    INSERT INTO {}
    ({})
    VALUES (SELECT '{}', ({}))
    """.format(ps_table, columns, run_id, selection)

    # cur = conn.cursor()
    # cur.execute(sql)
    # conn.commit()
    print(sql)

    # Cleanse temporary summary table
    # cf.delete_from_table(sas_ps_table)
    print("cf.delete_from_table({})".format(sas_ps_table))


def sandbox(calculation):
    list = ["Foo", "Bar"]
    
    if calculation == "Foo":
        print("Result1")
    elif calculation in list:
        print("Result2")
    else:
        print("Nah")
    


if __name__ == "__main__":
    # calculation = "UNSAMPLED_WEIGHT"
    # sandbox(calculation)
    # update_survey_data_with_pv_output(calculation)

    # calculation = "SHIFT_WEIGHT"
    # update_survey_data_with_results(calculation)

    # run_id = "9e5c1872-3f8e-4ae5-85dc-c67a602d011e"
    #
    calculation = "SHIFT_WEIGHT"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    print("")
    update_survey_data_with_results(calculation)
    print("")

    calculation = "NON_RESPONSE"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")

    calculation = "MINIMUMS_WEIGHT"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")

    calculation = "TRAFFIC_WEIGHT"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")

    calculation = "UNSAMPLED_WEIGHT"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    # #
    calculation = "IMBALANCE_WEIGHT"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    # #
    calculation = "FINAL_WEIGHT"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    #
    calculation = "STAY_IMPUTATION"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    #
    calculation = "FARES_IMPUTATION"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    # #
    calculation = "SPEND_IMPUTATION"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    #
    calculation = "RAIL_IMPUTATION"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    # #
    calculation = "REGIONAL_WEIGHTS"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    # #
    calculation = "TOWN_AND_STAY_EXPENDITURE"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    # #
    calculation = "AIR_MILES"
    print(calculation)
    print("Update Survey Data with {} Results".format(calculation))
    update_survey_data_with_results(calculation)
    print("")
    #
    #
    # # run_id = "9e5c1872-3f8e-4ae5-85dc-c67a602d011e"
    # # conn = cf.get_oracle_connection()
    # # calculation = "MINIMUMS_WEIGHT"
    # #
    # # populate_calculation(run_id, conn, calculation)
    # #
    # # calculation = "RAIL_IMPUTATION"
    # # update_survey_data_with_pv_output(conn, calculation)
    # #
    # # calculation = "MINIMUMS_WEIGHT"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "NON_RESPONSE"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "SHIFT_WEIGHT"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # #
    # # calculation = "TRAFFIC_WEIGHT"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "FARES_IMPUTATION"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "IMBALANCE_WEIGHT"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "REGIONAL_WEIGHTS"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "SPEND_IMPUTATION"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "STAY_IMPUTATION"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "TOWN_AND_STAY_EXPENDITURE"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # # #
    # # calculation = "UNSAMPLED_WEIGHT"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # #
    # # calculation = "FINAL_WEIGHT"
    # # update_survey_data_with_pv_output(conn, calculation)
    # # #
    # # # # sandbox()



