'''
Created on 21 March 2018

@author: burrj
'''
from IPSTransformation import CommonFunctions as cf
import pandas as pd


def populate_survey_data_for_fares_imputation(run_id, conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Populates survey_data in preparation for performing the
                 : fares imputation calculation.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    def nullify_survey_subsample_pv_values(conn):
        # SQL that sets survey_subsample's PV values to null
        sql = """
                 update survey_subsample ss
                 set ss.FARES_IMP_FLAG_PV = null
                 ,ss.FARES_IMP_ELIGIBLE_PV = null,ss.DISCNT_PACKAGE_COST_PV = null, ss.DISCNT_F1_PV = null
                 ,ss.DISCNT_F2_PV = null, ss.FAGE_PV = null,ss.ss.OPERA_PV = null, ss.TYPE_PV = null
                 ,ss.UKPORT1_PV = null, ss.UKPORT2_PV = null, ss.UKPORT3_PV = null, ss.UKPORT4_PV = null
                 ,ss.OSPORT1_PV = null, ss.OSPORT2_PV, ss.OSPORT3_PV = null, ss.OSPORT4_PV = null
                 ,ss.APD_PV = null, ss.QMFARE_PV = null, ss.DUTY_FREE_PV = null, ss.FARE = null, ss.FAREK = null
                 ,ss.SPEND = null, ss.SPENDIMPREASON = null
                 where 
                    ss.RUN_ID = '""" + run_id + "'"

        # Executes and commits the SQL command
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

    def move_survey_subsample_to_sas_table(conn):
        column_string = """SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG,APORTLATMIN, APORTLATSEC, APORTLATNS
        ,APORTLONDEG, APORTLONMIN, APORTLONSEC, APORTLONEW, ARRIVEDEPART, BABYFARE, BEFAF, CHANGECODE, CHILDFARE
        ,COUNTRYVISIT, CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS, CPORTLONDEG,CPORTLONMIN, CPORTLONSEC
        ,CPORTLONEW, INTDATE, DAYTYPE, DIRECTLEG,DVEXPEND, DVFARE, DVLINECODE, DVPACKAGE, DVPACKCOST, DVPERSONS
        ,DVPORTCODE, EXPENDCODE, EXPENDITURE, FARE, FAREK, FLOW,HAULKEY, INTENDLOS, KIDAGE, LOSKEY, MAINCONTRA, MIGSI
        ,INTMONTH, NATIONALITY, NATIONNAME, NIGHTS1, NIGHTS2, NIGHTS3,NIGHTS4, NIGHTS5, NIGHTS6, NIGHTS7, NIGHTS8
        ,NUMADULTS,NUMDAYS, NUMNIGHTS, NUMPEOPLE, PACKAGEHOL, PACKAGEHOLUK, PERSONS,PORTROUTE, PACKAGE, PROUTELATDEG
        ,PROUTELATMIN, PROUTELATSEC, PROUTELATNS,PROUTELONDEG, PROUTELONMIN, PROUTELONSEC, PROUTELONEW, PURPOSE
        ,QUARTER,RESIDENCE, RESPNSE, SEX, SHIFTNO, SHUTTLE, SINGLERETURN,TANDTSI, TICKETCOST, TOWNCODE1, TOWNCODE2
        ,TOWNCODE3, TOWNCODE4,TOWNCODE5, TOWNCODE6, TOWNCODE7, TOWNCODE8, TRANSFER, UKFOREIGN,VEHICLE, VISITBEGAN
        ,WELSHNIGHTS, WELSHTOWN, AM_PM_NIGHT_PV, APD_PV,ARRIVEDEPART_PV, CROSSINGS_FLAG_PV, STAYIMPCTRYLEVEL1_PV
        ,STAYIMPCTRYLEVEL2_PV, STAYIMPCTRYLEVEL3_PV, STAYIMPCTRYLEVEL4_PV,DAY_PV, DISCNT_F1_PV, DISCNT_F2_PV
        ,DISCNT_PACKAGE_COST_PV, DUR1_PV, DUR2_PV,DUTY_FREE_PV, FAGE_PV, FARES_IMP_ELIGIBLE_PV, FARES_IMP_FLAG_PV
        ,FLOW_PV, FOOT_OR_VEHICLE_PV,HAUL_PV, IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV, IMBAL_ELIGIBLE_PV
        ,IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV,IMBAL_PORT_SUBGRP_PV, LOS_PV, LOSDAYS_PV, MIG_FLAG_PV, MINS_CTRY_GRP_PV
        ,MINS_CTRY_PORT_GRP_PV,MINS_FLAG_PV, MINS_NAT_GRP_PV, MINS_PORT_GRP_PV, MINS_QUALITY_PV, NR_FLAG_PV
        ,NR_PORT_GRP_PV,OPERA_PV, OSPORT1_PV, OSPORT2_PV, OSPORT3_PV, OSPORT4_PV, PUR1_PV,PUR2_PV, PUR3_PV, PURPOSE_PV
        ,QMFARE_PV, RAIL_EXERCISE_PV, RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV,SHIFT_FLAG_PV, SHIFT_PORT_GRP_PV
        ,STAY_IMP_ELIGIBLE_PV, STAY_IMP_FLAG_PV, STAY_PURPOSE_GRP_PV, TOWNCODE_PV,TYPE_PV, UK_OS_PV, UKPORT1_PV
        ,UKPORT2_PV, UKPORT3_PV, UKPORT4_PV,UNSAMP_PORT_GRP_PV, UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV, DIRECT
        ,EXPENDITURE_WT, EXPENDITURE_WTK,OVLEG, SPEND, SPEND1, SPEND2, SPEND3, SPEND4,SPEND5, SPEND6, SPEND7, SPEND8
        ,SPEND9, SPENDIMPREASON,SPENDK, STAY, STAYK, STAY1K, STAY2K, STAY3K,STAY4K, STAY5K, STAY6K, STAY7K,STAY8K
        ,STAY9K,STAYTLY, STAY_WT,  STAY_WTK, UKLEG, VISIT_WT, VISIT_WTK,SHIFT_WT, NON_RESPONSE_WT, MINS_WT, TRAFFIC_WT
        ,UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT, FAREKEY, TYPEINTERVIEW"""

        sql = "select " + column_string + """ 
                    from 
                        survey_subsample ss 
                    where 
                        ss.RUN_ID = '""" + run_id + """' 
                    and 
                        ss.SERIAL not like '9999%' 
                    and 
                        ss.RESPNSE between 1 and 6

                """

        df_content = pd.read_sql(sql, conn)

        cf.insert_into_table_many('SAS_SURVEY_SUBSAMPLE', df_content, conn)

    cf.delete_from_table("SAS_FARES_IMP")
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")

    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def copy_fares_imp_pvs_for_survey_data(run_id, conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Copies fares imputation process variables for survey data.
    Parameters   : NA
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_shift_spv_table = 'SAS_FARES_SPV'

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_shift_spv_table)

    sas_process_variable_insert_query1 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 1 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('FARES_IMP_FLAG_PV'))"

    sas_process_variable_insert_query2 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 2 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('FARES_IMP_ELIGIBLE_PV'))"

    sas_process_variable_insert_query3 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 3 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('DISCNT_F1_PV'))"

    sas_process_variable_insert_query4 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 4 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('DISCNT_PACKAGE_COST_PV'))"

    sas_process_variable_insert_query5 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 5 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('DISCNT_F2_PV'))"

    sas_process_variable_insert_query6 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 6 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('FAGE_PV'))"

    sas_process_variable_insert_query7 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 7 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('TYPE_PV'))"

    sas_process_variable_insert_query8 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 8 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('OPERA_PV'))"

    sas_process_variable_insert_query9 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 9 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('UKPORT1_PV'))"

    sas_process_variable_insert_query10 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 10 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('UKPORT2_PV'))"

    sas_process_variable_insert_query11 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 11 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('UKPORT3_PV'))"

    sas_process_variable_insert_query12 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 12 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('UKPORT4_PV'))"

    sas_process_variable_insert_query13 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 13 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('OSPORT1_PV'))"

    sas_process_variable_insert_query14 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 14 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('OSPORT2_PV'))"

    sas_process_variable_insert_query15 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 15 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('OSPORT3_PV'))"

    sas_process_variable_insert_query16 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 16 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('OSPORT4_PV'))"

    sas_process_variable_insert_query17 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 17 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('APD_PV'))"

    sas_process_variable_insert_query18 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 18 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('QMFARE_PV'))"

    sas_process_variable_insert_query19 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 19 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('DUTY_FREE_PV'))"

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query1)
    conn.commit()
    cur.execute(sas_process_variable_insert_query2)
    conn.commit()
    cur.execute(sas_process_variable_insert_query3)
    conn.commit()
    cur.execute(sas_process_variable_insert_query4)
    conn.commit()
    cur.execute(sas_process_variable_insert_query5)
    conn.commit()
    cur.execute(sas_process_variable_insert_query6)
    conn.commit()
    cur.execute(sas_process_variable_insert_query7)
    conn.commit()
    cur.execute(sas_process_variable_insert_query8)
    conn.commit()
    cur.execute(sas_process_variable_insert_query9)
    conn.commit()
    cur.execute(sas_process_variable_insert_query10)
    conn.commit()
    cur.execute(sas_process_variable_insert_query11)
    conn.commit()
    cur.execute(sas_process_variable_insert_query12)
    conn.commit()
    cur.execute(sas_process_variable_insert_query13)
    conn.commit()
    cur.execute(sas_process_variable_insert_query14)
    conn.commit()
    cur.execute(sas_process_variable_insert_query15)
    conn.commit()
    cur.execute(sas_process_variable_insert_query16)
    conn.commit()
    cur.execute(sas_process_variable_insert_query17)
    conn.commit()
    cur.execute(sas_process_variable_insert_query18)
    conn.commit()
    cur.execute(sas_process_variable_insert_query19)
    conn.commit()


def update_survey_data_with_fares_imp_pv_output(conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Updates survey_data with the fares imputation process variable
                 : output.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_pv_table = 'SAS_PROCESS_VARIABLE'
    sas_spv_table = 'SAS_FARES_SPV'

    sql = """update sas_survey_subsample sss
                set(sss.FARES_IMP_FLAG_PV
                ,sss.FARES_IMP_ELIGIBLE_PV
                ,sss.DISCNT_PACKAGE_COST_PV
                ,sss.DISCNT_F1_PV
                ,sss.DISCNT_F2_PV
                ,sss.FAGE_PV
                ,sss.OPERA_PV
                ,sss.TYPE_PV
                ,sss.UKPORT1_PV
                ,sss.UKPORT2_PV
                ,sss.UKPORT3_PV
                ,sss.UKPORT4_PV
                ,sss.OSPORT1_PV
                ,sss.OSPORT2_PV
                ,sss.OSPORT3_PV
                ,sss.OSPORT4_PV
                ,sss.APD_PV
                ,sss.QMFARE_PV
                ,sss.DUTY_FREE_PV) = 
                (select sssp.FARES_IMP_FLAG_PV
                ,sssp.FARES_IMP_ELIGIBLE_PV
                ,sssp.DISCNT_PACKAGE_COST_PV
                ,sssp.DISCNT_F1_PV
                ,sssp.DISCNT_F2_PV
                ,sssp.FAGE_PV
                ,sssp.OPERA_PV
                ,sssp.TYPE_PV
                ,sssp.UKPORT1_PV
                ,sssp.UKPORT2_PV
                ,sssp.UKPORT3_PV
                ,sssp.UKPORT4_PV
                ,sssp.OSPORT1_PV
                ,sssp.OSPORT2_PV
                ,sssp.OSPORT3_PV
                ,sssp.OSPORT4_PV
                ,sssp.APD_PV
                ,sssp.QMFARE_PV
                ,sssp.DUTY_FREE_PV
                from 
                    sas_fares_spv ssssp
                where 
                    sss.SERIAL = sssp.SERIAL)
                """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table(sas_pv_table)
    cf.delete_from_table(sas_spv_table)


def update_survey_data_with_fares_imp_results(conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Updates survey_data with the fares imputation results.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update sas_survey_subsample sss
            set (sss.FARE
            ,sss.FAREK
            ,sss.SPEND
            ,sss.SPENDIMPREASON ) =
            (select 
                sfi.FARE
                ,sfi.FAREK
                ,sfi.SPEND
                ,sfi.SPENDIMPREASON
            from 
                sas_fares_imp sfi
            where 
                sss.SERIAL = sfi.SERIAL) 
            """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table("SAS_FARES_IMP")


def store_survey_data_with_fares_imp_results(run_id, conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Stores the survey data with the fares imputation results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update survey_subsample ss
            set (ss.FARES_IMP_FLAG_PV,
                ss.FARES_IMP_ELIGIBLE_PV, 
                ss.DISCNT_PACKAGE_COST_PV,
                ss.DISCNT_F1_PV,
                ss.DISCNT_F2_PV,
                ss.FAGE_PV,
                ss.OPERA_PV,
                ss.TYPE_PV,
                ss.UKPORT1_PV,
                ss.UKPORT2_PV,
                ss.UKPORT3_PV,
                ss.UKPORT4_PV,
                ss.OSPORT1_PV,
                ss.OSPORT2_PV,
                ss.OSPORT3_PV,
                ss.OSPORT4_PV,
                ss.APD_PV,
                ss.QMFARE_PV,
                ss.DUTY_FREE_PV,
                ss.FARE,
                ss.FAREK,
                ss.SPEND,
                ss.SPENDIMPREASON) = 
            (select sss.FARES_IMP_FLAG_PV
            ,sss.FARES_IMP_ELIGIBLE_PV
            ,sss.DISCNT_PACKAGE_COST_PV
            ,sss.DISCNT_F1_PV
            ,sss.DISCNT_F2_PV
            ,sss.FAGE_PV
            ,sss.OPERA_PV
            ,sss.TYPE_PV
            ,sss.UKPORT1_PV
            ,sss.UKPORT2_PV
            ,sss.UKPORT3_PV
            ,sss.UKPORT4_PV
            ,sss.OSPORT1_PV
            ,sss.OSPORT2_PV
            ,sss.OSPORT3_PV
            ,sss.OSPORT4_PV
            ,sss.APD_PV
            ,sss.QMFARE_PV
            ,sss.DUTY_FREE_PV
            ,sss.FARE
            ,sss.FAREK
            ,sss.SPEND
            ,sss.SPENDIMPREASON
            from 
                sas_survey_subsample sss     
            where 
                sss.SERIAL = ss.SERIAL)
            where 
                ss.RUN_ID = '""" + run_id + "'"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")