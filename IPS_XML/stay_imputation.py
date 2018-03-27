'''
Created on 21 March 2018

@author: burrj
'''
from IPSTransformation import CommonFunctions as cf
import pandas as pd


def populate_survey_data_for_stay_imputation(run_id, conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Populates survey_data in preparation for performing the
                 : stay imputation calculation.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    def nullify_survey_subsample_pv_values(conn):
        # SQL that sets survey_subsample's PV values to null
        sql = """
                 update survey_subsample ss
                 set ss.STAY_IMP_FLAG_PV = null
                 ,ss.STAY_IMP_ELIGIBLE_PV = null,ss.STAYIMPCTRYLEVEL1_PV = null, ss.STAYIMPCTRYLEVEL2_PV = null
                 ,ss.STAYIMPCTRYLEVEL3_PV = null, ss.STAYIMPCTRYLEVEL4_PV = null,ss.STAY_PURPOSE_GRP_PV = null
                 ,ss.STAY = null, ss.STAYK = null
                 where ss.RUN_ID = '""" + run_id + "'"

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
        ,DISCNT_PACKAGE_COST_PV, DUR1_PV, DUR2_PV,DUTY_FREE_PV, FAGE_PV, STAY_IMP_ELIGIBLE_PV, STAY_IMP_FLAG_PV
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

    cf.delete_from_table("SAS_STAY_IMP")
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")

    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def copy_stay_imp_pvs_for_survey_data(run_id, conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Copies stay imputation process variables for survey data.
    Parameters   : NA
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_shift_spv_table = 'SAS_STAY_SPV'

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_shift_spv_table)

    sas_process_variable_insert_query1 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAY_IMP_FLAG_PV', 'STAY_IMP_ELIGIBLE_PV', 'STAY_PURPOSE_GRP_PV'))"

    sas_process_variable_insert_query2 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 1 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL1_PV'))"

    sas_process_variable_insert_query3 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 2 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL2_PV'))"

    sas_process_variable_insert_query4 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 3 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL3_PV'))"

    sas_process_variable_insert_query5 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 4 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL4_PV'))"

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


def update_survey_data_with_stay_imp_pv_output(conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Updates survey_data with the stay imputation process variable
                 : output.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_pv_table = 'SAS_PROCESS_VARIABLE'
    sas_spv_table = 'SAS_STAY_SPV'

    sql = """update sas_survey_subsample sss
                set(sss.STAY_IMP_FLAG_PV, sss.STAY_IMP_ELIGIBLE_PV
                ,sss.STAYIMPCTRYLEVEL1_PV
                ,sss.STAYIMPCTRYLEVEL2_PV
                ,sss.STAYIMPCTRYLEVEL3_PV
                ,sss.STAYIMPCTRYLEVEL4_PV, sss.STAY_PURPOSE_GRP_PV) =
                (select sssp.STAY_IMP_FLAG_PV
                ,sssp.STAY_IMP_ELIGIBLE_PV
                ,sssp.STAYIMPCTRYLEVEL1_PV
                ,sssp.STAYIMPCTRYLEVEL2_PV
                ,sssp.STAYIMPCTRYLEVEL3_PV
                ,sssp.STAYIMPCTRYLEVEL4_PV
                ,sssp.STAY_PURPOSE_GRP_PV
                from 
                    sas_stay_spv ssssp
                where 
                    sss.SERIAL = sssp.SERIAL)
                """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table(sas_pv_table)
    cf.delete_from_table(sas_spv_table)


def update_survey_data_with_stay_imp_results(conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Updates survey_data with the stay imputation results.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql1 = """update sas_survey_subsample sss
            set (sss.STAY, sss.STAYK ) =
            (select 
                ssi.STAY, ssi.STAYK   
            from 
                sas_stay_imp ssi        
            where 
                sss.SERIAL = ssi.SERIAL) 
            where 
                sss.serial in 
                (select sri2.serial from sas_stay_imp sri2 where sri2.spend >= 0)  
            """

    sql2 = """update sas_survey_subsample sss
            set(sss.STAY) = 
            (select
                sur.NUMNIGHTS
            from
                sas_survey_subsample sur
            where 
                sss.SERIAL = sur.SERIAL)
            where
                sss.SERIAL not in 
                (SELECT imp.SERIAL FROM sas_stay_imp imp)
            """

    cur = conn.cursor()
    cur.execute(sql1)
    conn.commit()
    cur.execute(sql2)
    conn.commit()

    cf.delete_from_table("SAS_STAY_IMP")


def store_survey_data_with_stay_imp_results(run_id, conn):
    """
    Author       : James Burr
    Date         : 21/03/2018
    Purpose      : Stores the survey data with the stay imputation results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update survey_subsample ss
            set (ss.STAY_IMP_FLAG_PV,
                ss.STAY_IMP_ELIGIBLE_PV, 
                ss.STAYIMPCTRYLEVEL1_PV,
                ss.STAYIMPCTRYLEVEL2_PV,
                ss.STAYIMPCTRYLEVEL3_PV,
                ss.STAYIMPCTRYLEVEL4_PV,
                ss.STAY_PURPOSE_GRP_PV,
                ss.STAY,
                ss.STAYK) = 
            (select sss.STAY_IMP_FLAG_PV, 
                sss.STAY_IMP_ELIGIBLE_PV, 
                sss.STAYIMPCTRYLEVEL1_PV,
                sss.STAYIMPCTRYLEVEL2_PV,
                sss.STAYIMPCTRYLEVEL3_PV,
                sss.STAYIMPCTRYLEVEL4_PV,
                sss.STAY_PURPOSE_GRP_PV,
                sss.STAY,
                sss.STAYK,
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