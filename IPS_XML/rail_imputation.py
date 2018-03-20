'''
Created on 9 Jan 2018

@author: mahont1
'''
from IPSTransformation import CommonFunctions as cf
import pandas as pd


def populate_survey_data_for_rail_imputation(run_id,conn):
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Populates survey_data in preperation for performing the 
                 : rail imputation calculation.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    def nullify_survey_subsample_pv_values(conn):
        
        # SQL that sets survey_subsample's PV values to null
        sql = """
                update survey_subsample ss 
                set ss.RAIL_CNTRY_GRP_PV = null, 
                    ss.RAIL_EXERCISE_PV = null, 
                    ss.RAIL_IMP_ELIGIBLE_PV = null
                where ss.RUN_ID = '""" + run_id + "'"
        
        # Executes and commits the SQL command
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    
    def move_survey_subsample_to_sas_table(conn):
    
        column_string = """SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG, APORTLATMIN, 
            APORTLATSEC, APORTLATNS, APORTLONDEG, APORTLONMIN, APORTLONSEC,
            APORTLONEW, ARRIVEDEPART, BABYFARE, BEFAF, CHANGECODE, CHILDFARE,
            COUNTRYVISIT, CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS, 
            CPORTLONDEG, CPORTLONMIN, CPORTLONSEC, CPORTLONEW, INTDATE, DAYTYPE, 
            DIRECTLEG, DVEXPEND, DVFARE, DVLINECODE, DVPACKAGE, DVPACKCOST, 
            DVPERSONS, DVPORTCODE, EXPENDCODE, EXPENDITURE, FARE, FAREK, FLOW,
            HAULKEY, INTENDLOS, KIDAGE, LOSKEY, MAINCONTRA, MIGSI, INTMONTH, 
            NATIONALITY, NATIONNAME, NIGHTS1, NIGHTS2, NIGHTS3, NIGHTS4, NIGHTS5, 
            NIGHTS6, NIGHTS7, NIGHTS8, NUMADULTS, NUMDAYS, NUMNIGHTS, NUMPEOPLE, 
            PACKAGEHOL, PACKAGEHOLUK, PERSONS, PORTROUTE, PACKAGE, PROUTELATDEG, 
            PROUTELATMIN, PROUTELATSEC, PROUTELATNS, PROUTELONDEG, PROUTELONMIN, 
            PROUTELONSEC, PROUTELONEW, PURPOSE, QUARTER, RESIDENCE, RESPNSE, SEX, 
            SHIFTNO, SHUTTLE, SINGLERETURN, TANDTSI, TICKETCOST, TOWNCODE1, 
            TOWNCODE2, TOWNCODE3, TOWNCODE4, TOWNCODE5, TOWNCODE6, TOWNCODE7, 
            TOWNCODE8, TRANSFER, UKFOREIGN, VEHICLE, VISITBEGAN, WELSHNIGHTS, 
            WELSHTOWN, AM_PM_NIGHT_PV, APD_PV, ARRIVEDEPART_PV, CROSSINGS_FLAG_PV, 
            STAYIMPCTRYLEVEL1_PV, STAYIMPCTRYLEVEL2_PV, STAYIMPCTRYLEVEL3_PV, 
            STAYIMPCTRYLEVEL4_PV, DAY_PV, DISCNT_F1_PV, DISCNT_F2_PV, 
            DISCNT_PACKAGE_COST_PV, DUR1_PV, DUR2_PV, DUTY_FREE_PV, FAGE_PV, 
            FARES_IMP_ELIGIBLE_PV, FARES_IMP_FLAG_PV, FLOW_PV, FOOT_OR_VEHICLE_PV,
            HAUL_PV, IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV, IMBAL_ELIGIBLE_PV, 
            IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV, IMBAL_PORT_SUBGRP_PV, LOS_PV, 
            LOSDAYS_PV, MIG_FLAG_PV, MINS_CTRY_GRP_PV, MINS_CTRY_PORT_GRP_PV, 
            MINS_FLAG_PV, MINS_NAT_GRP_PV, MINS_PORT_GRP_PV, MINS_QUALITY_PV, 
            NR_FLAG_PV, NR_PORT_GRP_PV, OPERA_PV, OSPORT1_PV, OSPORT2_PV, 
            OSPORT3_PV, OSPORT4_PV, PUR1_PV, PUR2_PV, PUR3_PV, PURPOSE_PV, 
            QMFARE_PV, RAIL_EXERCISE_PV, RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV,
            SHIFT_FLAG_PV, SHIFT_PORT_GRP_PV, STAY_IMP_ELIGIBLE_PV, 
            STAY_IMP_FLAG_PV, STAY_PURPOSE_GRP_PV, TOWNCODE_PV, TYPE_PV, UK_OS_PV, 
            UKPORT1_PV, UKPORT2_PV, UKPORT3_PV, UKPORT4_PV, UNSAMP_PORT_GRP_PV, 
            UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV, DIRECT, EXPENDITURE_WT, 
            EXPENDITURE_WTK, OVLEG, SPEND, SPEND1, SPEND2, SPEND3, SPEND4, SPEND5, 
            SPEND6, SPEND7, SPEND8, SPEND9, SPENDIMPREASON, SPENDK, STAY, STAYK, 
            STAY1K, STAY2K, STAY3K, STAY4K, STAY5K, STAY6K, STAY7K,  STAY8K, 
            STAY9K, STAYTLY, STAY_WT, STAY_WTK, UKLEG, VISIT_WT, VISIT_WTK, 
            SHIFT_WT, NON_RESPONSE_WT, MINS_WT, TRAFFIC_WT, UNSAMP_TRAFFIC_WT, 
            IMBAL_WT, FINAL_WT, FAREKEY, TYPEINTERVIEW"""
    
        
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
        
        df_content = pd.read_sql(sql,conn)
        
        cf.insert_into_table_many('SAS_SURVEY_SUBSAMPLE', df_content, conn)
    
    cf.delete_from_table("SAS_RAIL_IMP")
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")
    
    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)
    
   
def copy_rail_imp_pvs_for_survey_data(run_id,conn): 
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Copies rail imputation process variables for survey data.
    Parameters   : NA
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_shift_spv_table = 'SAS_RAIL_SPV'
    
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_shift_spv_table)


    sas_process_variable_insert_query1 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 1 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('RAIL_CNTRY_GRP_PV'))"

    sas_process_variable_insert_query2 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 2 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('RAIL_EXERCISE_PV'))"

    sas_process_variable_insert_query3 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 3 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('RAIL_IMP_ELIGIBLE_PV'))"

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query1)
    conn.commit()  
    cur.execute(sas_process_variable_insert_query2)
    conn.commit()  
    cur.execute(sas_process_variable_insert_query3)
    conn.commit()  


def update_survey_data_with_rail_imp_pv_output(conn): 
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Updates survey_data with the rail imputation process variable 
                 : output.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sas_pv_table = 'SAS_PROCESS_VARIABLE'
    sas_spv_table = 'SAS_RAIL_SPV'
    
    sql = """update sas_survey_subsample sss       
                set (sss.RAIL_CNTRY_GRP_PV, 
                    sss.RAIL_EXERCISE_PV, 
                    sss.RAIL_IMP_ELIGIBLE_PV) =
                (select sssp.RAIL_CNTRY_GRP_PV, 
                    sssp.RAIL_EXERCISE_PV, 
                    sssp.RAIL_IMP_ELIGIBLE_PV
                from 
                    sas_rail_spv sssp
                where 
                    sss.SERIAL = sssp.SERIAL)
                """
    
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  
    
    cf.delete_from_table(sas_pv_table)
    cf.delete_from_table(sas_spv_table)          


def update_survey_data_with_rail_imp_results(conn):
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Updates survey_data with the rail imputation results.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sql = """update sas_survey_subsample sss
            set (sss.SPEND ) =
            (select 
                sri.SPEND        
            from 
                sas_rail_imp sri        
            where 
                sss.SERIAL = sri.SERIAL) 
            where 
                sss.serial in 
                (select sri2.serial from sas_rail_imp sri2 where sri2.spend >= 0)  
            """


    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  
        
    cf.delete_from_table("SAS_RAIL_IMP")    


def store_survey_data_with_rail_imp_results(run_id,conn):
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Stores the survey data with the rail imputation results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sql = """update survey_subsample ss
            set (ss.RAIL_CNTRY_GRP_PV,
                ss.RAIL_EXERCISE_PV, 
                ss.RAIL_IMP_ELIGIBLE_PV,
                ss.SPEND) = 
            (select sss..RAIL_CNTRY_GRP_PV, 
                sss.RAIL_EXERCISE_PV, 
                sss.RAIL_IMP_ELIGIBLE_PV,
                sss.SPEND
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
