'''
Created on 9 Jan 2018

@author: mahont1
'''
from main.io import CommonFunctions as cf
import pandas as pd


def populate_survey_data_for_regional_wt(run_id, conn):
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Populates survey_data in preperation for regional weights
                 : calculation.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    def nullify_survey_subsample_pv_values(conn):
        
        # SQL that sets survey_subsample's PV values to null
        sql = """
                update survey_subsample ss 
                set ss.STAYIMPCTRYLEVEL1_PV = null,          
                    ss.STAYIMPCTRYLEVEL2_PV = null, 
                    ss.STAYIMPCTRYLEVEL3_PV = null,          
                    ss.STAYIMPCTRYLEVEL4_PV = null, 
                    ss.REG_IMP_ELIGIBLE_PV = null,          
                    ss.VISIT_WT = null, 
                    ss.VISIT_WTK = null, 
                    ss.STAY_WT = null, 
                    ss.STAY_WTK = null,          
                    ss.EXPENDITURE_WT = null, 
                    ss.EXPENDITURE_WTK = null, 
                    ss.NIGHTS1 = null,          
                    ss.NIGHTS2 = null, 
                    ss.NIGHTS3 = null, 
                    ss.NIGHTS4 = null, 
                    ss.NIGHTS5 = null,           
                    ss.NIGHTS6 = null, 
                    ss.NIGHTS7 = null, 
                    ss.NIGHTS8 = null, 
                    ss.STAY1K = null,          
                    ss.STAY2K = null, 
                    ss.STAY3K = null, 
                    ss.STAY4K = null, 
                    ss.STAY5K = null,           
                    ss.STAY6K = null, 
                    ss.STAY7K = null, 
                    ss.STAY8K = null        
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
        
        df_content = pd.read_sql(sql, conn)
        
        cf.insert_into_table_many('SAS_SURVEY_SUBSAMPLE', df_content, conn)
    
    cf.delete_from_table("SAS_REGIONAL_IMP")
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")
    
    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def copy_regional_wt_pvs_for_survey_data(run_id, conn):
    """
    Author       : Richmond Rice
    Date         : 20/03/2018
    Purpose      : Copies the regional weights process variables for survey data.
    Parameters   : NA
    Returns      : NA
    Requirements : 
    Dependencies : 
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_shift_spv_table = 'SAS_REGIONAL_SPV'
    
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_shift_spv_table)

    sas_process_variable_insert_query1 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 1 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('PURPOSE_PV'))"

    sas_process_variable_insert_query2 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 2 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL1_PV'))"

    sas_process_variable_insert_query3 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 3 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL2_PV'))"
        
    sas_process_variable_insert_query4 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 4 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL3_PV'))"

    sas_process_variable_insert_query5 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 5 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('STAYIMPCTRYLEVEL4_PV'))"

    sas_process_variable_insert_query6 = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER) \
        (SELECT PV.PV_NAME, PV.PV_DEF, 6 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('REG_IMP_ELIGIBLE_PV'))"

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


def update_survey_data_with_regional_wt_pv_output(conn): 
    """
    Author       : Thomas Mahoney
    Date         : 10 Jan 2017
    Purpose      : Updates survey_data with the shift weight process variable 
                 : output.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sas_pv_table = 'SAS_PROCESS_VARIABLE'
    sas_spv_table = 'SAS_RAIL_SPV'   
    
    sql = """update sas_survey_subsample sss       
                set (sss.PURPOSE_PV, 
                    sss.STAYIMPCTRYLEVEL1_PV, 
                    sss.STAYIMPCTRYLEVEL2_PV, 
                    sss.STAYIMPCTRYLEVEL3_PV, 
                    sss.STAYIMPCTRYLEVEL4_PV, 
                    sss.REG_IMP_ELIGIBLE_PV ) =
                (select srs.PURPOSE_PV, 
                    srs.STAYIMPCTRYLEVEL1_PV,          
                    srs.STAYIMPCTRYLEVEL2_PV, 
                    srs.STAYIMPCTRYLEVEL3_PV,          
                    srs.STAYIMPCTRYLEVEL4_PV, 
                    srs.REG_IMP_ELIGIBLE_PV
                from 
                    sas_regional_spv srs
                where 
                    sss.SERIAL = srs.SERIAL)
                """
    
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  
    
    cf.delete_from_table(sas_pv_table)
    cf.delete_from_table(sas_spv_table)         


def update_survey_data_with_regional_wt_results(conn):
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Updates survey_data with the regional weight results.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sql = """update sas_survey_subsample sss
            set (sss.VISIT_WT, 
                    sss.VISIT_WTK, 
                    sss.STAY_WT, 
                    sss.STAY_WTK,          
                    sss.EXPENDITURE_WT, 
                    sss.EXPENDITURE_WTK, 
                    sss.NIGHTS1, 
                    sss.NIGHTS2, 
                    sss.NIGHTS3, 
                    sss.NIGHTS4, 
                    sss.NIGHTS5, 
                    sss.NIGHTS6, 
                    sss.NIGHTS7,
                    sss.NIGHTS8, 
                    sss.STAY1K,          
                    sss.STAY2K,
                    sss.STAY3K,
                    sss.STAY4K, 
                    sss.STAY5K,           
                    sss.STAY6K, 
                    sss.STAY7K, 
                    sss.STAY8K =
            (select 
                sri.VISIT_WT, 
                    sri.VISIT_WTK, 
                    sri.STAY_WT, 
                    sri.STAY_WTK,          
                    sri.EXPENDITURE_WT, 
                    sri.EXPENDITURE_WTK, 
                    sri.NIGHTS1, 
                    sri.NIGHTS2, 
                    sri.NIGHTS3, 
                    sri.NIGHTS4, 
                    sri.NIGHTS5, 
                    sri.NIGHTS6, 
                    sri.NIGHTS7,
                    sri.NIGHTS8, 
                    sri.STAY1K,          
                    sri.STAY2K,
                    sri.STAY3K,
                    sri.STAY4K, 
                    sri.STAY5K,           
                    sri.STAY6K, 
                    sri.STAY7K, 
                    sri.STAY8K       
            from 
                SAS_REGIONAL_IMP sri        
            where 
                sss.SERIAL = sri.SERIAL)
            """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  
        
    cf.delete_from_table("SAS_REGIONAL_IMP")    


def store_survey_data_with_regional_wt_results(run_id, conn):
    """
    Author       : Thomas Mahoney
    Date         : 20/03/2018
    Purpose      : Stores the survey data with the regional weight results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    sql = """update survey_subsample ss
            set (ss.VISIT_WT, 
                 ss.VISIT_WTK, 
                 ss.STAY_WT, 
                 ss.STAY_WTK,         
                 ss.EXPENDITURE_WT, 
                 ss.EXPENDITURE_WTK, 
                 ss.NIGHTS1,          
                 ss.NIGHTS2 , 
                 ss.NIGHTS3, 
                 ss.NIGHTS4, 
                 ss.NIGHTS5,           
                 ss.NIGHTS6, 
                 ss.NIGHTS7, 
                 ss.NIGHTS8, 
                 ss.STAY1K,          
                 ss.STAY2K, 
                 ss.STAY3K, 
                 ss.STAY4K, 
                 ss.STAY5K,           
                 ss.STAY6K, 
                 ss.STAY7K, 
                 ss.STAY8K, 
                 ss.PURPOSE_PV,           
                 ss.STAYIMPCTRYLEVEL1_PV,  
                 ss.STAYIMPCTRYLEVEL2_PV,           
                 ss.STAYIMPCTRYLEVEL3_PV, 
                 ss.STAYIMPCTRYLEVEL4_PV,           
                 ss.REG_IMP_ELIGIBLE_PV) =
            (select sss.VISIT_WT, 
                    sss.VISIT_WTK, 
                    sss.STAY_WT, 
                    sss.STAY_WTK,          
                    sss.EXPENDITURE_WT, 
                    sss.EXPENDITURE_WTK, 
                    sss.NIGHTS1, 
                    sss.NIGHTS2, 
                    sss.NIGHTS3, 
                    sss.NIGHTS4, 
                    sss.NIGHTS5, 
                    sss.NIGHTS6, 
                    sss.NIGHTS7,
                    sss.NIGHTS8, 
                    sss.STAY1K,          
                    sss.STAY2K,
                    sss.STAY3K,
                    sss.STAY4K, 
                    sss.STAY5K,           
                    sss.STAY6K, 
                    sss.STAY7K, 
                    sss.STAY8K, 
                    sss.PURPOSE_PV,           
                    sss.STAYIMPCTRYLEVEL1_PV,  
                    sss.STAYIMPCTRYLEVEL2_PV,           
                    sss.STAYIMPCTRYLEVEL3_PV, 
                    sss.STAYIMPCTRYLEVEL4_PV,           
                    sss.REG_IMP_ELIGIBLE_PV
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


if __name__ == '__main__':
    pass