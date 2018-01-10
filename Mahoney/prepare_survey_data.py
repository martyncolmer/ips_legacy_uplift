'''
Created on 9 Jan 2018

@author: mahont1
'''
from IPSTransformation import CommonFunctions as cf
import pandas as pd
import sys


def nullify_survey_subsample_pv_values(conn):
    cur = conn.cursor()
    
    # SQL that sets survey_subsample's PV values to null
    sql = """
            update survey_subsample ss 
                set ss.SHIFT_PORT_GRP_PV = null, 
                ss.WEEKDAY_END_PV = null, 
                ss.AM_PM_NIGHT_PV = null, 
                ss.SHIFT_FLAG_PV = null, 
                ss.CROSSINGS_FLAG_PV = null, 
                ss.SHIFT_WT = null        
            where ss.RUN_ID = '""" + run_id + "'"
    
    # Executes and commits the SQL command
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


"""
Step Functions
"""

def populate_survey_data_for_shift_wt(conn):
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")
    cf.delete_from_table("SAS_SHIFT_WT")
    cf.delete_from_table("SAS_PS_SHIFT_DATA")
    
    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def update_survey_data_with_shift_wt_pv_output(conn): 
    
    sql = """update sas_survey_subsample sss       
                set (sss.SHIFT_PORT_GRP_PV, 
                    sss.WEEKDAY_END_PV, 
                    sss.AM_PM_NIGHT_PV, 
                    sss.SHIFT_FLAG_PV, 
                    sss.CROSSINGS_FLAG_PV ) =
                (select sssp.SHIFT_PORT_GRP_PV, 
                    sssp.WEEKDAY_END_PV, 
                    sssp.AM_PM_NIGHT_PV, 
                    sssp.SHIFT_FLAG_PV, 
                    sssp.CROSSINGS_FLAG_PV
                from 
                    sas_shift_spv sssp
                where 
                    sss.SERIAL = sssp.SERIAL)
                """
    
    cur.execute(sql)
    conn.commit()            


def copy_shift_wt_pvs_for_shift_data(conn):
    
    cf.delete_from_table("SAS_PROCESS_VARIABLE")
    cf.delete_from_table("SAS_SHIFT_PV")
    
    sql ="""select 
                pv.PV_NAME, pv.PV_DEF, 0 
            from 
                process_variable pv 
            where 
                pv.RUN_ID = '""" + run_id + """' 
            and 
                upper(pv.PV_NAME) in ('SHIFT_PORT_GRP_PV','WEEKDAY_END_PV','AM_PM_NIGHT_PV')
            """
    
    df_content = pd.read_sql(sql,conn)
    
    cf.insert_into_table_many("SAS_PROCESS_VARIABLE", df_content, conn)
    
    
    #     """            
    #     insert into sas_process_variable ( PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER ) 
    #     (
    #     select 
    #         pv.PV_NAME, pv.PV_DEF, 0                  
    #     from 
    #         process_variable pv 
    #     where 
    #         pv.RUN_ID = '{RID}'
    #     and 
    #         upper(pv.PV_NAME) in ('SHIFT_PORT_GRP_PV','WEEKDAY_END_PV','AM_PM_NIGHT_PV')
    #     ) 
    #     """


def update_survey_data_with_shift_wt_results(conn):
    
    sql = """update sas_survey_subsample sss
            set (sss.SHIFT_WT ) =
            (select 
                ssw.SHIFT_WT        
            from 
                sas_shift_wt ssw        
            where 
                sss.SERIAL = ssw.SERIAL)
            """

    cur.execute(sql)
    conn.commit()  
        
    cf.delete_from_table("SAS_SHIFT_WT")    


def store_survey_data_with_shift_wt_results(conn):
    
    sql = """update survey_subsample ss
            set (ss.SHIFT_PORT_GRP_PV,
                ss.WEEKDAY_END_PV, 
                ss.AM_PM_NIGHT_PV,
                ss.SHIFT_FLAG_PV, 
                ss.CROSSINGS_FLAG_PV, 
                ss.SHIFT_WT ) = 
            (select sss.SHIFT_PORT_GRP_PV, 
                sss.WEEKDAY_END_PV, 
                sss.AM_PM_NIGHT_PV,
                sss.SHIFT_FLAG_PV, 
                sss.CROSSINGS_FLAG_PV, 
                sss.SHIFT_WT 
            from 
                sas_survey_subsample sss         
            where 
                sss.SERIAL = ss.SERIAL)        
            where 
                ss.RUN_ID = '""" + run_id + "'"    

    cur.execute(sql)
    conn.commit()  

    cf.delete_from_table('PS_SHIFT_DATA', 'RUN_ID', '=', run_id)
    
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")  


def store_shift_wt_summary(conn):
    
    cf.delete_from_table('PS_SHIFT_DATA', 'RUN_ID', '=', run_id)

    column_string = "'"+ run_id+"""', 
                    SHIFT_PORT_GRP_PV, 
                    ARRIVEDEPART, 
                    WEEKDAY_END_PV, 
                    AM_PM_NIGHT_PV, 
                    MIGSI, 
                    POSS_SHIFT_CROSS, 
                    SAMP_SHIFT_CROSS, 
                    MIN_SH_WT, 
                    MEAN_SH_WT, 
                    MAX_SH_WT, 
                    COUNT_RESPS, 
                    SUM_SH_WT """     
    
    sql = "select "+column_string+" from sas_ps_shift_data"
    
    df_content = pd.read_sql(sql,conn)
    df_content.columns.values[0] = 'RUN_ID'
    
    cf.insert_into_table_many('PS_SHIFT_DATA', df_content, conn)
    cf.delete_from_table('SAS_PS_SHIFT_DATA')

"""
Process start
"""

run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

conn = cf.get_oracle_connection()
cur = conn.cursor()

# Testing
store_shift_wt_summary(conn)
sys.exit()
#  Populate Survey Data For Shift Wt                   TM

populate_survey_data_for_shift_wt(conn)


# Populate Shift Data                                  RR


# Copy Shift Wt PVs For Survey Data                    RR


# Apply Shift Wt PVs On Survey Data                    X


# Update Survey Data with Shift Wt PV Output           TM

update_survey_data_with_shift_wt_pv_output(conn)


# Copy Shift Wt PVs For Shift Data                     TM

copy_shift_wt_pvs_for_shift_data(conn)


# Apply Shift Wt PVs On Shift Data                     X


# Update Shift Data with PVs Output                    RR


# Calculate Shift Weight                               X 


# Update Survey Data With Shift Wt Results             TM

update_survey_data_with_shift_wt_results(conn)

# Store Survey Data With Shift Wt Results              TM

store_survey_data_with_shift_wt_results(conn)

# Store Shift Wt Summary                               TM 

store_shift_wt_summary(conn)













