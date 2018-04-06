'''
Created on 20 Mar 2018

@author: thorne1
'''
from main.io import CommonFunctions as cf


def populate_survey_data_for_final_wt(run_id, conn):
    """
    Author        : Elinor Thorne
    Date          : 21 Mar 2018
    Purpose       : Populate survey data for final weight 
    Parameters    : run_id - Run ID
                  : conn - connection object pointing at the database
    Returns       : N/A  
    """
    
    sas_final_wt = "SAS_FINAL_WT"
    sas_ps_final = "SAS_PS_FINAL"
    sas_survey_subsample = "SAS_SURVEY_SUBSAMPLE"
    survey_subsample = "SURVEY_SUBSAMPLE"
    
    cf.delete_from_table(sas_final_wt)
    cf.delete_from_table(sas_ps_final)
    
    sql1 = """update survey_subsample
                set final_wt = null
            where RUN_ID = '""" + run_id + """'"""
            
    cur = conn.cursor()
    cur.execute(sql1)
    conn.commit()
    
    cf.delete_from_table(sas_survey_subsample)

    sql2 = """INSERT INTO """ + sas_survey_subsample + """(
                    SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG, APORTLATMIN
                    , APORTLATSEC, APORTLATNS, APORTLONDEG, APORTLONMIN, APORTLONSEC
                    , APORTLONEW, ARRIVEDEPART, BABYFARE, BEFAF, CHANGECODE, CHILDFARE
                    , COUNTRYVISIT, CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS
                    , CPORTLONDEG, CPORTLONMIN, CPORTLONSEC, CPORTLONEW, INTDATE
                    , DAYTYPE, DIRECTLEG, DVEXPEND, DVFARE, DVLINECODE, DVPACKAGE
                    , DVPACKCOST, DVPERSONS, DVPORTCODE, EXPENDCODE, EXPENDITURE
                    , FARE, FAREK, FLOW, HAULKEY, INTENDLOS, KIDAGE, LOSKEY, MAINCONTRA
                    , MIGSI, INTMONTH, NATIONALITY, NATIONNAME, NIGHTS1, NIGHTS2, NIGHTS3
                    , NIGHTS4, NIGHTS5, NIGHTS6, NIGHTS7, NIGHTS8, NUMADULTS, NUMDAYS
                    , NUMNIGHTS, NUMPEOPLE, PACKAGEHOL, PACKAGEHOLUK, PERSONS, PORTROUTE
                    , PACKAGE, PROUTELATDEG, PROUTELATMIN, PROUTELATSEC, PROUTELATNS
                    , PROUTELONDEG, PROUTELONMIN, PROUTELONSEC, PROUTELONEW, PURPOSE
                    , QUARTER, RESIDENCE, RESPNSE, SEX, SHIFTNO, SHUTTLE, SINGLERETURN
                    , TANDTSI, TICKETCOST, TOWNCODE1, TOWNCODE2, TOWNCODE3, TOWNCODE4
                    , TOWNCODE5, TOWNCODE6, TOWNCODE7, TOWNCODE8, TRANSFER, UKFOREIGN
                    , VEHICLE, VISITBEGAN, WELSHNIGHTS, WELSHTOWN, AM_PM_NIGHT_PV
                    , APD_PV, ARRIVEDEPART_PV, CROSSINGS_FLAG_PV, STAYIMPCTRYLEVEL1_PV
                    , STAYIMPCTRYLEVEL2_PV, STAYIMPCTRYLEVEL3_PV, STAYIMPCTRYLEVEL4_PV
                    , DAY_PV, DISCNT_F1_PV, DISCNT_F2_PV, DISCNT_PACKAGE_COST_PV
                    , DUR1_PV, DUR2_PV, DUTY_FREE_PV, FAGE_PV, FARES_IMP_ELIGIBLE_PV
                    , FARES_IMP_FLAG_PV, FLOW_PV, FOOT_OR_VEHICLE_PV, HAUL_PV
                    , IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV, IMBAL_ELIGIBLE_PV
                    , IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV, IMBAL_PORT_SUBGRP_PV
                    , LOS_PV, LOSDAYS_PV, MIG_FLAG_PV, MINS_CTRY_GRP_PV, MINS_CTRY_PORT_GRP_PV
                    , MINS_FLAG_PV, MINS_NAT_GRP_PV, MINS_PORT_GRP_PV, MINS_QUALITY_PV
                    , NR_FLAG_PV, NR_PORT_GRP_PV, OPERA_PV, OSPORT1_PV, OSPORT2_PV
                    , OSPORT3_PV, OSPORT4_PV, PUR1_PV, PUR2_PV, PUR3_PV, PURPOSE_PV
                    , QMFARE_PV, RAIL_EXERCISE_PV, RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV
                    , SHIFT_FLAG_PV, SHIFT_PORT_GRP_PV, STAY_IMP_ELIGIBLE_PV
                    , STAY_IMP_FLAG_PV, STAY_PURPOSE_GRP_PV, TOWNCODE_PV, TYPE_PV
                    , UK_OS_PV, UKPORT1_PV, UKPORT2_PV, UKPORT3_PV, UKPORT4_PV
                    , UNSAMP_PORT_GRP_PV, UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV
                    , DIRECT, EXPENDITURE_WT, EXPENDITURE_WTK, OVLEG, SPEND, SPEND1
                    , SPEND2, SPEND3, SPEND4, SPEND5, SPEND6, SPEND7, SPEND8, SPEND9
                    , SPENDIMPREASON, SPENDK, STAY, STAYK, STAY1K, STAY2K, STAY3K
                    , STAY4K, STAY5K, STAY6K, STAY7K,  STAY8K, STAY9K, STAYTLY, STAY_WT
                    , STAY_WTK, UKLEG, VISIT_WT, VISIT_WTK, SHIFT_WT, NON_RESPONSE_WT
                    , MINS_WT, TRAFFIC_WT, UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT
                    , FAREKEY, TYPEINTERVIEW)
                (SELECT SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG, APORTLATMIN
                , APORTLATSEC, APORTLATNS, APORTLONDEG, APORTLONMIN, APORTLONSEC
                , APORTLONEW, ARRIVEDEPART, BABYFARE, BEFAF, CHANGECODE, CHILDFARE
                , COUNTRYVISIT, CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS
                , CPORTLONDEG, CPORTLONMIN, CPORTLONSEC, CPORTLONEW, INTDATE, DAYTYPE
                , DIRECTLEG, DVEXPEND, DVFARE, DVLINECODE, DVPACKAGE, DVPACKCOST
                , DVPERSONS, DVPORTCODE, EXPENDCODE, EXPENDITURE, FARE, FAREK, FLOW
                , HAULKEY, INTENDLOS, KIDAGE, LOSKEY, MAINCONTRA, MIGSI, INTMONTH
                , NATIONALITY, NATIONNAME, NIGHTS1, NIGHTS2, NIGHTS3, NIGHTS4
                , NIGHTS5, NIGHTS6, NIGHTS7, NIGHTS8, NUMADULTS, NUMDAYS, NUMNIGHTS
                , NUMPEOPLE, PACKAGEHOL, PACKAGEHOLUK, PERSONS, PORTROUTE, PACKAGE
                , PROUTELATDEG, PROUTELATMIN, PROUTELATSEC, PROUTELATNS, PROUTELONDEG
                , PROUTELONMIN, PROUTELONSEC, PROUTELONEW, PURPOSE, QUARTER, RESIDENCE
                , RESPNSE, SEX, SHIFTNO, SHUTTLE, SINGLERETURN, TANDTSI, TICKETCOST
                , TOWNCODE1, TOWNCODE2, TOWNCODE3, TOWNCODE4, TOWNCODE5, TOWNCODE6
                , TOWNCODE7, TOWNCODE8, TRANSFER, UKFOREIGN, VEHICLE, VISITBEGAN
                , WELSHNIGHTS, WELSHTOWN, AM_PM_NIGHT_PV, APD_PV, ARRIVEDEPART_PV
                , CROSSINGS_FLAG_PV, STAYIMPCTRYLEVEL1_PV, STAYIMPCTRYLEVEL2_PV
                , STAYIMPCTRYLEVEL3_PV, STAYIMPCTRYLEVEL4_PV, DAY_PV, DISCNT_F1_PV
                , DISCNT_F2_PV, DISCNT_PACKAGE_COST_PV, DUR1_PV, DUR2_PV, DUTY_FREE_PV
                , FAGE_PV, FARES_IMP_ELIGIBLE_PV, FARES_IMP_FLAG_PV, FLOW_PV
                , FOOT_OR_VEHICLE_PV, HAUL_PV, IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV
                , IMBAL_ELIGIBLE_PV, IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV
                , IMBAL_PORT_SUBGRP_PV, LOS_PV, LOSDAYS_PV, MIG_FLAG_PV
                , MINS_CTRY_GRP_PV, MINS_CTRY_PORT_GRP_PV, MINS_FLAG_PV
                , MINS_NAT_GRP_PV, MINS_PORT_GRP_PV, MINS_QUALITY_PV, NR_FLAG_PV
                , NR_PORT_GRP_PV, OPERA_PV, OSPORT1_PV, OSPORT2_PV, OSPORT3_PV
                , OSPORT4_PV, PUR1_PV, PUR2_PV, PUR3_PV, PURPOSE_PV, QMFARE_PV
                , RAIL_EXERCISE_PV, RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV
                , SHIFT_FLAG_PV, SHIFT_PORT_GRP_PV, STAY_IMP_ELIGIBLE_PV
                , STAY_IMP_FLAG_PV, STAY_PURPOSE_GRP_PV, TOWNCODE_PV, TYPE_PV
                , UK_OS_PV, UKPORT1_PV, UKPORT2_PV, UKPORT3_PV, UKPORT4_PV
                , UNSAMP_PORT_GRP_PV, UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV
                , DIRECT, EXPENDITURE_WT, EXPENDITURE_WTK, OVLEG, SPEND, SPEND1
                , SPEND2, SPEND3, SPEND4, SPEND5, SPEND6, SPEND7, SPEND8, SPEND9
                , SPENDIMPREASON, SPENDK, STAY, STAYK, STAY1K, STAY2K, STAY3K
                , STAY4K, STAY5K, STAY6K, STAY7K,  STAY8K, STAY9K, STAYTLY
                , STAY_WT,  STAY_WTK, UKLEG, VISIT_WT, VISIT_WTK, SHIFT_WT
                , NON_RESPONSE_WT, MINS_WT, TRAFFIC_WT, UNSAMP_TRAFFIC_WT, IMBAL_WT
                , FINAL_WT, FAREKEY, TYPEINTERVIEW 
                FROM """ + survey_subsample + """ ss WHERE ss.RUN_ID = '""" + run_id + """' 
                AND ss.SERIAL NOT LIKE '9999%' AND ss.RESPNSE BETWEEN 1 AND 6)"""
                
    cur = conn.cursor()
    cur.execute(sql2)
    conn.commit()


def calculate_final_weight(conn):
    """
    Author        : Elinor Thorne
    Date          : 21 Mar 2018
    Purpose       : Calculates final weight 
    Parameters    : conn - connection object pointing at the database
    Returns       : N/A  
    """
    pass


def update_survey_data_with_final_wt_results(conn):
    """
    Author        : Elinor Thorne
    Date          : 21 Mar 2018
    Purpose       : Updates survey data with final weight results
    Parameters    : conn - connection object pointing at the database
    Returns       : N/A  
    """
    
    sas_final_wt = "SAS_FINAL_WT"
    sas_survey_subsample = "SAS_SURVEY_SUBSAMPLE"
    
    sql = """UPDATE """ + sas_survey_subsample + """ sss       
            SET (sss.FINAL_WT) =        
            (SELECT sfw.FINAL_WT 
            FROM """ + sas_final_wt + """ sfw  
            WHERE sss.SERIAL = sfw.SERIAL)"""

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()   
    
    cf.delete_from_table(sas_final_wt);     


def store_survey_data_with_final_wt_results(run_id, conn):
    """
    Author        : Elinor Thorne
    Date          : 21 Mar 2018
    Purpose       : Stores survey data with final weight results
    Parameters    : run_id - Run ID
                  : conn - connection object pointing at the database
    Returns       : N/A  
    """
    
    ps_final = "PS_FINAL"
    sas_survey_subsample = "SAS_SURVEY_SUBSAMPLE"
    survey_subsample = "SURVEY_SUBSAMPLE"
    
    sql = """UPDATE """ + survey_subsample + """ ss       
            SET ss.final_wt = 
            (SELECT sss.final_wt
            FROM """ + sas_survey_subsample + """ sss
            WHERE sss.SERIAL = ss.SERIAL)
            WHERE ss.RUN_ID = """ + run_id 
    
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
            
            
    cf.delete_from_table(ps_final, "RUN_ID", "=", run_id)
    cf.delete_from_table(sas_survey_subsample) 


def store_final_weight_summary(run_id, conn):
    """
    Author        : Elinor Thorne
    Date          : 21 Mar 2018
    Purpose       : Stores final weight summary
    Parameters    : run_id - Run ID
                  : conn - connection object pointing at the database
    Returns       : N/A  
    """
    
    ps_final = "PS_FINAL"
    sas_ps_final = "SAS_PS_FINAL"
    
    cf.delete_from_table(ps_final, "RUN_ID", "=", run_id)

    sql = """INSERT INTO """ + ps_final + """ 
            (""" + run_id + """, SERIAL, SHIFT_WT, NON_RESPONSE_WT, MINS_WT
            , TRAFFIC_WT, UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT)
            (SELECT """ + run_id + """, SERIAL, SHIFT_WT, NON_RESPONSE_WT
            , MINS_WT, TRAFFIC_WT, UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT
            FROM """ + sas_ps_final + """)"""
            
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    
    cf.delete_from_table(sas_ps_final)
    
    
def run_all(run_id,conn):
    """
    Author        : Elinor Thorne
    Date          : Mar 2018
    Purpose       : Hard Coded for now - runs all above functions for 
                  : Step 7, Final Weight
    Parameters    : run_id - Run ID
                  : conn - connection object pointing at the database 
    Returns       : N/A   
    """
    populate_survey_data_for_final_wt(conn)
    calculate_final_weight(conn)
    update_survey_data_with_final_wt_results(conn)
    store_survey_data_with_final_wt_results(run_id, conn)                                                     
    store_final_weight_summary(run_id,conn)
    

if __name__ == "__main__":
#    run_all(run_id,conn)
    pass 