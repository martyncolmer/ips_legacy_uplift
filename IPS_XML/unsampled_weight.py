'''
Created on 9 Jan 2018

@author: mahont1
'''
from main.io import CommonFunctions as cf


def populate_survey_data_for_unsampled_wt(run_id, conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Populates Survey Data for Unsampled Wt
    Parameters   : run_id - Run ID
                 : conn - connection object pointing at the database 
    Returns      : NA
    """
    
    sas_survey_subsample = "SAS_SURVEY_SUBSAMPLE"
    sas_unsampled_ooh_wt = "SAS_UNSAMPLED_OOH_WT"
    sas_ps_unsampled_ooh = "SAS_PS_UNSAMPLED_OOH"
    survey_subsample = "SURVEY_SUBSAMPLE"
    
    cf.delete_from_table(sas_survey_subsample)       
    cf.delete_from_table(sas_unsampled_ooh_wt)
    cf.delete_from_table(sas_ps_unsampled_ooh)              

    sql1 = """UPDATE """ + survey_subsample + """ ss        
                        SET ss.UNSAMP_PORT_GRP_PV = null, 
                        ss.UNSAMP_REGION_GRP_PV = null,         
                        ss.UNSAMP_TRAFFIC_WT = null        
                    WHERE ss.RUN_ID = '""" + run_id + """'"""
    
    cur = conn.cursor()
    cur.execute(sql1)
    conn.commit()
    
    sql2 = """INSERT INTO """ + sas_survey_subsample + """       
                        (SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG
                        , APORTLATMIN, APORTLATSEC, APORTLATNS, APORTLONDEG
                        , APORTLONMIN, APORTLONSEC, APORTLONEW, ARRIVEDEPART
                        , BABYFARE, BEFAF, CHANGECODE, CHILDFARE, COUNTRYVISIT
                        , CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS
                        , CPORTLONDEG, CPORTLONMIN, CPORTLONSEC, CPORTLONEW
                        , INTDATE, DAYTYPE, DIRECTLEG, DVEXPEND, DVFARE, DVLINECODE
                        , DVPACKAGE, DVPACKCOST, DVPERSONS, DVPORTCODE, EXPENDCODE
                        , EXPENDITURE, FARE, FAREK, FLOW, HAULKEY, INTENDLOS
                        , KIDAGE, LOSKEY, MAINCONTRA, MIGSI, INTMONTH, NATIONALITY
                        , NATIONNAME, NIGHTS1, NIGHTS2, NIGHTS3, NIGHTS4, NIGHTS5
                        , NIGHTS6, NIGHTS7, NIGHTS8, NUMADULTS, NUMDAYS, NUMNIGHTS
                        , NUMPEOPLE, PACKAGEHOL, PACKAGEHOLUK, PERSONS, PORTROUTE
                        , PACKAGE, PROUTELATDEG, PROUTELATMIN, PROUTELATSEC, PROUTELATNS
                        , PROUTELONDEG, PROUTELONMIN, PROUTELONSEC, PROUTELONEW
                        , PURPOSE, QUARTER, RESIDENCE, RESPNSE, SEX, SHIFTNO, SHUTTLE
                        , SINGLERETURN, TANDTSI, TICKETCOST, TOWNCODE1, TOWNCODE2
                        , TOWNCODE3, TOWNCODE4, TOWNCODE5, TOWNCODE6, TOWNCODE7
                        , TOWNCODE8, TRANSFER, UKFOREIGN, VEHICLE, VISITBEGAN
                        , WELSHNIGHTS, WELSHTOWN, AM_PM_NIGHT_PV, APD_PV, ARRIVEDEPART_PV
                        , CROSSINGS_FLAG_PV, STAYIMPCTRYLEVEL1_PV, STAYIMPCTRYLEVEL2_PV
                        , STAYIMPCTRYLEVEL3_PV, STAYIMPCTRYLEVEL4_PV, DAY_PV
                        , DISCNT_F1_PV, DISCNT_F2_PV, DISCNT_PACKAGE_COST_PV, DUR1_PV
                        , DUR2_PV, DUTY_FREE_PV, FAGE_PV, FARES_IMP_ELIGIBLE_PV
                        , FARES_IMP_FLAG_PV, FLOW_PV, FOOT_OR_VEHICLE_PV, HAUL_PV
                        , IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV, IMBAL_ELIGIBLE_PV
                        , IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV, IMBAL_PORT_SUBGRP_PV
                        , LOS_PV, LOSDAYS_PV, MIG_FLAG_PV, MINS_CTRY_GRP_PV
                        , MINS_CTRY_PORT_GRP_PV, MINS_FLAG_PV, MINS_NAT_GRP_PV
                        , MINS_PORT_GRP_PV, MINS_QUALITY_PV, NR_FLAG_PV, NR_PORT_GRP_PV
                        , OPERA_PV, OSPORT1_PV, OSPORT2_PV, OSPORT3_PV, OSPORT4_PV
                        , PUR1_PV, PUR2_PV, PUR3_PV, PURPOSE_PV, QMFARE_PV
                        , RAIL_EXERCISE_PV, RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV
                        , SHIFT_FLAG_PV, SHIFT_PORT_GRP_PV, STAY_IMP_ELIGIBLE_PV
                        , STAY_IMP_FLAG_PV, STAY_PURPOSE_GRP_PV, TOWNCODE_PV, TYPE_PV
                        , UK_OS_PV, UKPORT1_PV, UKPORT2_PV, UKPORT3_PV, UKPORT4_PV
                        , UNSAMP_PORT_GRP_PV, UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV
                        , DIRECT, EXPENDITURE_WT, EXPENDITURE_WTK, OVLEG, SPEND
                        , SPEND1, SPEND2, SPEND3, SPEND4, SPEND5, SPEND6, SPEND7
                        , SPEND8, SPEND9, SPENDIMPREASON, SPENDK, STAY, STAYK
                        , STAY1K, STAY2K, STAY3K, STAY4K, STAY5K, STAY6K, STAY7K
                        , STAY8K, STAY9K, STAYTLY, STAY_WT,  STAY_WTK, UKLEG, VISIT_WT
                        , VISIT_WTK, SHIFT_WT, NON_RESPONSE_WT, MINS_WT, TRAFFIC_WT
                        , UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT, FAREKEY, TYPEINTERVIEW)
                (SELECT 
                SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG, APORTLATMIN
                , APORTLATSEC, APORTLATNS, APORTLONDEG, APORTLONMIN, APORTLONSEC
                , APORTLONEW, ARRIVEDEPART, BABYFARE, BEFAF, CHANGECODE, CHILDFARE
                , COUNTRYVISIT, CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS
                , CPORTLONDEG, CPORTLONMIN, CPORTLONSEC, CPORTLONEW, INTDATE, DAYTYPE
                , DIRECTLEG, DVEXPEND, DVFARE, DVLINECODE, DVPACKAGE, DVPACKCOST
                , DVPERSONS, DVPORTCODE, EXPENDCODE, EXPENDITURE, FARE, FAREK, FLOW
                , HAULKEY, INTENDLOS, KIDAGE, LOSKEY, MAINCONTRA, MIGSI, INTMONTH
                , NATIONALITY, NATIONNAME, NIGHTS1, NIGHTS2, NIGHTS3, NIGHTS4, NIGHTS5
                , NIGHTS6, NIGHTS7, NIGHTS8, NUMADULTS, NUMDAYS, NUMNIGHTS
                , NUMPEOPLE, PACKAGEHOL, PACKAGEHOLUK, PERSONS, PORTROUTE, PACKAGE
                , PROUTELATDEG, PROUTELATMIN, PROUTELATSEC, PROUTELATNS, PROUTELONDEG
                , PROUTELONMIN, PROUTELONSEC, PROUTELONEW, PURPOSE, QUARTER, RESIDENCE
                , RESPNSE, SEX, SHIFTNO, SHUTTLE, SINGLERETURN, TANDTSI, TICKETCOST
                , TOWNCODE1, TOWNCODE2, TOWNCODE3, TOWNCODE4, TOWNCODE5, TOWNCODE6
                , TOWNCODE7, TOWNCODE8, TRANSFER, UKFOREIGN, VEHICLE, VISITBEGAN
                , WELSHNIGHTS, WELSHTOWN, AM_PM_NIGHT_PV, APD_PV,ARRIVEDEPART_PV
                , CROSSINGS_FLAG_PV, STAYIMPCTRYLEVEL1_PV, STAYIMPCTRYLEVEL2_PV
                , STAYIMPCTRYLEVEL3_PV, STAYIMPCTRYLEVEL4_PV, DAY_PV, DISCNT_F1_PV
                , DISCNT_F2_PV, DISCNT_PACKAGE_COST_PV, DUR1_PV, DUR2_PV, DUTY_FREE_PV
                , FAGE_PV, FARES_IMP_ELIGIBLE_PV, FARES_IMP_FLAG_PV, FLOW_PV
                , FOOT_OR_VEHICLE_PV, HAUL_PV, IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV
                , IMBAL_ELIGIBLE_PV, IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV
                , IMBAL_PORT_SUBGRP_PV, LOS_PV, LOSDAYS_PV, MIG_FLAG_PV
                , MINS_CTRY_GRP_PV, MINS_CTRY_PORT_GRP_PV, MINS_FLAG_PV, MINS_NAT_GRP_PV
                , MINS_PORT_GRP_PV, MINS_QUALITY_PV, NR_FLAG_PV, NR_PORT_GRP_PV
                , OPERA_PV, OSPORT1_PV, OSPORT2_PV, OSPORT3_PV, OSPORT4_PV, PUR1_PV
                , PUR2_PV, PUR3_PV, PURPOSE_PV, QMFARE_PV, RAIL_EXERCISE_PV
                , RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV, SHIFT_FLAG_PV
                , SHIFT_PORT_GRP_PV, STAY_IMP_ELIGIBLE_PV, STAY_IMP_FLAG_PV
                , STAY_PURPOSE_GRP_PV, TOWNCODE_PV, TYPE_PV, UK_OS_PV, UKPORT1_PV
                , UKPORT2_PV, UKPORT3_PV, UKPORT4_PV, UNSAMP_PORT_GRP_PV
                , UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV, DIRECT, EXPENDITURE_WT
                , EXPENDITURE_WTK, OVLEG, SPEND, SPEND1, SPEND2, SPEND3, SPEND4
                , SPEND5, SPEND6, SPEND7, SPEND8, SPEND9, SPENDIMPREASON, SPENDK
                , STAY, STAYK, STAY1K, STAY2K, STAY3K, STAY4K, STAY5K, STAY6K
                , STAY7K,  STAY8K, STAY9K, STAYTLY, STAY_WT, STAY_WTK, UKLEG
                , VISIT_WT, VISIT_WTK, SHIFT_WT, NON_RESPONSE_WT, MINS_WT, TRAFFIC_WT
                , UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT, FAREKEY, TYPEINTERVIEW 
                FROM """ + survey_subsample + """WHERE RUN_ID = '""" + run_id + """' 
                AND SERIAL NOT LIKE '9999%' AND RESPNSE BETWEEN 1 AND 2)"""
    
    cur = conn.cursor()
    cur.execute(sql2)
    conn.commit()


def populate_unsampled_data(run_id, conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Populates unsampled data
    Parameters   : run_id - Run ID
                 : conn - connection object pointing at the database
    Returns      : NA
    """

    sas_unsampled_ooh_data_table = 'SAS_UNSAMPLED_OOH_DATA'
    unsampled_ooh_data_table = "UNSAMPLED_OOH_DATA"

    sas_unsampled_data_insert_query = """INSERT INTO """ + sas_unsampled_ooh_data_table + """       
                                            (REC_ID, PORTROUTE, REGION, ARRIVEDEPART
                                            , UNSAMP_TOTAL)
                                        (SELECT REC_ID_S.nextval, uod.PORTROUTE
                                            , uod.REGION, uod.ARRIVEDEPART, uod.UNSAMP_TOTAL
                                        FROM """ + unsampled_ooh_data_table + """ uod
                                        WHERE uod.RUN_ID = '""" + run_id + "')"

    cf.delete_from_table(sas_unsampled_ooh_data_table)

    cur = conn.cursor()
    cur.execute(sas_unsampled_data_insert_query)
    conn.commit()

    
def copy_unsampled_wt_pvs_for_survey_data(run_id, conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Copies unsampled weight process variables for survey data
    Parameters   : run_id - Run ID
                 : conn - connection object pointing at the database
    Returns      : NA
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_unsampled_ooh_spv_table = 'SAS_UNSAMPLED_OOH_SPV'
    process_variable_table = "PROCESS_VARIABLE"
    
    sas_process_variable_1_insert_query = """INSERT INTO """ + sas_process_variable_table + """
                                                    (PROCVAR_NAME, PROCVAR_RULE
                                                    , PROCVAR_ORDER)
                                                (SELECT pspv.PV_NAME, pspv.PV_DEF, 1 
                                                FROM """ + process_variable_table + """ pspv
                                                WHERE pspv.RUN_ID = '""" + run_id + """'
                                                AND UPPER(pspv.PV_NAME) IN ('UNSAMP_PORT_GRP_PV'))"""          

    sas_process_variable_2_insert_query = """INSERT INTO """ + sas_process_variable_table + """
                                                    (PROCVAR_NAME, PROCVAR_RULE
                                                    , PROCVAR_ORDER)
                                                (SELECT pspv.PV_NAME, pspv.PV_DEF, 2
                                                FROM """ + process_variable_table + """ pspv
                                                WHERE pspv.RUN_ID = '""" + run_id + """'
                                                AND UPPER(pspv.PV_NAME) IN ('UNSAMP_REGION_GRP_PV'))"""
    
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_unsampled_ooh_spv_table)
    
    cur = conn.cursor()
    cur.execute(sas_process_variable_1_insert_query)
    cur.execute(sas_process_variable_2_insert_query)
    conn.commit()


def update_survey_data_with_unsampled_wt_pv_output(conn): 
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Updates survey_data with the unsampled weight process variable 
                 : output
    Parameters   : conn - connection object pointing at the database
    Returns      : N/A
    """
    
    sas_survey_subsample_table = "SAS_SURVEY_SUBSAMPLE"
    sas_unsampled_ooh_spv_table = "SAS_UNSAMPLED_OOH_SPV"
    sas_process_variable_table = "SAS_PROCESS_VARIABLE"
    
    sas_survey_subsample_update = """UPDATE """ + sas_survey_subsample_table + """ ss       
                    SET (ss.UNSAMP_PORT_GRP_PV, ss.UNSAMP_REGION_GRP_PV) =        
                    (SELECT suos.UNSAMP_PORT_GRP_PV, suos.UNSAMP_REGION_GRP_PV        
                    FROM """ + sas_unsampled_ooh_spv_table + """ suos        
                    WHERE ss.SERIAL = suos.SERIAL)
                    """
    cur = conn.cursor()
    cur.execute(sas_survey_subsample_update)
    conn.commit()
    
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_unsampled_ooh_spv_table)            


def copy_unsampled_wt_pvs_for_unsampled_data(run_id, conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Copies the unsampeld weight process variables for unsampled_data
    Parameters   : run_id - Run ID
                 : conn - connection object pointing at the database
    Returns      : NA
    """
    
    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_unsampled_ooh_pv_table = 'SAS_UNSAMPLED_OOH_PV_TABLE'
    process_variable_table = "PROCESS_VARIABLE"
    
    sas_process_variable_def1_insert_query = """INSERT INTO """ + sas_process_variable_table + """
                                        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)
                                        (SELECT pspv.PV_NAME, pspv.PV_DEF, 1        
                                        FROM """ + process_variable_table + """ pspv
                                        WHERE pspv.RUN_ID = '""" + run_id + """'
                                        AND UPPER(pspv.PV_NAME) IN ('UNSAMP_PORT_GRP_PV'))"""
    
    sas_process_variable_def2_insert_query = """INSERT INTO """ + sas_process_variable_table + """
                                        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)
                                        (SELECT pspv.PV_NAME, pspv.PV_DEF, 2
                                        FROM """ + process_variable_table + """ pspv
                                        WHERE pspv.RUN_ID = '""" + run_id + """'
                                        AND UPPER(pspv.PV_NAME) IN ('UNSAMP_REGION_GRP_PV'))"""
    
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_unsampled_ooh_pv_table)
    
    cur = conn.cursor()
    cur.execute(sas_process_variable_def1_insert_query)
    cur.execute(sas_process_variable_def2_insert_query)
    conn.commit()  


def update_unsampled_data_with_pv_output(conn): 
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Updates unsampled data with the process variable output
    Parameters   : conn - connection object pointing at the database
    Returns      : NA
    """

    sas_unsampled_ooh_data_table = "SAS_UNSAMPLED_OOH_DATA"
    sas_unsampled_ooh_pv_table = "SAS_UNSAMPLED_OOH_PV"
    sas_unsampled_ooh_wt_table = "SAS_UNSAMPLED_OOH_WT"
    sas_process_variable_table = "SAS_PROCESS_VARIABLE"
    sas_ps_unsampled_ooh_table = "SAS_PS_UNSAMPLED_OOH"
    
    sas_unsampled_ooh_data_update = """UPDATE """ + sas_unsampled_ooh_data_table + """ suod
                                        SET (suod.UNSAMP_PORT_GRP_PV
                                            , suod.UNSAMP_REGION_GRP_PV ) =
                                        (SELECT suop.UNSAMP_PORT_GRP_PV
                                            , suop.UNSAMP_REGION_GRP_PV
                                        FROM """ + sas_unsampled_ooh_pv_table + """ suop
                                        WHERE suod.REC_ID = suop.REC_ID)"""
    
    cur = conn.cursor()
    cur.execute(sas_unsampled_ooh_data_update)
    conn.commit()  
    
    cf.delete_from_table(sas_unsampled_ooh_pv_table)
    cf.delete_from_table(sas_unsampled_ooh_wt_table)
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_ps_unsampled_ooh_table)
    

def update_survey_data_with_unsampled_wt_results(conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Updates survey_data with the unsampled weight results
    Parameters   : conn - connection object pointing at the database
    Returns      : NA
    """
    
    sas_survey_subsample_table = "SAS_SURVEY_SUBSAMPLE"
    sas_unsampled_ooh_wt_table = "SAS_UNSAMPLED_OOH_WT"
    
    sql = """UPDATE """ + sas_survey_subsample_table + """ sss 
                SET (sss.UNSAMP_TRAFFIC_WT) = (SELECT suow.UNSAMP_TRAFFIC_WT
                FROM """ + sas_unsampled_ooh_wt_table + """ suow
                WHERE sss.SERIAL = suow.SERIAL)"""
                
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  
        
    cf.delete_from_table(sas_unsampled_ooh_wt_table)    


def store_survey_data_with_unsampled_wt_results(run_id, conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Stores the survey data with the unsampled weight results
    Parameters   : run_id - Run ID
                 : conn - connection object pointing at the database
    Returns      : NA
    """
    
    survey_subsample_table = "SURVEY_SUBSAMPLE"
    sas_survey_subsample_table = "SAS_SURVEY_SUBSAMPLE"
    ps_unsampled_ooh_table = "PS_UNSAMPLED_OOH"
    
    sql = """UPDATE """ + survey_subsample_table + """ ss
            SET (ss.UNSAMP_PORT_GRP_PV, ss.UNSAMP_REGION_GRP_PV
                , ss.UNSAMP_TRAFFIC_WT ) = 
            (SELECT sss.UNSAMP_PORT_GRP_PV
                , sss.UNSAMP_REGION_GRP_PV, sss.UNSAMP_TRAFFIC_WT
                FROM """ + sas_survey_subsample_table + """ sss
                WHERE sss.SERIAL = ss.SERIAL)
                WHERE ss.RUN_ID = '""" + run_id + """'"""    

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  

    cf.delete_from_table(ps_unsampled_ooh_table, 'RUN_ID', '=', run_id)
    cf.delete_from_table(sas_survey_subsample_table)  


def store_unsampled_wt_summary(run_id, conn):
    """
    Author       : Elinor Thorne
    Date         : March 2018
    Purpose      : Stores the unsampled weight summary
    Parameters   : run_id - Run ID
                 : conn - connection object pointing at the database
    Returns      : NA
    """
    
    ps_unsampled_ooh_table = "PS_UNSAMPLED_OOH"
    sas_ps_unsampled_ooh_table = "SAS_PS_UNSAMPLED_OOH"
    
    cf.delete_from_table(ps_unsampled_ooh_table, 'RUN_ID', '=', run_id)

    sql = """INSERT INTO """ + ps_unsampled_ooh_table + """(RUN_ID, UNSAMP_PORT_GRP_PV
                , ARRIVEDEPART, UNSAMP_REGION_GRP_PV, CASES, SUM_PRIOR_WT
                , SUM_UNSAMP_TRAFFIC_WT, UNSAMP_TRAFFIC_WT)(SELECT '""" + run_id + """'
                , UNSAMP_PORT_GRP_PV, ARRIVEDEPART, UNSAMP_REGION_GRP_PV, CASES
                , SUM_PRIOR_WT, SUM_UNSAMP_TRAFFIC_WT, UNSAMP_TRAFFIC_WT
                FROM """ + sas_ps_unsampled_ooh_table + """)"""
                
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()  
    
    cf.delete_from_table(sas_ps_unsampled_ooh_table)


def run_all(run_id, conn):
    """
    Author        : Elinor Thorne
    Date          : Mar 2018
    Purpose       : Hard Coded for now - runs all above functions for 
                  : Step 5, Unsmapled Weight
    Parameters    : run_id - Run ID
                  : conn - connection object pointing at the database 
    Returns       : N/A   
    """

    populate_survey_data_for_unsampled_wt(run_id, conn)
    populate_unsampled_data(run_id, conn)
    copy_unsampled_wt_pvs_for_survey_data(run_id, conn)
    update_survey_data_with_unsampled_wt_pv_output(run_id, conn)
    copy_unsampled_wt_pvs_for_unsampled_data(run_id, conn)
    update_unsampled_data_with_pv_output()
    update_survey_data_with_unsampled_wt_results(run_id, conn)
    store_survey_data_with_unsampled_wt_results(run_id, conn)
    store_unsampled_wt_summary(run_id, conn)


if __name__ == '__main__':
    run_id = ""
    conn = cf.get_oracle_connection()
    update_survey_data_with_unsampled_wt_pv_output(conn)
