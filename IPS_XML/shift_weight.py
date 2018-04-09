'''
Created on 9 Jan 2018

@author: mahont1
'''
from main.io import CommonFunctions as cf
import pandas as pd
from main.utils import process_variables


def populate_survey_data_for_shift_wt(run_id, conn):
    """
    Author       : Thomas Mahoney
    Date         : 10 Jan 2017
    Purpose      : Populates survey_data in preperation for shift weight 
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
                    set ss.SHIFT_PORT_GRP_PV = null, 
                    ss.WEEKDAY_END_PV = null, 
                    ss.AM_PM_NIGHT_PV = null, 
                    ss.SHIFT_FLAG_PV = null, 
                    ss.CROSSINGS_FLAG_PV = null, 
                    ss.SHIFT_WT = null        
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

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")
    cf.delete_from_table("SAS_SHIFT_WT")
    cf.delete_from_table("SAS_PS_SHIFT_DATA")

    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def populate_shift_data(run_id, conn):
    """
    Author       : Richmond Rice
    Date         : Jan 2018
    Purpose      : 
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies : 
    """

    sas_shift_data_table = 'SAS_SHIFT_DATA'

    sas_shift_data_insert_query = "INSERT INTO " + sas_shift_data_table + " \
        (REC_ID, PORTROUTE, WEEKDAY, ARRIVEDEPART, TOTAL, AM_PM_NIGHT) \
        (SELECT REC_ID_S.NEXTVAL, SD.PORTROUTE, SD.WEEKDAY, SD.ARRIVEDEPART, \
        SD.TOTAL, SD.AM_PM_NIGHT \
        FROM SHIFT_DATA SD WHERE SD.RUN_ID = '" + run_id + "')"

    cf.delete_from_table(sas_shift_data_table)

    cur = conn.cursor()
    cur.execute(sas_shift_data_insert_query)
    conn.commit()


def copy_shift_wt_pvs_for_survey_data(run_id, conn):
    """
    Author       : Richmond Rice
    Date         : Jan 2018
    Purpose      : 
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies : 
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_shift_spv_table = 'SAS_SHIFT_SPV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE_PY PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('SHIFT_PORT_GRP_PV', 'WEEKDAY_END_PV', \
        'AM_PM_NIGHT_PV', 'SHIFT_FLAG_PV', 'CROSSINGS_FLAG_PV'))"

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_shift_spv_table)

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()


def update_survey_data_with_shift_wt_pv_output(conn):
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

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def copy_shift_wt_pvs_for_shift_data(run_id, conn):
    """
    Author       : Thomas Mahoney
    Date         : 10 Jan 2017
    Purpose      : Copies the shift weight process variables for shift_data
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_shift_pv_table = 'SAS_SHIFT_PV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('SHIFT_PORT_GRP_PV', 'WEEKDAY_END_PV', \
        'AM_PM_NIGHT_PV'))"

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_shift_pv_table)

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()


def update_shift_data_with_pvs_output(conn):
    """
    Author       : Richmond Rice
    Date         : Jan 2018
    Purpose      : 
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies : 
    """

    sas_shift_pv_table = 'SAS_SHIFT_PV'
    sas_shift_wt_table = 'SAS_SHIFT_WT'
    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_ps_shift_data_table = 'SAS_PS_SHIFT_DATA'

    sas_shift_data_update_query = "UPDATE SAS_SHIFT_DATA SSD SET (SSD.SHIFT_PORT_GRP_PV, \
        SSD.WEEKDAY_END_PV, SSD.AM_PM_NIGHT_PV) = (SELECT SSP.SHIFT_PORT_GRP_PV, \
        SSP.WEEKDAY_END_PV,SSP.AM_PM_NIGHT_PV FROM " + sas_shift_pv_table + " SSP \
        WHERE SSD.REC_ID = SSP.REC_ID)"

    print(sas_shift_data_update_query)

    cur = conn.cursor()
    cur.execute(sas_shift_data_update_query)
    conn.commit()

    cf.delete_from_table(sas_shift_pv_table)
    cf.delete_from_table(sas_shift_wt_table)
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_ps_shift_data_table)


def update_survey_data_with_shift_wt_results(conn):
    """
    Author       : Thomas Mahoney
    Date         : 10 Jan 2017
    Purpose      : Updates survey_data with the shift weight results.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update sas_survey_subsample sss
            set (sss.SHIFT_WT ) =
            (select 
                ssw.SHIFT_WT        
            from 
                sas_shift_wt ssw        
            where 
                sss.SERIAL = ssw.SERIAL)
            """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table("SAS_SHIFT_WT")


def store_survey_data_with_shift_wt_results(run_id, conn):
    """
    Author       : Thomas Mahoney
    Date         : 10 Jan 2017
    Purpose      : Stores the survey data with the shift weight results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

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

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('PS_SHIFT_DATA', 'RUN_ID', '=', run_id)

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")


def store_shift_wt_summary(run_id, conn):
    """
    Author       : Thomas Mahoney
    Date         : 10 Jan 2017
    Purpose      : Stores the shift weight summary
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    cf.delete_from_table('PS_SHIFT_DATA', 'RUN_ID', '=', run_id)

    sql = """
     insert into ps_shift_data 
     (RUN_ID, SHIFT_PORT_GRP_PV, ARRIVEDEPART, WEEKDAY_END_PV, AM_PM_NIGHT_PV, MIGSI, POSS_SHIFT_CROSS, SAMP_SHIFT_CROSS, MIN_SH_WT, MEAN_SH_WT, MAX_SH_WT, COUNT_RESPS, SUM_SH_WT)
     (select '""" + run_id + """', SHIFT_PORT_GRP_PV, ARRIVEDEPART, WEEKDAY_END_PV, AM_PM_NIGHT_PV, MIGSI, POSS_SHIFT_CROSS, SAMP_SHIFT_CROSS, MIN_SH_WT, MEAN_SH_WT, MAX_SH_WT, COUNT_RESPS, SUM_SH_WT        
     from sas_ps_shift_data)
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('SAS_PS_SHIFT_DATA')


def run_all(run_id, conn):
    # Hard Coded for now, this will be generated
    # run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    #  Populate Survey Data For Shift Wt                   TM
    populate_survey_data_for_shift_wt(run_id, conn)

    # Populate Shift Data                                  RR
    populate_shift_data(run_id, conn)

    # Copy Shift Wt PVs For Survey Data                    RR
    copy_shift_wt_pvs_for_survey_data(run_id, conn)

    # Apply Shift Wt PVs On Survey Data                    X
    process_variables.process()  ##############

    # Update Survey Data with Shift Wt PV Output           TM
    update_survey_data_with_shift_wt_pv_output(run_id, conn)

    # Copy Shift Wt PVs For Shift Data                     TM
    copy_shift_wt_pvs_for_shift_data(run_id, conn)

    # Apply Shift Wt PVs On Shift Data                     X
    process_variables  ##############

    # Update Shift Data with PVs Output                    RR
    update_shift_data_with_pvs_output()

    # Calculate Shift Weight                               X 

    # Update Survey Data With Shift Wt Results             TM
    update_survey_data_with_shift_wt_results(run_id, conn)

    # Store Survey Data With Shift Wt Results              TM
    store_survey_data_with_shift_wt_results(run_id, conn)

    # Store Shift Wt Summary                               TM 
    store_shift_wt_summary(run_id, conn)

    pass


if __name__ == '__main__':
    run_all()
