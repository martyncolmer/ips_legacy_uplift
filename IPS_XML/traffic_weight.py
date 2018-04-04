from main.io import CommonFunctions as cf
import pandas as pd
from main.utils import process_variables


def populate_survey_data_for_traffic_wt(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Populates survey_data in preparation for traffic
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
                    set ss.SAMP_PORT_GRP_PV = null, 
                    ss.FOOT_OR_VEHICLE_PV = null, 
                    ss.HAUL_PV = null,        
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
    cf.delete_from_table("SAS_TRAFFIC_WT")
    cf.delete_from_table("SAS_PS_TRAFFIC")

    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def populate_traffic_data(run_id, conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Populate traffic data
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_traffic_data_table = 'sas_traffic_data'

    sas_traffic_data_insert_query = "INSERT INTO " + sas_traffic_data_table + " \
        (REC_ID, PORTROUTE, ARRIVEDEPART, TRAFFICTOTAL, PERIODSTART, PERIODEND,  AM_PM_NIGHT,    HAUL, VEHICLE) \
        (SELECT REC_ID_S.NEXTVAL, tr.PORTROUTE, tr.ARRIVEDEPART, tr.TRAFFICTOTAL, \
        tr.PERIODSTART, tr.PERIODEND, tr.AM_PM_NIGHT, tr.HAUL, tr.VEHICLE\
        FROM TRAFFIC_DATA tr WHERE SD.RUN_ID = '" + run_id + "')"

    cf.delete_from_table(sas_traffic_data_table)

    cur = conn.cursor()
    cur.execute(sas_traffic_data_insert_query)
    conn.commit()


def copy_traffic_wt_pvs_for_survey_data(run_id, conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Copy traffic process variable data
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_traffic_spv_table = 'SAS_TRAFFIC_SPV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('SAMP_PORT_GRP_PV', 'FOOT_OR_VEHICLE_PV', \
        'HAUL_PV'))"

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_traffic_spv_table)

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()


def update_survey_data_with_traffic_wt_pv_output(conn):
        """
        Author       : David Powell
        Date         : 22 Mar 2018
        Purpose      : Updates survey_data with the traffic weight process variable
                     : output.
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
        """

        sql = """update sas_survey_subsample sss       
                    set (sss.sss.SAMP_PORT_GRP_PV, 
                        sss.sss.FOOT_OR_VEHICLE_PV, 
                        sss.sss.HAUL_PV ) =
                    (select sts.SAMP_PORT_GRP_PV, 
                        sts.FOOT_OR_VEHICLE_PV, 
                        sts.sss.HAUL_PV, 
                    from 
                        sas_traffic_spv sts
                    where 
                        sss.SERIAL = sts.SERIAL)
                    """

        sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
        sas_traffic_table = 'SAS_TRAFFIC_SPV'

        cf.delete_from_table(sas_process_variable_table)
        cf.delete_from_table(sas_traffic_table)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def copy_traffic_wt_pvs_for_traffic_data(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Copies the traffic weight process variables for traffic
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_traffic_pv_table = 'SAS_TRAFFIC_PV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('SAMP_PORT_GRP_PV', 'FOOT_OR_VEHICLE_PV', 'HAUL_PV' ))"

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_traffic_pv_table)


def update_traffic_data_with_pvs_output(conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Update traffic data with process variables
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_traffic_pv_table = 'SAS_TRAFFIC_PV'
    sas_traffic_wt_table = 'SAS_TRAFFIC_WT'
    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_ps_traffic_table = 'SAS_PS_TRAFFIC'

    sas_traffic_data_update_query = "UPDATE SAS_TRAFFIC_DATA SSD SET (SSD.SAMP_PORT_GRP_PV, \
        SSD.FOOT_OR_VEHICLE_PV, SSD.HAUL_PV) = (SELECT SSP.SAMP_PORT_GRP_PV, \
        SSP.FOOT_OR_VEHICLE_PV,SSP.HAUL_PV FROM " + sas_traffic_pv_table + " SSP \
        WHERE SSD.REC_ID = SSP.REC_ID)"

    cur = conn.cursor()
    cur.execute(sas_traffic_data_update_query)
    conn.commit()

    cf.delete_from_table(sas_traffic_pv_table)
    cf.delete_from_table(sas_traffic_wt_table)
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_ps_traffic_table)


def update_survey_data_with_traffic_wt_results(conn):
        """
        Author       : David Powell
        Date         : 22 Mar 2018
        Purpose      : Updates survey_data with the traffic weight results.
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
        """

        sql = """update sas_survey_subsample sss
                set (sss.TRAFFIC_WT ) =
                (select 
                    snr.TRAFFIC_WT        
                from 
                    sas_traffic_wt snr        
                where 
                    sss.SERIAL = snr.SERIAL)
                """

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        cf.delete_from_table("SAS_TRAFFIC_WT")


def store_survey_data_with_traffic_wt_results(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Stores the survey data with the traffic weight results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update survey_subsample ss
            set (ss.SAMP_PORT_GRP_PV,
                ss.FOOT_OR_VEHICLE_PV, 
                ss.HAUL_PV,
                ss.TRAFFIC_WT ) = 
            (select sss.SAMP_PORT_GRP_PV, 
                sss.FOOT_OR_VEHICLE_PV, 
                sss.HAUL_PV,
                sss.TRAFFIC_WT, 
            from 
                sas_survey_subsample sss         
            where 
                sss.SERIAL = ss.SERIAL)        
            where 
                ss.RUN_ID = '""" + run_id + "'"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('PS_TRAFFIC', 'RUN_ID', '=', run_id)

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")


def store_traffic_wt_summary(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Stores the traffic weight summary
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    cf.delete_from_table('PS_NON_RESPONSE', 'RUN_ID', '=', run_id)

    sql = """
     insert into ps_traffic 
     (RUN_ID, SAMP_PORT_GRP_PV, ARRIVEDEPART, FOOT_OR_VEHICLE_PV, CASES,          TRAFFICTOTAL, SUM_TRAFFIC_WT, TRAFFIC_WT)
     (select '""" + run_id + """', SAMP_PORT_GRP_PV, ARRIVEDEPART, FOOT_OR_VEHICLE_PV, CASES, TRAFFICTOTAL, SUM_TRAFFIC_WT, TRAFFIC_WT        
     from sas_ps_traffic)
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('SAS_PS_TRAFFIC')


def run_all(run_id, conn):

    # Hard Coded for now, this will be generated
    # run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    #  Populate Survey Data For Traffic Wt                   TM
    populate_survey_data_for_traffic_wt(run_id, conn)

    # Populate Traffic Data                                  RR
    populate_traffic_data(run_id, conn)

    # Copy Traffic Wt PVs For Survey Data                    RR
    copy_traffic_wt_pvs_for_survey_data(run_id, conn)

    # Apply Traffic Wt PVs On Survey Data                    X
    process_variables.process()  ##############

    # Update Survey Data with Traffic Wt PV Output           TM
    update_survey_data_with_traffic_wt_pv_output(run_id, conn)

    # Copy Traffic Wt PVs For Traffic Data                     TM
    copy_traffic_wt_pvs_for_traffic_data(run_id, conn)

    # Apply Traffic Wt PVs On Shift Data                     X
    process_variables  ##############

    # Update Traffic Data with PVs Output                    RR
    update_traffic_data_with_pvs_output()

    # Calculate Traffic Weight                               X

    # Update Survey Data With Traffic Wt Results             TM
    update_survey_data_with_traffic_wt_results(run_id, conn)

    # Store Survey Data With Traffic Wt Results              TM
    store_survey_data_with_traffic_wt_results(run_id, conn)

    # Store Traffic Wt Summary                               TM
    store_traffic_wt_summary(run_id, conn)

    pass


if __name__ == '__main__':
        run_all()