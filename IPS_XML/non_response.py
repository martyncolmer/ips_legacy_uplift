from main.io import CommonFunctions as cf
import pandas as pd
from IPS_Stored_Procedures import process_variables


def populate_survey_data_for_non_response_wt(run_id, conn):
    """
    Author       : David Powell
    Date         : 21 Mar 2018
    Purpose      : Populates survey_data in preparation for non response
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
                    set ss.NR_PORT_GRP_PV = null, 
                    ss.MIG_FLAG_PV = null, 
                    ss.NR_FLAG_PV = null,        
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
    cf.delete_from_table("SAS_NON_RESPONSE_WT")
    cf.delete_from_table("SAS_PS_NON_RESPONSE")

    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def populate_non_response_data(run_id, conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Populate non response data
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_response_data_table = 'sas_non_response_data'

    sas_response_data_insert_query = "INSERT INTO " + sas_response_data_table + " \
        (REC_ID, PORTROUTE, WEEKDAY, ARRIVEDEPART, AM_PM_NIGHT, SAMPINTERVAL, MIGTOTAL, ORDTOTAL) \
        (SELECT REC_ID_S.NEXTVAL, NR.PORTROUTE, NR.WEEKDAY, NR.ARRIVEDEPART, \
        NR.AM_PM_NIGHT, NR.SAMPINTERVAL, NR.MIGTOTAL, NR.ORDTOTAL \
        FROM NON_RESPONSE_DATA NR WHERE SD.RUN_ID = '" + run_id + "')"

    cf.delete_from_table(sas_response_data_table)

    cur = conn.cursor()
    cur.execute(sas_response_data_insert_query)
    conn.commit()


def copy_non_response_wt_pvs_for_survey_data(run_id, conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Copy non response process variable data
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_non_response_spv_table = 'SAS_NON_RESPONSE_SPV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('NR_PORT_GRP_PV', 'MIG_FLAG_PV', \
        'NR_FLAG_PV'))"

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_non_response_spv_table)

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()


def update_survey_data_with_non_response_wt_pv_output(conn):
        """
        Author       : David Powell
        Date         : 21 Mar 2018
        Purpose      : Updates survey_data with the non response weight process variable
                     : output.
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
        """

        sql = """update sas_survey_subsample sss       
                    set (sss.MIG_FLAG_PV, 
                        sss.NR_PORT_GRP_PV, 
                        sss.NR_FLAG_PV ) =
                    (select snr.MIG_FLAG_PV, 
                        snr.NR_PORT_GRP_PV, 
                        snr.NR_FLAG_PV, 
                    from 
                        sas_non_response_spv snr
                    where 
                        sss.SERIAL = snr.SERIAL)
                    """

        sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
        sas_non_response_table = 'SAS_NON_RESPONSE_SPV'

        cf.delete_from_table(sas_process_variable_table)
        cf.delete_from_table(sas_non_response_table)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def copy_non_response_wt_pvs_for_non_response_data(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Copies the non response weight process variables for shift_data
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_non_response_pv_table = 'SAS_NON_RESPONSE_PV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('NR_PORT_GRP_PV', 'WEEKDAY_END_PV' ))"

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_non_response_pv_table)


def update_non_response_data_with_pvs_output(conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Update non response data with process variables
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_non_response_pv_table = 'SAS_NON_RESPONSE_PV'
    sas_non_response_wt_table = 'SAS_NON_RESPONSE_WT'
    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_ps_non_response_data_table = 'SAS_PS_NON_RESPONSE'

    sas_non_response_data_update_query = "UPDATE SAS_NON_RESPONSE_DATA SSD SET (SSD.MIG_FLAG_PV, \
        SSD.NR_PORT_GRP_PV, SSD.NR_FLAG_PV) = (SELECT SSP.MIG_FLAG_PV, \
        SSP.NR_PORT_GRP_PV,SSP.NR_FLAG_PV FROM " + sas_non_response_pv_table + " SSP \
        WHERE SSD.REC_ID = SSP.REC_ID)"

    cur = conn.cursor()
    cur.execute(sas_non_response_data_update_query)
    conn.commit()

    cf.delete_from_table(sas_non_response_pv_table)
    cf.delete_from_table(sas_non_response_wt_table)
    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_ps_non_response_data_table)


def update_survey_data_with_non_response_wt_results(conn):
        """
        Author       : David Powell
        Date         : 21 Mar 2018
        Purpose      : Updates survey_data with the non response weight results.
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
        """

        sql = """update sas_survey_subsample sss
                set (sss.NON_RESPONSE_WT ) =
                (select 
                    snr.NON_RESPONSE_WT        
                from 
                    sas_non_response_wt snr        
                where 
                    sss.SERIAL = snr.SERIAL)
                """

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        cf.delete_from_table("SAS_NON_RESPONSE_WT")


def store_survey_data_with_non_response_wt_results(run_id, conn):
    """
    Author       : David Powell
    Date         : 21 Mar 2018
    Purpose      : Stores the survey data with the non response weight results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update survey_subsample ss
            set (ss.MIG_FLAG_PV,
                ss.NR_PORT_GRP_PV, 
                ss.NR_FLAG_PV,
                ss.NON_RESPONSE_WT ) = 
            (select sss.MIG_FLAG_PV, 
                sss.NR_PORT_GRP_PV, 
                sss.NR_FLAG_PV,
                sss.NON_RESPONSE_WT, 
            from 
                sas_survey_subsample sss         
            where 
                sss.SERIAL = ss.SERIAL)        
            where 
                ss.RUN_ID = '""" + run_id + "'"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('PS_NON_RESPONSE', 'RUN_ID', '=', run_id)

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")


def store_non_response_wt_summary(run_id, conn):
    """
    Author       : David Powell
    Date         : 21 Mar 2018
    Purpose      : Stores the non response weight summary
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    cf.delete_from_table('PS_NON_RESPONSE', 'RUN_ID', '=', run_id)

    sql = """
     insert into ps_non_response 
     (RUN_ID, NR_PORT_GRP_PV, ARRIVEDEPART, WEEKDAY_END_PV, MEAN_RESPS_SH_WT, COUNT_RESPS, PRIOR_SUM, GROSS_RESP, GNR, MEAN_NR_WT)
     (select '""" + run_id + """', NR_PORT_GRP_PV, ARRIVEDEPART, WEEKDAY_END_PV, MEAN_RESPS_SH_WT, COUNT_RESPS, PRIOR_SUM, GROSS_RESP, GNR, MEAN_NR_WT        
     from sas_ps_non_response)
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('SAS_PS_NON_RESPONSE')


def run_all(run_id, conn):

    # Hard Coded for now, this will be generated
    # run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    #  Populate Survey Data For Non Response Wt                   TM
    populate_survey_data_for_non_response_wt(run_id, conn)

    # Populate Non Response Data                                  RR
    populate_non_response_data(run_id, conn)

    # Copy Non Response Wt PVs For Survey Data                    RR
    copy_non_response_wt_pvs_for_survey_data(run_id, conn)

    # Apply Non Response Wt PVs On Survey Data                    X
    process_variables.process()  ##############

    # Update Survey Data with Non Response Wt PV Output           TM
    update_survey_data_with_non_response_wt_pv_output(run_id, conn)

    # Copy Non Response Wt PVs For Non Response Data                     TM
    copy_non_response_wt_pvs_for_non_response_data(run_id, conn)

    # Apply Non Response Wt PVs On Shift Data                     X
    process_variables  ##############

    # Update Non Response Data with PVs Output                    RR
    update_non_response_data_with_pvs_output()

    # Calculate Non Response Weight                               X

    # Update Survey Data With Non Response Wt Results             TM
    update_survey_data_with_non_response_wt_results(run_id, conn)

    # Store Survey Data With Non Response Wt Results              TM
    store_survey_data_with_non_response_wt_results(run_id, conn)

    # Store Non Response Wt Summary                               TM
    store_non_response_wt_summary(run_id, conn)

    pass


if __name__ == '__main__':
        run_all()