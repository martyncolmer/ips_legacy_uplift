from main.io import CommonFunctions as cf
import pandas as pd
from main.utils import process_variables


def populate_survey_data_for_min_wt(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Populates survey_data in preparation for minimum weights
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
                    set ss.MINS_PORT_GRP_PV = null, 
                    ss.MINS_CTRY_GRP_PV = null, 
                    ss.MINS_NAT_GRP_PV = null,
                    ss.MINS_CTRY_PORT_GRP_PV = null,
                    ss.MINS_FLAG_PV = null,        
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
    cf.delete_from_table("SAS_MINIMUMS_WT")
    cf.delete_from_table("SAS_PS_MINIMUMS")

    nullify_survey_subsample_pv_values(conn)
    move_survey_subsample_to_sas_table(conn)


def copy_min_wt_pvs_for_survey_data(run_id, conn):
    """
    Author       : David Powell
    Date         : Mar 2018
    Purpose      : Copy minimums weight process variable data
    Parameters   : NA
    Returns      : NA
    Requirements : IPSTransformation
    Dependencies :
    """

    sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
    sas_min_spv_table = 'SAS_MINIMUMS_SPV'

    sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
        (PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
        FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + run_id + "' \
        AND UPPER(PV.PV_NAME) IN ('MINS_PORT_GRP_PV', 'MINS_CTRY_GRP_PV', 'MINS_NAT_GRP_PV', \
        'MINS_CTRY_PORT_GRP_PV', 'MINS_FLAG_PV'))"

    cf.delete_from_table(sas_process_variable_table)
    cf.delete_from_table(sas_min_spv_table)

    cur = conn.cursor()
    cur.execute(sas_process_variable_insert_query)
    conn.commit()


def update_survey_data_with_min_wt_pv_output(conn):
        """
        Author       : David Powell
        Date         : 22 Mar 2018
        Purpose      : Updates survey_data with the min weight process variable
                     : output.
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
        """

        sql = """ update sas_survey_subsample sss       
                    set (sss.MINS_FLAG_PV
                        , sss.MINS_PORT_GRP_PV
                        , sss.MINS_CTRY_GRP_PV
                        , sss.MINS_NAT_GRP_PV
                        , sss.MINS_CTRY_PORT_GRP_PV) =
                        (select smr.MINS_FLAG_PV, 
                            smr.MINS_PORT_GRP_PV, 
                            smr.MINS_CTRY_GRP_PV, 
                            smr.MINS_NAT_GRP_PV,
                            smr.MINS_CTRY_PORT_GRP_PV
                        from 
                            sas_minimums_spv smr
                        where 
                            sss.SERIAL = smr.SERIAL)
                    """

        sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
        sas_minimums_table = 'SAS_MINIMUMS_SPV'

        cf.delete_from_table(sas_process_variable_table)
        cf.delete_from_table(sas_minimums_table)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def update_survey_data_with_min_wt_results(conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Updates survey_data with the non response weight results.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update sas_survey_subsample sss
                set (sss.MINS_WT ) =
                (select 
                    smr.MINS_WT        
                from 
                    sas_minimums_wt smr        
                where 
                    sss.SERIAL = smr.SERIAL)
                """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table("SAS_MINIMUMS_WT")


def store_survey_data_with_min_wt_results(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Stores the survey data with the minimums weight results
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    sql = """update survey_subsample ss
            set (ss.MINS_FLAG_PV,
                ss.MINS_PORT_GRP_PV, 
                ss.MINS_CTRY_GRP_PV,
                ss.MINS_NAT_GRP_PV,
                ss.MINS_CTRY_PORT_GRP_PV,
                ss.MINS_WT ) = 
            (select sss.MINS_FLAG_PV, 
                sss.MINS_PORT_GRP_PV, 
                sss.MINS_CTRY_GRP_PV,
                sss.MINS_NAT_GRP_PV, 
                sss.MINS_CTRY_PORT_GRP_PV,
                sss.MINS_WT
            from 
                sas_survey_subsample sss         
            where 
                sss.SERIAL = ss.SERIAL)        
            where 
                ss.RUN_ID = '""" + run_id + "'"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('PS_MINIMUMS', 'RUN_ID', '=', run_id)

    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")


def store_min_wt_summary(run_id, conn):
    """
    Author       : David Powell
    Date         : 22 Mar 2018
    Purpose      : Stores the minimums weight summary
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    cf.delete_from_table('PS_MINIMUMS', 'RUN_ID', '=', run_id)

    sql = """
     insert into ps_minimums 
     (RUN_ID, MINS_PORT_GRP_PV, ARRIVEDEPART, MINS_CTRY_GRP_PV, MINS_NAT_GRP_PV,  \
      MINS_CTRY_PORT_GRP_PV, MINS_CASES, FULLS_CASES, PRIOR_GROSS_MINS, PRIOR_GROSS_FULLS, PRIOR_GROSS_ALL, \
      MINS_WT, POST_SUM, CASES_CARRIED_FWD)
     (select '""" + run_id + """', MINS_PORT_GRP_PV, ARRIVEDEPART, MINS_CTRY_GRP_PV, MINS_NAT_GRP_PV, \
      MINS_CTRY_PORT_GRP_PV, MINS_CASES, FULLS_CASES, PRIOR_GROSS_MINS, PRIOR_GROSS_FULLS, PRIOR_GROSS_ALL, \
      MINS_WT, POST_SUM, CASES_CARRIED_FWD       
      from sas_ps_minimums)
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table('SAS_PS_MINIMUMS')
    conn.commit()


def run_all(run_id, conn):

    # Hard Coded for now, this will be generated
    # run_id = '9e5c1872-3f8e-4ae5-85dc-c67a602d011e'

    #  Populate Survey Data For Min Wt                   TM
    populate_survey_data_for_min_wt(run_id, conn)

    # Copy Min Wt PVs For Survey Data                    RR
    copy_min_wt_pvs_for_survey_data(run_id, conn)

    # Apply Minimums Wt PVs On Survey Data           X
    process_variables.process()

    # Update Survey Data with Min Wt PV Output           TM
    update_survey_data_with_min_wt_pv_output(conn)

    # Calculate Minimums Weight                      X

    # Update Survey Data With Min Wt Results             TM
    update_survey_data_with_min_wt_results(conn)

    # Store Survey Data With Min Wt Results              TM
    store_survey_data_with_min_wt_results(run_id, conn)

    # Store Min Wt Summary                               TM
    store_min_wt_summary(run_id, conn)

    pass


if __name__ == '__main__':
        conn = cf.get_oracle_connection()
        run_all()