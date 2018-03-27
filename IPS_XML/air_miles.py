'''
Created on 27 March 2018

@author: Nassir Mohammad
'''
from IPSTransformation import CommonFunctions as cf
import pandas as pd


def populate_survey_data_for_air_miles(run_id, conn):

    """
    Author       : Nassir Mohammad
    Date         : 27/03/2018
    Purpose      : Populates survey_data in preparation for performing the
                 : fair miles calculation.
    Parameters   : conn - connection object pointing at the database.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    def nullify_survey_subsample_pv_values(conn):

        # SQL that sets survey_subsample's PV values to null
        sql = """
            update survey_subsample ss
            set
                ss.DIRECTLEG = null, 
                ss.OVLEG = null, 
                ss.UKLEG = null
            where
                ss.RUN_ID = '""" + run_id + "'"

        # Executes and commits the SQL command
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

    def move_survey_subsample_to_sas_table(conn):
        column_string = """SERIAL, AGE, AM_PM_NIGHT, ANYUNDER16, APORTLATDEG, APORTLATMIN, APORTLATSEC, APORTLATNS,
             APORTLONDEG, APORTLONMIN, APORTLONSEC, APORTLONEW, ARRIVEDEPART, BABYFARE, BEFAF, CHANGECODE,
             CHILDFARE, COUNTRYVISIT, CPORTLATDEG, CPORTLATMIN, CPORTLATSEC, CPORTLATNS, CPORTLONDEG,
             CPORTLONMIN, CPORTLONSEC, CPORTLONEW, INTDATE, DAYTYPE, DIRECTLEG, DVEXPEND, DVFARE,
             DVLINECODE, DVPACKAGE, DVPACKCOST, DVPERSONS, DVPORTCODE, EXPENDCODE, EXPENDITURE, FARE, FAREK,
             FLOW, HAULKEY, INTENDLOS, KIDAGE, LOSKEY, MAINCONTRA, MIGSI, INTMONTH, NATIONALITY, NATIONNAME,
             NIGHTS1, NIGHTS2, NIGHTS3, NIGHTS4, NIGHTS5, NIGHTS6, NIGHTS7, NIGHTS8, NUMADULTS, NUMDAYS,
             NUMNIGHTS, NUMPEOPLE, PACKAGEHOL, PACKAGEHOLUK, PERSONS, PORTROUTE, PACKAGE, PROUTELATDEG,
             PROUTELATMIN, PROUTELATSEC, PROUTELATNS, PROUTELONDEG, PROUTELONMIN, PROUTELONSEC, PROUTELONEW,
             PURPOSE, QUARTER, RESIDENCE, RESPNSE, SEX, SHIFTNO, SHUTTLE, SINGLERETURN, TANDTSI, TICKETCOST,
             TOWNCODE1, TOWNCODE2, TOWNCODE3, TOWNCODE4, TOWNCODE5, TOWNCODE6, TOWNCODE7, TOWNCODE8,
             TRANSFER, UKFOREIGN, VEHICLE, VISITBEGAN, WELSHNIGHTS, WELSHTOWN, AM_PM_NIGHT_PV, APD_PV,
             ARRIVEDEPART_PV, CROSSINGS_FLAG_PV, STAYIMPCTRYLEVEL1_PV, STAYIMPCTRYLEVEL2_PV,
             STAYIMPCTRYLEVEL3_PV, STAYIMPCTRYLEVEL4_PV, DAY_PV, DISCNT_F1_PV, DISCNT_F2_PV,
             DISCNT_PACKAGE_COST_PV, DUR1_PV, DUR2_PV, DUTY_FREE_PV, FAGE_PV, FARES_IMP_ELIGIBLE_PV,
             FARES_IMP_FLAG_PV, FLOW_PV, FOOT_OR_VEHICLE_PV, HAUL_PV, IMBAL_CTRY_FACT_PV, IMBAL_CTRY_GRP_PV,
             IMBAL_ELIGIBLE_PV, IMBAL_PORT_FACT_PV, IMBAL_PORT_GRP_PV, IMBAL_PORT_SUBGRP_PV, LOS_PV,
             LOSDAYS_PV, MIG_FLAG_PV, MINS_CTRY_GRP_PV, MINS_CTRY_PORT_GRP_PV, MINS_FLAG_PV,
             MINS_NAT_GRP_PV, MINS_PORT_GRP_PV, MINS_QUALITY_PV, NR_FLAG_PV, NR_PORT_GRP_PV, OPERA_PV,
             OSPORT1_PV, OSPORT2_PV, OSPORT3_PV, OSPORT4_PV, PUR1_PV, PUR2_PV, PUR3_PV, PURPOSE_PV,
             QMFARE_PV, RAIL_EXERCISE_PV, RAIL_IMP_ELIGIBLE_PV, SAMP_PORT_GRP_PV, SHIFT_FLAG_PV,
             SHIFT_PORT_GRP_PV, STAY_IMP_ELIGIBLE_PV, STAY_IMP_FLAG_PV, STAY_PURPOSE_GRP_PV, TOWNCODE_PV,
             TYPE_PV, UK_OS_PV, UKPORT1_PV, UKPORT2_PV, UKPORT3_PV, UKPORT4_PV, UNSAMP_PORT_GRP_PV,
             UNSAMP_REGION_GRP_PV, WEEKDAY_END_PV, DIRECT, EXPENDITURE_WT, EXPENDITURE_WTK, OVLEG, SPEND,
             SPEND1, SPEND2, SPEND3, SPEND4, SPEND5, SPEND6, SPEND7, SPEND8, SPEND9, SPENDIMPREASON, SPENDK,
             STAY, STAYK, STAY1K, STAY2K, STAY3K, STAY4K, STAY5K, STAY6K, STAY7K, STAY8K, STAY9K, STAYTLY,
             STAY_WT, STAY_WTK, UKLEG, VISIT_WT, VISIT_WTK, SHIFT_WT, NON_RESPONSE_WT, MINS_WT, TRAFFIC_WT,
             UNSAMP_TRAFFIC_WT, IMBAL_WT, FINAL_WT, FAREKEY, TYPEINTERVIEW"""

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

    # SQL that deletes from SAS_AIR_MILES
    cf.delete_from_table("SAS_AIR_MILES")

    # SQL that deletes sas_survey_subsample
    cf.delete_from_table("SAS_SURVEY_SUBSAMPLE")

    nullify_survey_subsample_pv_values(conn)

    move_survey_subsample_to_sas_table(conn)


def update_survey_data_with_air_miles_results(conn):
    """
        Author       : Nassir Mohammad
        Date         : 27/03/2018
        Purpose      : Updates survey_data with the air miles results.
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
    """

    sql = """update sas_survey_subsample sss
             set(sss.DIRECTLEG, sss.OVLEG, sss.UKLEG) = 
             (select 
                sam.DIRECTLEG, 
                sam.OVLEG, 
                sam.UKLEG        
             from 
                sas_air_miles sam         
             where 
                sss.SERIAL = sam.SERIAL);
        """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cf.delete_from_table("SAS_AIR_MILES")


def store_survey_data_with_air_miles_results(run_id, conn):
    """
        Author       : Nassir Mohammad
        Date         : 27/03/2018
        Purpose      : Store Survey Data with Air Miles Results
        Parameters   : conn - connection object pointing at the database.
        Returns      : NA
        Requirements : NA
        Dependencies : NA
    """

    sql = """update survey_subsample ss
             set(ss.DIRECTLEG, ss.OVLEG, ss.UKLEG) = 
             (select
                sss.DIRECTLEG, sss.OVLEG, sss.UKLEG         
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





