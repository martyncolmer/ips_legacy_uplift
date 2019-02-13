import pandas as pd


def get_schema() -> pd.DataFrame.dtypes:
    return {
        'RUN_ID': 'str',
        'SERIAL': 'Int64',
        'AGE': 'Int64',
        'AM_PM_NIGHT': 'Int64',
        'ANYUNDER16': 'Int64',
        'APORTLATDEG': 'Int64',
        'APORTLATMIN': 'Int64',
        'APORTLATSEC': 'Int64',
        'APORTLATNS': 'category',
        'APORTLONDEG': 'Int64',
        'APORTLONMIN': 'Int64',
        'APORTLONSEC': 'Int64',
        'APORTLONEW': 'category',
        'ARRIVEDEPART': 'Int64',
        'BABYFARE': "float64",
        'BEFAF': 'Int64',
        'CHANGECODE': 'Int64',
        'CHILDFARE': "float64",
        'COUNTRYVISIT': 'Int64',
        'CPORTLATDEG': 'Int64',
        'CPORTLATMIN': 'Int64',
        'CPORTLATSEC': 'Int64',
        'CPORTLATNS': 'category',
        'CPORTLONDEG': 'Int64',
        'CPORTLONMIN': 'Int64',
        'CPORTLONSEC': 'Int64',
        'CPORTLONEW': 'category',
        'INTDATE': 'Int64',
        'DAYTYPE': 'Int64',
        'DIRECTLEG': 'Int64',
        'DVEXPEND': 'Int64',
        'DVFARE': 'Int64',
        'DVLINECODE': 'Int64',
        'DVPACKAGE': 'Int64',
        'DVPACKCOST': 'Int64',
        'DVPERSONS': 'Int64',
        'DVPORTCODE': 'Int64',
        'EXPENDCODE': 'object',
        'EXPENDITURE': 'Int64',
        'FARE': 'Int64',
        'FAREK': 'Int64',
        'FLOW': 'Int64',
        'HAULKEY': 'Int64',
        'INTENDLOS': 'Int64',
        'KIDAGE': 'Int64',
        'LOSKEY': 'Int64',
        'MAINCONTRA': 'Int64',
        'MIGSI': 'Int64',
        'INTMONTH': 'float64',
        'NATIONALITY': 'Int64',
        'NATIONNAME': 'object',
        'NIGHTS1': 'Int64',
        'NIGHTS2': 'Int64',
        'NIGHTS3': 'Int64',
        'NIGHTS4': 'Int64',
        'NIGHTS5': 'Int64',
        'NIGHTS6': 'Int64',
        'NIGHTS7': 'Int64',
        'NIGHTS8': 'Int64',
        'NUMADULTS': 'Int64',
        'NUMDAYS': 'Int64',
        'NUMNIGHTS': 'Int64',
        'NUMPEOPLE': 'Int64',
        'PACKAGEHOL': 'Int64',
        'PACKAGEHOLUK': 'Int64',
        'PERSONS': 'Int64',
        'PORTROUTE': 'Int64',
        'PACKAGE': 'Int64',
        'PROUTELATDEG': 'Int64',
        'PROUTELATMIN': 'Int64',
        'PROUTELATSEC': 'Int64',
        'PROUTELATNS': 'category',
        'PROUTELONDEG': 'Int64',
        'PROUTELONMIN': 'Int64',
        'PROUTELONSEC': 'Int64',
        'PROUTELONEW': 'category',
        'PURPOSE': 'Int64',
        'QUARTER': 'Int64',
        'RESIDENCE': 'Int64',
        'RESPNSE': 'Int64',
        'SEX': 'Int64',
        'SHIFTNO': 'Int64',
        'SHUTTLE': 'Int64',
        'SINGLERETURN': 'Int64',
        'TANDTSI': 'Int64',
        'TICKETCOST': 'Int64',
        'TOWNCODE1': 'Int64',
        'TOWNCODE2': 'Int64',
        'TOWNCODE3': 'Int64',
        'TOWNCODE4': 'Int64',
        'TOWNCODE5': 'Int64',
        'TOWNCODE6': 'Int64',
        'TOWNCODE7': 'Int64',
        'TOWNCODE8': 'Int64',
        'TRANSFER': 'Int64',
        'UKFOREIGN': 'Int64',
        'VEHICLE': 'Int64',
        'VISITBEGAN': 'Int64',
        'WELSHNIGHTS': 'Int64',
        'WELSHTOWN': 'Int64'
    }
