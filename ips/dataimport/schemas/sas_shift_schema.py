import pandas as pd


def get_schema():
    return {
        'REC_ID': 'Int64',
        'PORTROUTE': 'Int64',
        'WEEKDAY': 'Int64',
        'ARRIVEDEPART': 'Int64',
        'TOTAL': 'Int64',
        'AM_PM_NIGHT': 'Int64',
        'SHIFT_PORT_GRP_PV': 'Int64',
        'AM_PM_NIGHT_PV': 'Int64',
        'WEEKDAY_END_PV': 'Int64'
    }


def convert_dtype(df):
    df['REC_ID'] = pd.to_numeric(df['REC_ID'])
    df['PORTROUTE'] = pd.to_numeric(df['PORTROUTE'])
    df['WEEKDAY'] = pd.to_numeric(df['WEEKDAY'])
    df['ARRIVEDEPART'] = pd.to_numeric(df['ARRIVEDEPART'])
    df['TOTAL'] = pd.to_numeric(df['TOTAL'])
    df['AM_PM_NIGHT'] = pd.to_numeric(df['AM_PM_NIGHT'])
    df['SHIFT_PORT_GRP_PV'] = df.astype(str)
    df['AM_PM_NIGHT_PV'] = pd.to_numeric(df['AM_PM_NIGHT_PV'])
    df['WEEKDAY_END_PV'] = pd.to_numeric(df['WEEKDAY_END_PV'])
    return df
