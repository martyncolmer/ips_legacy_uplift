import pandas as pd


def get_schema() -> pd.DataFrame.dtypes:
    return {
        'RUN_ID': 'str',
        'YEAR': 'Int64',
        'MONTH': 'Int64',
        'DATASOURCE': 'str',
        'PORTROUTE': 'Int64',
        'WEEKDAY': 'Int64',
        'ARRIVEDEPART': 'Int64',
        'AM_PM_NIGHT': 'Int64',
        'SAMPINTERVAL': 'Int64',
        'MIGTOTAL': 'Int64',
        'ORDTOTAL': 'Int64'
    }
