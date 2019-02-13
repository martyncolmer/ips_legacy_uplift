import pandas as pd


def get_schema() -> pd.DataFrame.dtypes:
    return {
        'RUN_ID': 'str',
        'YEAR': 'Int64',
        'MONTH': 'Int64',
        'DATASOURCE': 'str',
        'PORTROUTE': 'Int64',
        'ARRIVEDPART': 'Int64',
        'TRAFFICTOTAL': 'float',
        'PERIODSTART': 'str',
        'PERIODEND': 'str',
        'AM_PM_NIGHT': 'Int64',
        'HAUL': 'str'
    }
