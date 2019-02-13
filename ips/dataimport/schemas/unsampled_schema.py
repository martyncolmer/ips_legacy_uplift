import pandas as pd


def get_schema() -> pd.DataFrame.dtypes:
    return {
        'RUN_ID': 'str',
        'YEAR': 'Int64',
        'MONTH': 'Int64',
        'DATASOURCE': 'str',
        'PORTROUTE': 'Int64',
        'REGION': 'Int64',
        'ARRIVEDEPART': 'Int64',
        'UNSAMP_TOTAL': 'float'
    }
