'''
Created on 29 Jan 2018

@author: mahont1
'''
from IPSTransformation import CommonFunctions as cf
import pandas as pd

def NewCol(row):
    row['NewColumn'] = 1+2

df = cf.get_table_values('sas_shift_data')

print df

df['NewColumn'] = df['WEEKDAY_END_PV'] +df['AM_PM_NIGHT_PV']

print df

