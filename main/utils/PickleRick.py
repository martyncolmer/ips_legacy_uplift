'''
Created on 5 Mar 2018

@author: mahont1
'''
import pandas as pd
from sas7bdat import SAS7BDAT

in_path = r"C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\ips_data_management\shift_weight\copy_pvs_stay_imp_resultV2.csv"
out_path = r"C:\Users\thorne1\PycharmProjects\IPS_Legacy_Uplift\tests\data\ips_data_management\shift_weight\copy_pvs_shift_data_result.pkl"

if in_path[-3:] == 'csv':
    df = pd.read_csv(in_path)

else:
    try:
        df = pd.read_sas(in_path)
    except:
        df = SAS7BDAT(in_path).to_data_frame()

df.to_pickle(out_path)
