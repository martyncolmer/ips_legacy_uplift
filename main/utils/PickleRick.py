'''
Created on 5 Mar 2018

@author: mahont1
'''
import pandas as pd
from sas7bdat import SAS7BDAT

in_path = r"ustotals.SAS7BDAT"
out_path = r"tests\data\r_setup\October_2017\unsampled_weight\ustotals.pkl"

if in_path[-3:] == 'csv':
    df = pd.read_csv(in_path)

else:
    try:
        df = pd.read_sas(in_path)
    except:
        df = SAS7BDAT(in_path).to_data_frame()

df.to_pickle(out_path)
