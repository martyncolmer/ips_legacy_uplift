'''
Created on 5 Mar 2018

@author: mahont1
'''
import pandas as pd
from sas7bdat import SAS7BDAT

in_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\ips1712bv4_amtspnd.sas7bdat"
out_path = r'../../tests/data/import/input/input_data.pkl'

try:
    df = pd.read_sas(in_path)
except:
    df = SAS7BDAT(in_path).to_data_frame()

df.to_pickle(out_path)
