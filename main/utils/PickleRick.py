'''
Created on 5 Mar 2018

@author: mahont1
'''
import pandas as pd


in_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\min_weight\surveydata.sas7bdat'
out_path = r'../../tests/data/minimums_input.pkl'

df = pd.read_sas(in_path)
df.to_pickle(out_path)

in_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\min_weight\summarydata_final.sas7bdat'
out_path = r'../../tests/data/minimums_summary.pkl'

df = pd.read_sas(in_path)
df.to_pickle(out_path)

in_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\min_weight\outputdata_final.sas7bdat'
out_path = r'../../tests/data/minimums_output.pkl'

df = pd.read_sas(in_path)
df.to_pickle(out_path)
