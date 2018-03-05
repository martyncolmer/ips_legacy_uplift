'''
Created on 5 Mar 2018

@author: mahont1
'''
import pandas as pd


in_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Rail Imputation\output_final.sas7bdat'
out_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Rail Imputation\output_test.pkl'

df = pd.read_sas(in_path)

df.to_pickle(out_path)