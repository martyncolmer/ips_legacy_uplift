'''
Created on 5 Mar 2018

@author: mahont1
'''
import pandas as pd


in_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\shift_weight\shiftsdata.sas7bdat'
out_path = r'C:\Git_projects\IPS_Legacy_Uplift\tests\data\shift_weight\shiftsdata.pkl'

df = pd.read_sas(in_path)

df.to_pickle(out_path)

import glob
import os

sas_path = r'\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec Data\shift_weight' + r'\*.sas7bdat'
pkl_path = r'C:\Git_projects\IPS_Legacy_Uplift\tests\data\shift_weight'

# collect all the sas7bdat files
files = glob.glob(sas_path)

for file in files:
    # print(file.title())

    # get file name and extension
    base = os.path.basename(file.title())

    # get file name only
    filename = os.path.splitext(base)[0]

    # create path to pickle file
    path_to_write_pickle_file = pkl_path + '\\' + filename + ".pkl"

    # read sas file
    df = pd.read_sas(file.title())

    # write pkl file
    df.to_pickle(path_to_write_pickle_file)



