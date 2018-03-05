'''
Created on 7 Feb 2018

@author: mahont1
'''

from IPSTransformation import CommonFunctions as cf
import pandas as pd
from sas7bdat import SAS7BDAT

root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_IPS_Shift_Weight"
path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"
path_to_shifts_data = root_data_path + r"\shiftsdata.sas7bdat"


df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
df_shiftsdata = SAS7BDAT(path_to_shifts_data).to_data_frame()

#df_surveydata = pd.read_sas(path_to_survey_data)
#df_shiftsdata = pd.read_sas(path_to_shifts_data)

cf.insert_into_table_many('SAS_SHIFT_DATA', df_shiftsdata)
cf.insert_into_table_many('SAS_SURVEY_SUBSAMPLE', df_surveydata)

print df_shiftsdata.head()