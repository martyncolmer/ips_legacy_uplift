
# coding: utf-8

# In[2]:

import pandas as pd
from sas7bdat import SAS7BDAT
import numpy as np


# In[3]:

sas_file_surveydata = (r"d:\\surveydata.sas7bdat")
sas_file_summary = (r"d:\\summary_2.sas7bdat")


# In[4]:

# sas import via sas7bdat function
def import_sas_sas7bdat(sas_file):
    return SAS7BDAT(sas_file).to_data_frame()


# In[5]:

data_surveydata = import_sas_sas7bdat(sas_file_surveydata)
data_surveydata.columns = data_surveydata.columns.str.upper()
data_summary = import_sas_sas7bdat(sas_file_summary)
data_summary.columns = data_summary.columns.str.upper()


# In[6]:

data_surveydata.head()


# In[7]:

# Function - do_ips_shift_weight_calculation
# Table to check - data_surveydata

# if (shift_factor null and SHIFT_FLAG_PV = 1) is true:
# ERROR - Case(s) contain no shift factor(s)
# else data_surveydata.loc[data_surveydata.SHIFT_FACTOR.isna() & (data_surveydata.SHIFT_FLAG_PV != 1), 'SHIFT_FACTOR'] = 1


# In[8]:

df_shift_flag = data_surveydata[data_surveydata['SHIFT_FLAG_PV'] == 1]


# In[9]:

if(len(df_shift_flag[df_shift_flag.SHIFT_FACTOR.isnull()])>0):
    print("Case(s) contain no shift factor(s)")
else:
   data_surveydata.loc[data_surveydata.SHIFT_FACTOR.isnull() & (data_surveydata.SHIFT_FLAG_PV != 1), 'SHIFT_FACTOR'] = 1


# In[10]:

# if (shift_factor null and SHIFT_FLAG_PV = 1) is true:
# ERROR - Case(s) contain no crossings factor(s)
# else data_surveydata.loc[data_surveydata.SHIFT_FACTOR.isna() & (data_surveydata.CROSSINGS_FLAG_PV != 1),
# 'CROSSINGS_FACTOR'] = 1


# In[11]:

df_crossings_flag = data_surveydata[data_surveydata['CROSSINGS_FLAG_PV'] == 1]

if(len(df_crossings_flag[df_crossings_flag.CROSSINGS_FACTOR.isnull()])>0):
    print("Case(s) contain no crossings factor(s)")
else:
   data_surveydata.loc[data_surveydata.CROSSINGS_FACTOR.isnull() &
                        (data_surveydata.CROSSINGS_FLAG_PV != 1), 'CROSSINGS_FACTOR'] = 1


# In[12]:

# if (shift_flag_pv eq 1) and (shift_factor le 0) is true:
# ERROR - Case(s) has an invalid number of possible shifts
# else continue


# In[13]:

df_invalid_shifts = data_surveydata[data_surveydata['SHIFT_FACTOR'] < 0]


# In[14]:

if len(df_shift_flag)>0 & len(df_invalid_shifts)>0:
    print("ERROR - Case(s) has an invalid number of possible shifts")


# In[15]:

# if (crossings_flag_pv eq 1) and (crossings_factor le 0) is true:
# ERROR - Case(s) has an invalid number of total crossings
# else continue


# In[16]:

df_invalid_crossings = data_surveydata[data_surveydata['CROSSINGS_FACTOR'] < 0]
if len(df_crossings_flag)>0 & len(df_invalid_crossings)>0:
    print("ERROR - Case(s) has an invalid number of total crossings")


# In[17]:

# if missing(migSI) is ture:
# ERROR - Case(s) missing sampling interval
# else continue


# In[20]:

df_missing_migsi = data_surveydata['MIGSI'].isnull().sum()


# In[21]:

if df_missing_migsi > 0:
    print("ERROR - Case(s) missing sampling interval")


# In[22]:

# Function - do_ips_shift_weight_calculation
# Table to check - data_summary

# if (min_sh_wt lt 50 or max_sh_wt gt 5000) and not missing(samp_shift_cross) then
# WARNING - "Shift weight outside thresholds for: &cases


# In[30]:

df_min_sh_check = data_summary[data_summary['SAMP_SHIFT_CROSS'].notnull() & (data_summary['MIN_SH_WT'] < 50)]


# In[32]:

df_max_sh_check = data_summary[data_summary['SAMP_SHIFT_CROSS'].notnull() & (data_summary['MAX_SH_WT'] > 5000)]


# In[45]:

cols = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI']


# In[46]:

sh_wt_thresholds_check = pd.merge(df_min_sh_check, df_max_sh_check, on = cols, how = 'outer')


# In[47]:

sh_wt_thresholds_check


# In[49]:

if len(sh_wt_thresholds_check) > 0:
    print("WARNING - Shift weight outside thresholds for: &cases")


# In[ ]:



