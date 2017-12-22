# In[22]:

# Function - do_ips_shift_weight_calculation
# Table to check - data_summary

# if (min_sh_wt lt 50 or max_sh_wt gt 5000) and not missing(samp_shift_cross) then
# WARNING - "Shift weight outside thresholds for: &cases


# In[30]:

df_min_sw_check = data_summary[data_summary['SAMP_SHIFT_CROSS'].notnull() & (data_summary['MIN_SH_WT'] < 50)]


# In[32]:

df_max_sw_check = data_summary[data_summary['SAMP_SHIFT_CROSS'].notnull() & (data_summary['MAX_SH_WT'] > 5000)]


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



