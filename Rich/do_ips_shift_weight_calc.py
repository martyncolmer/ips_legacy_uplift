# Author: Richmond Rice
# Date: 2017-12
# Project: IPS Legacy Uplift
# Purpose: Calculate shift weight

import pandas as pd
from sas7bdat import SAS7BDAT

# sas files we are working with
sas_file_surveydata = r"d:\\#\\surveydata.sas7bdat"
sas_file_possshifts = r"d:\\#\\possibleshifts.sas7bdat"
sas_file_totsampcrossings = r"d:\\#\\totalsampledcrossings.sas7bdat"
sas_file_totsampshifts = r"d:\\#\\totalsampledshifts.sas7bdat"

# columns we are working with
cols1 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI']
cols2 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV']
cols3 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART']
cols4 = ['WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS']

# sas import function via pandas
def import_sas_pandas(sas_file):
    return pd.read_sas(sas_file)

# sas import function via sas7bdat
def import_sas_sas7bdat(sas_file):
    return SAS7BDAT(sas_file).to_data_frame()

# import sas data via sas7bdat as pandas read_sas has issues with these data sets
data_surveydata = import_sas_sas7bdat(sas_file_surveydata)
data_possshifts = import_sas_sas7bdat(sas_file_possshifts)
data_totsampcrossings = import_sas_sas7bdat(sas_file_totsampcrossings)
data_totsampshifts = import_sas_sas7bdat(sas_file_totsampshifts)

# make all column headers upper case
data_surveydata.columns = data_surveydata.columns.str.upper()
data_possshifts.columns = data_possshifts.columns.str.upper()
data_totsampcrossings.columns = data_totsampcrossings.columns.str.upper()
data_totsampshifts.columns = data_totsampshifts.columns.str.upper()

# check for missing shift factor
data_surveydata.loc[data_surveydata.SHIFT_FACTOR.isna() & (data_surveydata.SHIFT_FLAG_PV != 1), 'SHIFT_FACTOR'] = 1

# check for missing crossings factor
data_surveydata.loc[data_surveydata.CROSSINGS_FACTOR.isna() & (data_surveydata.CROSSINGS_FLAG_PV != 1), 'CROSSINGS_FACTOR'] = 1

# calculate shift weight
data_surveydata['SHIFT_WT'] = data_surveydata.SHIFT_FACTOR * data_surveydata.CROSSINGS_FACTOR * data_surveydata.MIGSI

# sort data
data_surveydata = data_surveydata.sort_values(cols1)

# groupby and aggregate summary
data_summary = data_surveydata.groupby(cols1)['SHIFT_WT'].agg({'COUNT_RESPS' : 'count'
															,'SUM_SH_WT' : 'sum'
															,'MIN_SH_WT' : 'min'
															,'MEAN_SH_WT' : 'mean'
															,'MAX_SH_WT' : 'max'})

# flatten
data_summary = data_summary.reset_index()

# merge possshifts to summary
data_summary = pd.merge(data_summary, data_possshifts, on = cols2)
data_summary = data_summary.drop({'_TYPE_', '_FREQ_'}, 1)
data_summary = data_summary.rename(columns = {'NUMERATOR' : 'POSS_SHIFT_CROSS'})

# merge totsampcrossings to summary
data_summary = pd.merge(data_summary, data_totsampcrossings, on = cols2)
data_summary = data_summary.drop({'_TYPE_', '_FREQ_'}, 1)
data_summary = data_summary.rename(columns = {'DENOMINATOR' : 'SAMP_SHIFT_CROSS'})

# merge totsampshifts to summary - CREATES TWO COLUMNS WITH THE SAME NAME !? SAS ONLY HAS THE ONE COLUMN !?
data_summary = pd.merge(data_summary, data_totsampshifts, on = cols2)
data_summary = data_summary.drop({'_TYPE_', '_FREQ_'}, 1)
data_summary = data_summary.rename(columns = {'DENOMINATOR' : 'SAMP_SHIFT_CROSS'})

# sort data
data_surveydata = data_surveydata.sort_values(cols3)

# groupby and aggregate summary high
data_summary_high = data_surveydata.groupby(cols3)['SHIFT_WT'].agg({'COUNT_RESPS' : 'count'
															,'SUM_SH_WT' : 'sum'
															,'MIN_SH_WT' : 'min'
															,'MEAN_SH_WT' : 'mean'
															,'MAX_SH_WT' : 'max'})

# flatten
data_summary_high = data_summary_high.reset_index()

# append tot samp crossings to tot samp shifts
data_totsampshifts.append(data_totsampcrossings)

# sort data
data_totsampshifts = data_totsampshifts.sort_values(cols3)

# groupby and aggregate
data_summary_high_sampled = data_totsampshifts.groupby(cols3)['DENOMINATOR'].agg({'SAMP_SHIFT_CROSS' : 'sum'})

# flatten
data_summary_high_sampled = data_summary_high_sampled.reset_index()

# left merge summary high with summary high sampled
data_summary_high = pd.merge(data_summary_high, data_summary_high_sampled, on = cols3, how='left')

# RETAIN SET BY !?

# drop columns - SET !? WHERE DOES SERIAL COME FROM !?
data_summary = data_summary['SHIFT_WT']