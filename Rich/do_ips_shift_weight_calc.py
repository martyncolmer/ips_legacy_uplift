# Author: Richmond Rice
# Date: 2017-12
# Project: IPS Legacy Uplift
# Purpose: Calculate shift weight

import pandas as pd
from sas7bdat import SAS7BDAT

# sas files we are working with
sas_file_surveydata = (r"d:\\#\\surveydata.sas7bdat")
sas_file_possshifts = (r"d:\\#\\possibleshifts.sas7bdat")
sas_file_totsampcrossings = (r"d:\\#\\totalsampledcrossings.sas7bdat")
sas_file_totsampshifts = (r"d:\\#\\totalsampledshifts.sas7bdat")

# column sets we are working with
cols1 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV',
    'AM_PM_NIGHT_PV', 'MIGSI']
cols2 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV']
cols3 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART']
cols4 = ['WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS']
cols5 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV',
    'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS', 'SAMP_SHIFT_CROSS',
    'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT']
cols6 = ['SERIAL', 'SHIFT_WT']

# sas import via pandas function
def import_sas_pandas(sas_file):
    return pd.read_sas(sas_file)

# sas import via sas7bdat function
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
data_surveydata.loc[data_surveydata.SHIFT_FACTOR.isna()
    & (data_surveydata.SHIFT_FLAG_PV != 1),
    'SHIFT_FACTOR'] = 1

# check for missing crossings factor
data_surveydata.loc[data_surveydata.CROSSINGS_FACTOR.isna()
    & (data_surveydata.CROSSINGS_FLAG_PV != 1),
    'CROSSINGS_FACTOR'] = 1

# calculate shift weight
data_surveydata['SHIFT_WT'] = data_surveydata.SHIFT_FACTOR \
    * data_surveydata.CROSSINGS_FACTOR \
    * data_surveydata.MIGSI

# sort surveydata
data_surveydata = data_surveydata.sort_values(cols1)

# groupby and aggregate summary
data_summary = data_surveydata.groupby(cols1)['SHIFT_WT'].agg({
    'COUNT_RESPS': 'count',
    'SUM_SH_WT': 'sum',
    'MIN_SH_WT': 'min',
    'MEAN_SH_WT': 'mean',
    'MAX_SH_WT': 'max'
})

# flatten summary
data_summary = data_summary.reset_index()

# merge possshifts to summary
data_summary = pd.merge(data_summary, data_possshifts, on = cols2, how = 'outer')
data_summary = data_summary.drop({'_TYPE_', '_FREQ_'}, 1)
data_summary = data_summary.rename(columns = {'NUMERATOR': 'POSS_SHIFT_CROSS'})

# merge totsampcrossings to summary
data_summary = pd.merge(data_summary, data_totsampcrossings, on = cols2, how = 'outer')
data_summary = data_summary.drop({'_TYPE_', '_FREQ_'}, 1)
data_summary = data_summary.rename(columns = {'DENOMINATOR': 'SAMP_SHIFT_CROSS'})

# merge totsampshifts to summary
data_summary = pd.merge(data_summary, data_totsampshifts, on = cols2, how = 'outer')
data_summary = data_summary.drop({'_TYPE_', '_FREQ_'}, 1)
data_summary = data_summary.rename(columns = {'DENOMINATOR': 'SAMP_SHIFT_CROSS_TEMP'})

# merge totsampcrossings and totsampshifts to single column via addition
data_summary['SAMP_SHIFT_CROSS'] = data_summary.SAMP_SHIFT_CROSS.fillna(0) \
    + data_summary.SAMP_SHIFT_CROSS_TEMP.fillna(0)
data_summary = data_summary.drop(['SAMP_SHIFT_CROSS_TEMP'], 1)

# sort summary
data_summary = data_summary.sort_values(cols2)

# sort surveydata
data_surveydata = data_surveydata.sort_values(cols3)

# groupby and aggregate summary high
data_summary_high = data_surveydata.groupby(cols3)['SHIFT_WT'].agg({
    'COUNT_RESPS': 'count',
    'SUM_SH_WT': 'sum',
    'MIN_SH_WT': 'min',
    'MEAN_SH_WT': 'mean',
    'MAX_SH_WT': 'max'
})

# flatten summary high
data_summary_high = data_summary_high.reset_index()

# append totsampcrossings to totsampshifts
data_totsampshifts.append(data_totsampcrossings)

# sort totsampshifts
data_totsampshifts = data_totsampshifts.sort_values(cols3)

# groupby and aggregate summary high sampled
data_summary_high_sampled = data_totsampshifts.groupby(cols3)['DENOMINATOR'].agg({
    'SAMP_SHIFT_CROSS': 'sum'
})

# flatten summary high sampled
data_summary_high_sampled = data_summary_high_sampled.reset_index()

# left merge summary high with summary high sampled
data_summary_high = pd.merge(data_summary_high, data_summary_high_sampled,
    on = cols3, how = 'left')

# append summary and summary high
data_summary = data_summary.append(data_summary_high)

# set summary columns
data_summary = data_summary[cols5]

# sort summary
data_summary = data_summary.sort_values(cols5)

# set surveydata columns
data_surveydata = data_surveydata[cols6]

# sort surveydata
data_surveydata = data_surveydata.sort_values(cols6)