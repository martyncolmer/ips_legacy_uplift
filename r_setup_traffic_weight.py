'''
Created on 1 Jun 2018

@author: poweld2
'''

import pandas as pd
from sas7bdat import SAS7BDAT

# Load survey Data
df_survey_input = pd.read_csv("D:/survey_input.csv", encoding = "ISO-8859-1")

# Sort input values
sort1 = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
df_survey_input_sorted = df_survey_input.sort_values(sort1)

# Cleanse data
df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['SAMP_PORT_GRP_PV'].isnull()]
df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

# Create lookup. Group by and aggregate
lookup_dataframe = df_survey_input_sorted.copy()
lookup_dataframe["count"] = ""
lookup_dataframe = lookup_dataframe.groupby(['SAMP_PORT_GRP_PV',
                                             'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

# Cleanse data
lookup_dataframe.drop(["count"], axis=1)
lookup_dataframe["T1"] = range(len(lookup_dataframe))
lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

# Merge lookup data in to source dataframe
df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['SAMP_PORT_GRP_PV',
                                                                          'ARRIVEDEPART'], how='left')

# Load GES data
input = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\gesinput_1.sas7bdat"
foo = SAS7BDAT(input)
df_gesinput = foo.to_data_frame()

# Cleanse data
df_gesinput = df_gesinput.rename(index=str, columns={"GES_serial": "SERIAL"})

# Sort values ready for merge
df_survey_input_serial = df_survey_input.sort_values(['SERIAL'])
df_gesinput_sorted = df_gesinput.sort_values(['SERIAL'])
df_aux_variables_sorted = df_aux_variables.sort_values(['SERIAL'])

# Cleanse and merge data ready for input into R GES
df_aux_variables_column = df_aux_variables_sorted[['SERIAL','T1']]

df_trafdesignwt = pd.merge(df_survey_input_serial, df_gesinput_sorted, on=['SERIAL'], how='left')
df_trafdesignwt = df_trafdesignwt.drop(columns=['ARRIVEDEPART_y', 'SAMP_PORT_GRP_PV_y'])
df_trafdesignwt = df_trafdesignwt.rename(index=str, columns={"ARRIVEDEPART_x": "ARRIVEDEPART"})

df_r_ges_input = pd.merge(df_trafdesignwt, df_aux_variables_column, on=['SERIAL'], how='left')
df_r_ges_input = df_r_ges_input[~df_r_ges_input['T1'].isnull()]

# Export dataframes to CSV
df_r_ges_input.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\df_r_ges_input.csv")

# Prepare population totals to create AUX lookup variables

# Load data
input = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\trtotals.sas7bdat"
foo = SAS7BDAT(input)
df_pop_totals = foo.to_data_frame()

# Cleanse data
df_pop_totals = df_pop_totals[~df_pop_totals['SAMP_PORT_GRP_PV'].isnull()]
df_pop_totals = df_pop_totals[~df_pop_totals['ARRIVEDEPART'].isnull()]

# Create unique list of items from survey input
items = df_survey_input_sorted['SAMP_PORT_GRP_PV'].tolist()
unique = []
[unique.append(x) for x in items if x not in unique]

df_pop_totals_match = df_pop_totals[df_pop_totals['SAMP_PORT_GRP_PV'].isin(unique)]

# Create traffic totals
df_pop_totals_match = df_pop_totals_match.sort_values(['ARRIVEDEPART','SAMP_PORT_GRP_PV'])
df_traffic_totals = df_pop_totals_match.groupby(['SAMP_PORT_GRP_PV',
                                             'ARRIVEDEPART']).agg({"TRAFFICTOTAL": 'sum'}).reset_index()

# Create population totals for current survey data - Cleanse data and merge
lookup_dataframe_aux = lookup_dataframe[['SAMP_PORT_GRP_PV','ARRIVEDEPART','T1']]
df_mod_totals = pd.merge(df_traffic_totals, lookup_dataframe_aux, on=['SAMP_PORT_GRP_PV','ARRIVEDEPART'], how='left')
C_group = 1
df_mod_totals['C_group'] = C_group
df_mod_totals = df_mod_totals.drop(columns=['ARRIVEDEPART', 'SAMP_PORT_GRP_PV'])
df_modpop_totals = df_mod_totals.pivot_table(index='C_group',
                             columns='T1',
                             values='TRAFFICTOTAL')
df_modpop_totals = df_modpop_totals.add_prefix('T_')

# Export dataframes to CSV
df_modpop_totals.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\df_modpop_totals.csv")








