import pandas as pd
from sas7bdat import SAS7BDAT

# Load survey Data
input = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\unsampled weight\survey_input.sas7bdat"
foo = SAS7BDAT(input)
df_survey_input = foo.to_data_frame()

# Sort input values
sort1 = ['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV','ARRIVEDEPART']
df_survey_input_sorted = df_survey_input.sort_values(sort1)

# Cleanse data
df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['UNSAMP_PORT_GRP_PV'].isnull()]
df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]
df_survey_input_sorted.UNSAMP_REGION_GRP_PV.fillna(value=0, inplace=True)

# Create lookup. Group by and aggregate
lookup_dataframe = df_survey_input_sorted.copy()
lookup_dataframe["count"] = ""
lookup_dataframe = lookup_dataframe.groupby(['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV',
                                             'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

# Cleanse data
lookup_dataframe.drop(["count"], axis=1)
lookup_dataframe["T1"] = range(len(lookup_dataframe))
lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

# Merge lookup data in to source dataframe
df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['UNSAMP_PORT_GRP_PV','UNSAMP_REGION_GRP_PV',
                                                                          'ARRIVEDEPART'], how='left')

# Load data
input = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\poprowvec.sas7bdat"
foo = SAS7BDAT(input)
df_modpop_totals = foo.to_data_frame()

# Export dataframes to CSV
df_modpop_totals.to_csv(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Dec_Data\Traffic_data\df_modpop_totals.csv")