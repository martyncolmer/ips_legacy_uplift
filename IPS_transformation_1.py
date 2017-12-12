
# coding: utf-8

# In[601]:

import pandas as pd
from pandas.util.testing import assert_frame_equal
import numpy as np
from sas7bdat import SAS7BDAT
import os
from collections import OrderedDict


# In[640]:

# paths used in notebook for development and testing

path_to_test_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\test.txt"
path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\surveydata.sas7bdat"
path_to_shifts_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\shiftsdata.sas7bdat"
test_path = r"D:\test.txt"

test_column_sampledcrossings = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor\column_sampledcrossings.sas7bdat"
test_totalSampledCrossings = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor\totalsampledcrossings.sas7bdat"
test_totalCrossings = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor\totalcrossings.sas7bdat"

test_survey_data_merge = r"C:\TestScripts\surveydata_merge.sas7bdat"


# In[603]:

# -----------------------------------------------------
# function definitions
# -----------------------------------------------------

# load sas file and convert to a pandas dataframe
def load_sas(sasfile, encoding="utf8", encoding_errors="replace"):

    with SAS7BDAT(sasfile, encoding=encoding,encoding_errors=encoding_errors) as sas:
        sas = iter(sas)
        columns = [c for c in next(sas)]
        df = pd.DataFrame(sas, columns=columns)
        return df


# In[260]:

# -----------------------------------------------------
#load sas files into dataframes
# -----------------------------------------------------

df_survey_data = load_sas(path_to_survey_data)
df_shifts_data = load_sas(path_to_shifts_data)


# In[604]:

#checks
type(df_survey_data)


# In[605]:

#checks
type(df_shifts_data)


# In[345]:

#checks
df_survey_data.head()


# In[346]:

#checks
df_shifts_data.head()


# In[610]:

# -----------------------------------------------------
# calculate_ips_crossings_factor
#
# SAS code for reference:
# 
# -----------------------------------------------------

#%do_ips_shftweight_calculation(surveyData, shiftsData, out, summary,
#							   &ShiftsStratumDef, &var_serialNum,
#							   &var_shiftFlag, &var_shiftFactor, &var_totals,
#							   &var_shiftNumber, &var_crossingFlag,
#							   &var_crossingsFactor, &var_crossingNumber,
#							   &var_SI, &var_shiftWeight, &var_count,
#							   &var_weightSum, &var_minWeight, &var_avgWeight,
#							   &var_maxWeight, &var_summaryKey, &subStrata,
#							   &var_possibleCount, &var_sampledCount, &minWeightThresh,
#							   &maxWeightThresh);


#%macro do_ips_shftweight_calculation(in, shiftData, out, summary, 
#									 ShiftsStratumDef, var_serialNum, var_shiftFlag, 
#									 var_shiftFactor, var_totals, var_shiftNumber, 
#									 var_crossingFlag, var_crossingsFactor, 
#									 var_crossingNumber, var_SI, var_shiftWeight,
#									 var_count, var_weightSum, var_minWeight,
#									 var_avgWeight, var_maxWeight, var_summaryKey,
#									 substrata, var_possibleCount, var_sampledCount,
#									 minWeightThresh, maxWeightThresh);

#%calculate_ips_crossings_factor(&shiftData, &in, &ShiftsStratumDef, &var_crossingFlag, 
#                                &var_shiftNumber, &var_crossingNumber, &var_crossingsFactor, 
#                                &var_totals);

#macro calculate_ips_crossings_factor(crossingsData, OutputData, StratumDef, 
#                                     crossingFlag, shiftNumber, crossingNumber,
#                                     crossingsFactor, totals);	

# -----------------------------------------------------
# assign all the variable values and make upper case
# as all table column names are upper case
# -----------------------------------------------------

# ShiftsStratumDef = Variable holding the shift weight stratum definition
StratumDef_temp = ["shift_port_grp_pv", "arrivedepart", "weekday_end_pv", "am_pm_night_pv"] #ShiftsStratumDef

# table fields are upper case, so require conversion to upper case
StratumDef = list(map(lambda x: x.upper(), StratumDef_temp))

# crossingFlag = variable that indicates that this record is crossing 
crossingFlag = "crossings_flag_pv".upper() #var_crossingFlag 

# var_shiftNumber = Variable holding the shift number
shiftNumber = "shiftno".upper() #var_shiftNumber

# var_crossingNumber = Variable holding the crossing number
crossingNumber = "shuttle".upper() #var_crossingNumber

# var_crossingsFactor = Variable holding the name of the crossings factor
crossingsFactor = "crossings_factor" # it is lower case.upper() #var_crossingsFactor

# var_totals = Variable holding the number of possible shifts / total
totals = "total".upper() #var_totals

# -----------------------------------------------------
# set the new dataframes from SAS datasets
# -----------------------------------------------------
df_crossingsData = df_shifts_data # (334, 9)
df_outputData = df_survey_data # shape=(26347, 212)

# -----------------------------------------------------
# Get survey records that are crossing based
# -----------------------------------------------------
df_sampled_crossings = df_survey_data.loc[df_survey_data[crossingFlag] == 1] # shape=(3084, 212)


# In[611]:

# -----------------------------------------------------
# calculate the number of sampled crossings by strata 
# -----------------------------------------------------

#proc sort data = sampledCrossings (keep = &StratumDef &shiftNumber &crossingNumber) nodupkey;
#by &StratumDef &shiftNumber &crossingNumber;
#run;

# keep, sort and drop duplicates
selected_columns = StratumDef + [shiftNumber, crossingNumber]

temp_d1 = df_sampled_crossings[selected_columns]
df_sorted_sampled_crossings = temp_d1.sort_values(selected_columns).drop_duplicates()# shape= (180,6)

# reindex the dataframe
df_sorted_sampled_crossings.index = range(df_sorted_sampled_crossings.shape[0])

# ------------------------------------------
# test tables are the same as ground 
# truth from sas
# ------------------------------------------

# read in the SAS version of sorted data
df_test_column_sampledcrossings = load_sas(test_column_sampledcrossings)

# check dataframes are equal
assert_frame_equal(df_sorted_sampled_crossings, df_test_column_sampledcrossings, check_dtype=False)
#df_sorted_sampled_crossings


# In[612]:

#proc summary data = sampledCrossings;
#var &crossingNumber;
#by &StratumDef; 
#output out = totalSampledCrossings N = denominator;
#run;

# note - we require reset_index() here to compose the correctly laid out dataframe
df_totalSampledCrossings = df_sorted_sampled_crossings.groupby(StratumDef)[crossingNumber]                                                 .agg(OrderedDict([('_FREQ_', 'count'),('denominator', 'count')]))                                                 .reset_index() # shape = (36, 6)

# note - not required but put incase required in future for similar
df_totalSampledCrossings.index = range(df_totalSampledCrossings.shape[0])

# insert the constant class type in this case as no class specified in SAS proc
df_totalSampledCrossings.insert(4, "_TYPE_", 0)

# ------------------------------------------
# test tables are the same as read from sas
# ------------------------------------------
df_test_totalSampledCrossings = load_sas(test_totalSampledCrossings)

# check dataframes are equal
assert_frame_equal(df_totalSampledCrossings, df_test_totalSampledCrossings, check_dtype=False)
#df_totalSampledCrossings


# In[642]:

# -----------------------------------------------------
# calculate the total number of crossings by strata
# -----------------------------------------------------
#proc sort data = &crossingsData;
#by &StratumDef;
#run;

# sort the data (if required)
df_sorted_crossingsData = df_crossingsData.sort_values(StratumDef)

#print(df_crossingsData.shape)
#print(df_sorted_crossingsData.shape)

# proc summary data = &crossingsData;
# var &totals;
# by &StratumDef;
# output out = totalCrossings sum=numerator;
# run;

# note - ensure we reindex to correctly format the dataframe
# df_totalCrossings = df_sorted_crossingsData.groupby(StratumDef)[totals]\
#                                             .agg({'numerator':'sum'}).reset_index() # shape = (286, 1)

# note - we require reset_index() here to compose the correctly laid out dataframe
df_totalCrossings = df_sorted_crossingsData.groupby(StratumDef)[totals]                                                 .agg(OrderedDict([('_FREQ_', 'count'),('numerator', 'sum')]))                                                 .reset_index() # shape = (36, 6)        

df_totalCrossings.index = range(df_totalCrossings.shape[0])

# insert the constant class type in this case as no class specified in SAS proc
df_totalCrossings.insert(4, "_TYPE_", 0)

# # ------------------------------------------
# # test tables are the same as read from sas
# # ------------------------------------------
df_test_totalCrossings = load_sas(test_totalCrossings)

# # check dataframes are equal
assert_frame_equal(df_totalCrossings, df_test_totalCrossings, check_dtype=False)
# df_totalSampledCrossings
# df_sorted_crossingsData



# In[641]:

# -----------------------------------------------------
# now compute the crossings factor 
# -----------------------------------------------------

# proc sort data = &OutputData;
# by &StratumDef;
# run;

df_sorted_outputData = df_outputData.sort_values(StratumDef)

# -----------------------------------------------
#
# SAS code for reference
#
# -----------------------------------------------

# data &OutputData (drop = numerator denominator);
# merge &OutputData (in=a)
# totalCrossings (keep = &StratumDef numerator)
# totalSampledCrossings (keep = &StratumDef denominator);
# by &StratumDef;

# if a;

# if (&crossingFlag ne 0) then do;

# 	/* check for division by zero */
# 	if denominator ne 0 then
# 		&crossingsFactor = numerator / denominator;
# end;

# run;

# data testdata.surveydata_merge (drop = numerator denominator);
# merge testdata.surveydata (in=a) testdata.totalCrossings (keep = shift_port_grp_pv arrivedepart weekday_end_pv 
# am_pm_night_pv numerator) testdata.totalSampledCrossings (keep = shift_port_grp_pv arrivedepart weekday_end_pv am_pm_night_pv denominator);
# by shift_port_grp_pv arrivedepart weekday_end_pv am_pm_night_pv;
# if a;
# if (crossings_flag_pv  ne 0) then do;
# 	/* check for division by zero */
# 	if denominator ne 0
#     then crossings_factor  = numerator / denominator;
# end;
# run;

df_sorted_outputData #keep all
df_totalCrossings  = df_totalCrossings[StratumDef + ['numerator']] # keep &StratumDef numerator
df_totalSampledCrossings = df_totalSampledCrossings[StratumDef + ['denominator']] # (keep = &StratumDef denominator);

left_join_1 = df_sorted_outputData.merge(df_totalCrossings, on=StratumDef, how='left')
left_join_2 = left_join_1.merge(df_totalSampledCrossings, on=StratumDef, how='left')

# if crossingFlag column not equal 1:
#    if denominator not equal 0:
#        crossing_factor_column = numerator/denominator

# ------------------------------------------------------------

#function for calculating the crossings factor through mapping columns
def g(row):
    if(row[crossingFlag] != 0 and row['denominator'] != 0):        
        return row['numerator']/row['denominator']       
    else:       
        return None

# calculate crossings factor    
left_join_2[crossingsFactor] = left_join_2.apply(g, axis=1)

# drop numerator and denominator columns
df_final = left_join_2.drop(['numerator', 'denominator'], 1)  

# checks
#df[df.b > 10]
#df_final[df_final.crossings_factor > 0]

# # ------------------------------------------
# # test tables are the same as read from sas
# # ------------------------------------------
df_test_survey_data_merge = load_sas(test_survey_data_merge)

# # check dataframes are equal
assert_frame_equal(df_final, df_test_survey_data_merge, check_dtype=False)



# In[645]:

assert_frame_equal(df_final, df_test_survey_data_merge, check_dtype=False)


# In[579]:

# ---------------------------------
#
#Some test dataframes and code:
#
# ---------------------------------
df = pd.DataFrame({'crossing_flag': [1, 0, 0, 2],
                    #'crossing_factor': [0, 0, 0, 0],
                    'numerator': [3,2,1,10],
                    'denominator': [0,0,1,3]})


# In[580]:

df


# In[583]:

#f = df['A']/df['B'] 

# 
# def f(x):
#     return x['crossing_flag']
# 
# def g(x):
#     if(x['crossing_flag'] != 0 and x['denominator'] != 0):        
#         return x['numerator']/x['denominator']       
#     else:
#         #x['crossing_factor'] = x['crossing_factor']
#         #return x['crossing_factor']
#         return 0
#     
#     
# df['crossing_factor'] = df.apply(g, axis=1)
# df


# In[ ]:



