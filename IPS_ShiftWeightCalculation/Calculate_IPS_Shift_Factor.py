'''
Created on 6 Dec 2017

@author: mahont1
'''
import pandas as pd
from sas7bdat import SAS7BDAT

#****************************************************#
#* Calculate the number of sampled shifts by strata *#
#****************************************************#

#------------------------------ Sampled Shifts - Import ------------------------------#

surveyData = 'surveydata.sas7bdat'

surveyDataDF = pd.read_sas(surveyData)
cols = 'SHIFT_FLAG_PV'
sampledShifts = surveyDataDF[surveyDataDF[cols] == 1]
sampledShifts.dropna()

#------------------------------- Sampled Shifts - Sort -------------------------------#

columns = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV','SHIFTNO']
columns2= ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV']
sampledShiftsSmall = sampledShifts[columns].drop_duplicates()
sampledShiftsSorted = sampledShiftsSmall.sort_values(columns)

#------------------------------ Sampled Shifts - Summary ------------------------------#


print(sampledShiftsSorted)

print(sampledShiftsSorted.groupby(columns2)['SHIFTNO'].agg({'DENOMINATOR':'count'}))
sampledShiftsSorted.groupby(columns2)['SHIFTNO'].agg({'DENOMINATOR':'count'}).to_csv('totalSampledShifts.csv')


#Output - totalSampledShifts.csv



#*****************************************************#
#* Calculate the number of possible shifts by strata *#
#*****************************************************#

#-------------------------------- Shifts Data - Import --------------------------------#

shiftsData = 'shiftsdata.sas7bdat'

shiftsDataDF = pd.read_sas(shiftsData)
print(shiftsDataDF)

#--------------------------------- Shifts Data - Sort ---------------------------------#

shiftsDataColumns = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV']
shiftsDataSorted = shiftsDataDF.sort_values(shiftsDataColumns)

#-------------------------------- Shifts Data - Summary --------------------------------#

print(shiftsDataSorted.groupby(shiftsDataColumns)['TOTAL'].agg({'NUMERATOR':'sum'}))
shiftsDataSorted.groupby(shiftsDataColumns)['TOTAL'].agg({'NUMERATOR':'sum'}).to_csv('possibleShifts.csv')


#Output - possibleShifts.csv



#********************************#
#*   Compute the shift factor   *#
#********************************#



#-------------------------------- Merge --------------------------------#

# Sort (Filtered) SurveyData
computeColumns = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV']
surveyDataSorted = sampledShifts.sort_values(computeColumns)

# Files to be merged
psData = 'possibleShifts.csv'
tssData = 'totalSampledShifts.csv'

# Read CSV's into Pandas data frame
possibleShifts = pd.read_csv(psData)
totalSampledShifts = pd.read_csv(tssData)

# Merge loaded data
mergedDF = pd.merge(surveyDataSorted,possibleShifts,on = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV'])
mergedDF = pd.merge(mergedDF,totalSampledShifts,on = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV'])

mergedDF['Shift_Factor'] = mergedDF.NUMERATOR / mergedDF.DENOMINATOR



mergedDF2 = pd.merge(surveyDataDF,mergedDF,'outer')
mergedDF2 = mergedDF2.drop(['NUMERATOR','DENOMINATOR'],axis = 1)
mergedDF2 = mergedDF2.sort_values(by=['SERIAL'])
mergedDF2.to_csv('surveydata_merge.csv',index=False)
 
