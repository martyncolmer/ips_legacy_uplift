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



#-------------------------------- Survey Data - Sort --------------------------------#

computeColumns = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV']
surveyDataSorted = sampledShifts.sort_values(computeColumns)


psData = 'possibleShifts.csv'
tssData = 'totalSampledShifts.csv'
possibleShifts = pd.read_csv(psData)
totalSampledShifts = pd.read_csv(tssData)


mergedDF = pd.merge(surveyDataSorted,possibleShifts,on = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV'])
print("---------------------------------------------------------------------------")
print("---------------------------------------------------------------------------")
print("---------------------------------------------------------------------------")
print(possibleShifts.head(10))
print("---------------------------------------------------------------------------")
print("---------------------------------------------------------------------------")
print("---------------------------------------------------------------------------")
print(totalSampledShifts.head(10))
print("---------------------------------------------------------------------------")
print("---------------------------------------------------------------------------")
print("---------------------------------------------------------------------------")
#mergedDF['Shift_Factor'] = mergedDF.NUMERATOR / mergedDF.DENOMINATOR


print (mergedDF.head(10))



