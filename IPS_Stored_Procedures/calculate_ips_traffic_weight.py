from IPSTransformation import CommonFunctions as cf
import pandas as pd
from sas7bdat import SAS7BDAT
import sys

def calculate_ips_shift_factor():
    
    print("In calculate shift factor")
    
    #****************************************************#
    #*      Calculate the number of sampled shifts      *#
    #****************************************************#
    
    #------------------------------ Sampled Shifts - Import ------------------------------#
    
    """This works for survey data but not merged (index errors). But is much faster than SAS7BDAT().to_data_frame()"""
    #surveyData = 'InputFiles/surveydata.sas7bdat'
    #surveyDataDF = pd.read_sas(surveyData)
    filename = r'//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'surveydata.sas7bdat'
    #filename = 'InputFiles/surveydata.sas7bdat'                                                                                               #####filein surveydata
    surveyDataDF = SAS7BDAT(filename).to_data_frame()
    
    print (surveyDataDF)
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
    sampledShiftsSorted.groupby(columns2)['SHIFTNO'].agg({'DENOMINATOR':'count'}).to_csv('//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'totalSampledShifts.csv')
    
    
    #Output - totalSampledShifts.csv
    
    
    
    #*****************************************************#
    #* Calculate the number of possible shifts by strata *#
    #*****************************************************#
    
    #-------------------------------- Shifts Data - Import --------------------------------#
    
    shiftsData = r'//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'shiftsdata.sas7bdat'                                                                                     #####filein - shiftsData
    
    shiftsDataDF = pd.read_sas(shiftsData)
    print(shiftsDataDF)
    
    #--------------------------------- Shifts Data - Sort ---------------------------------#
    
    shiftsDataColumns = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV']
    shiftsDataSorted = shiftsDataDF.sort_values(shiftsDataColumns)
    
    #-------------------------------- Shifts Data - Summary --------------------------------#
    
    print(shiftsDataSorted.groupby(shiftsDataColumns)['TOTAL'].agg({'NUMERATOR':'sum'}))
    shiftsDataSorted.groupby(shiftsDataColumns)['TOTAL'].agg({'NUMERATOR':'sum'}).to_csv(r'//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'possibleShifts.csv')              #####fileout - possibleShifts
    
    
    #Output - possibleShifts.csv
    
    
    
    #********************************#
    #*   Compute the shift factor   *#
    #********************************#
    
    
    
    #-------------------------------- Merge --------------------------------#
    
    # Sort (Filtered) SurveyData
    computeColumns = ['SHIFT_PORT_GRP_PV','ARRIVEDEPART','WEEKDAY_END_PV','AM_PM_NIGHT_PV']
    surveyDataSorted = sampledShifts.sort_values(computeColumns)
    
    # Files to be merged
    psData = r'//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'possibleShifts.csv'                                                                                          #####filein - possibleShifts
    tssData = r'//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'totalSampledShifts.csv'                                                                                     #####filein - totalSampledShifts
    
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
    mergedDF2.to_csv(r'//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor/ExampleDir/' + 'surveydata_merge.csv',index=False)


def calculate_ips_crossing_factor():
    pass


def do_ips_shift_weight_calculation():
    
    calculate_ips_shift_factor()
    calculate_ips_crossing_factor()
    
    # Calculate the shift weight
    
    # Check for missing and invalid values
    
    # Produce shift weight summary
    
    # Report any weights not in bounds
    
    pass


"""Files"""
workingDir = (r"//nsdata3/Social_Surveys_team/CASPA/IPS/Testing/shiftfactor")


#Connect to Oracle                    #/
conn = cf.get_oracle_connection()

#ipsErrorCheck                        #/
cf.ips_error_check()

#get data from oracle (shift data)    #X
                                  
#doIPS shift weight calculation       #/
do_ips_shift_weight_calculation()
                                  
#append data to output table          #X
                                                 
#commit ips response                  #/               
#cf.commit_ips_response()

print("done")