from IPSTransformation import CommonFunctions as cf
import pandas as pd
from sas7bdat import SAS7BDAT
import sys
from pandas.util.testing import assert_frame_equal
import numpy as np
import os
from collections import OrderedDict

def calculate_ips_shift_factor():
    
    print("START - calculate_ips_shift_factor")
    
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

    print("DONE - calculate_ips_shift_factor")

def calculate_ips_crossing_factor():
    
    print("START - calculate_ips_crossing_factor")
    
    # modify or remove these as appropriate
    path_to_test_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor\test.txt"
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor\surveydata.sas7bdat"
    path_to_shifts_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor\shiftsdata.sas7bdat"
    
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
        
    # -----------------------------------------------------
    #load sas files into dataframes
    # -----------------------------------------------------



    df_survey_data = SAS7BDAT(path_to_survey_data).to_data_frame()
    df_shifts_data = SAS7BDAT(path_to_shifts_data).to_data_frame()
    
    
    
    #df_survey_data = load_sas(path_to_survey_data)
    #df_shifts_data = load_sas(path_to_shifts_data)
    
    
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
    
    # -----------------------------------------------------
    # calculate the number of sampled crossings by strata 
    # -----------------------------------------------------
    
    # keep, sort and drop duplicates
    selected_columns = StratumDef + [shiftNumber, crossingNumber]
    
    temp_d1 = df_sampled_crossings[selected_columns]
    df_sorted_sampled_crossings = temp_d1.sort_values(selected_columns).drop_duplicates()# shape= (180,6)
    
    # reindex the dataframe
    df_sorted_sampled_crossings.index = range(df_sorted_sampled_crossings.shape[0])
    
    # note - we require reset_index() here to compose the correctly laid out dataframe
    df_totalSampledCrossings = df_sorted_sampled_crossings.groupby(StratumDef)[crossingNumber]\
                                                     .agg(OrderedDict([('_FREQ_', 'count'),('denominator', 'count')]))\
                                                     .reset_index() # shape = (36, 6)
    
    # note - not required but put incase required in future for similar
    df_totalSampledCrossings.index = range(df_totalSampledCrossings.shape[0])
    
    # insert the constant class type in this case as no class specified in SAS proc
    df_totalSampledCrossings.insert(4, "_TYPE_", 0)
    
    # -----------------------------------------------------
    # calculate the total number of crossings by strata
    # -----------------------------------------------------
    
    # sort the data (if required)
    df_sorted_crossingsData = df_crossingsData.sort_values(StratumDef)
    
    # note - we require reset_index() here to compose the correctly laid out dataframe
    df_totalCrossings = df_sorted_crossingsData.groupby(StratumDef)[totals]\
                                                     .agg(OrderedDict([('_FREQ_', 'count'),('numerator', 'sum')]))\
                                                     .reset_index() # shape = (36, 6)        
    
    df_totalCrossings.index = range(df_totalCrossings.shape[0])
    
    # insert the constant class type in this case as no class specified in SAS proc
    df_totalCrossings.insert(4, "_TYPE_", 0)
    
    # -----------------------------------------------------
    # now compute the crossings factor 
    # -----------------------------------------------------
    
    df_sorted_outputData = df_outputData.sort_values(StratumDef)
    
    df_sorted_outputData #keep all
    df_totalCrossings  = df_totalCrossings[StratumDef + ['numerator']] # keep &StratumDef numerator
    df_totalSampledCrossings = df_totalSampledCrossings[StratumDef + ['denominator']] # (keep = &StratumDef denominator);
    
    left_join_1 = df_sorted_outputData.merge(df_totalCrossings, on=StratumDef, how='left')\
                                      .merge(df_totalSampledCrossings, on=StratumDef, how='left')
    
    # if crossingFlag column not equal 1:
    #    if denominator not equal 0:
    #        crossing_factor_column = numerator/denominator
    #function for calculating the crossings factor through mapping columns
    def g(row):
        if(row[crossingFlag] != 0 and row['denominator'] != 0):        
            return row['numerator']/row['denominator']       
        else:       
            return None
    
    # calculate crossings factor    
    left_join_1[crossingsFactor] = left_join_1.apply(g, axis=1)
    
    # drop numerator and denominator columns
    df_final = left_join_1.drop(['numerator', 'denominator'], 1)
    
    print("DONE - calculate_ips_crossing_factor")

def do_ips_shift_weight_calculation():
    
    calculate_ips_shift_factor()
    
    calculate_ips_crossing_factor()
    
    # Calculate the shift weight
    print("Process - calculate the shift weight")
    
    # Check for missing and invalid values
    print("Process - Check for missing and invalid values")
    
    # Produce shift weight summary
    print("Process - Produce shift weight summary")
    
    # Report any weights not in bounds
    print("Process - Report any weights not in bounds")
    


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