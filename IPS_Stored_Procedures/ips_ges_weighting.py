import sys
import os
import logging
import inspect
import numpy as np
import pandas as pd
from sas7bdat import SAS7BDAT
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict
import survey_support
from IPSTransformation import CommonFunctions as cf
import math
    
def compare_dfs(test_name, sas_file, df, col_list = False):
    
    import winsound
    
    def beep():
        frequency = 500  # Set Frequency To 2500 Hertz
        duration = 200  # Set Duration To 1000 ms == 1 second
        winsound.Beep(frequency, duration)
    
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Weight"
    print sas_root + "\\" + sas_file
    csv = pd.read_sas(sas_root + "\\" + sas_file)
    
    fdir = r"\\NDATA12\mahont1$\My Documents\GIT_Repositories\Test_Drop"
    sas = "_sas.csv"
    py = "_py.csv"
    
    print("TESTING " + test_name)
    
    if col_list == False:
        csv.to_csv(fdir+"\\"+test_name+sas)
        df.to_csv(fdir+"\\"+test_name+py)
    else:
        csv[col_list].to_csv(fdir+"\\"+test_name+sas)
        df[col_list].to_csv(fdir+"\\"+test_name+py)
    
    print(test_name + " COMPLETE")
    beep()
    print("") 
    
    
def ips_check_ges_totals(SampleData, SummarisedPopulation, StrataDef, var_sample,    
                        var_population):
    """
    Author       : Thomas Mahoney
    Date         : 08 / 03 / 2018
    Purpose      : Checks sample and population totals prior to IPS GES weighting.
    Parameters   : SampleData - the sample data                                
                   SummarisedPopulation - the population data summarised by the strata                            
                   StrataDef - List of classificatory variables          
                   var_sample - variable holding the sample design weights                
                   var_population - variable holding the population totals  
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Sort the sample data by strata
    SampleData = SampleData.sort_values(by = StrataDef)
    
    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process. 
    rsumsamp = SampleData.fillna('NOTHING')
    
    # Summarise the OOHDesignWeight over the strata
    rsumsamp = rsumsamp.groupby(StrataDef)[var_sample].agg({var_sample : 'sum'})
    rsumsamp.reset_index(inplace = True)
    
    # Replace the previously added 'NOTHING' values with their original blank values  
    rsumsamp = rsumsamp.replace('NOTHING', np.NaN)

    # Sort the population totals by strata
    SummarisedPopulation = SummarisedPopulation.sort_values(by = StrataDef)
        
    # Merge the two and identify any areas that can't be aligned
    df_merged = pd.merge(rsumsamp,SummarisedPopulation, on = StrataDef, how = 'left')
    
    # Collect rows that are missing population data
    df_missing_pop = df_merged[((df_merged[var_sample] > 0) 
                                & ((df_merged[var_population] <= 0) 
                                   | df_merged[var_population].isnull())
                                )]

    # Collect rows that are missing sample data
    df_missing_samp = df_merged[((df_merged[var_population] > 0) 
                                & ((df_merged[var_sample] <= 0) 
                                   | df_merged[var_population].isnull())
                                )]
    
    # Select strata columns
    df_missing_pop = df_missing_pop[StrataDef]
    
    error_string = ""
    for index, record in df_missing_pop.iterrows():
        error_string += "___||___" \
                         + df_missing_pop.columns[0] + " : " + str(record[0]) + " | "\
                         + df_missing_pop.columns[1] + " : " + str(record[1]) + " | "\
                         + df_missing_pop.columns[2] + " : " + str(record[2]) + " | "
                         
    # Report missing population values if any
    if len(df_missing_pop) > 0:
        error_string = 'ERROR: No traffic total but sampled records present for : ' + error_string
        error_string = error_string[:250]
        cf.database_logger().error(error_string)
        print("Error thrown in ges data - stopping application. This will be implemented when the application is built.")
        sys.exit()
    
    
    # Select strata columns
    df_missing_samp = df_missing_samp[StrataDef]
    
    error_string = ""
    for index, record in df_missing_samp.iterrows():
        error_string += "___||___" \
                         + df_missing_samp.columns[0] + " : " + str(record[0]) + " | "\
                         + df_missing_samp.columns[1] + " : " + str(record[1]) + " | "\
                         + df_missing_samp.columns[2] + " : " + str(record[2]) + " | "
    
    # Report missing sample values if any
    if len(df_missing_samp) > 0:
        error_string = 'ERROR: No records to match traffic against for : ' + error_string
        error_string = error_string[:250]
        cf.database_logger().error(error_string)
        print("Error thrown in ges data - stopping application. This will be implemented when the application is built.")
        sys.exit()


def ips_setup_ges_auxvars(df_poptotals, StrataDef, AuxVar, TotVar, 
                          AuxVarPrefix, TotVarPrefix):
    """
    Author       : Thomas Mahoney
    Date         : 08 / 03 / 2018
    Purpose      : Creates the GES cvset file and assigns the auxiliary count to a macro 
                   variable.
    Parameters   : df_poptotals - Population totals file   
                   StrataDef - List of classificatory variables          
                   AuxVar - Variable that will hold the names of the auxiliary variables
                   TotVar - Variable that will holds the names of the total variables
                   AuxVarPrefix - Prefix for auxiliary variables (e.g. Y_)
                   TotVarPrefix - Prefix for total variables (e.g. T_)
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    
    # Create the model definition data frame 
    df_model_definition = df_poptotals[StrataDef]
    
    # Set the aux variable count
    AuxVarCount = len(df_model_definition.index)
    
    
    # Set up the aux variable number format macro
    AuxNumFormat = int(math.log10(AuxVarCount)) + 1
    
    # Generate the df_cv_set values
    av = [""]*AuxVarCount
    tv = [""]*AuxVarCount
    for x in range (1,AuxVarCount+1):
        av[x-1] = AuxVarPrefix + str(x)[:AuxNumFormat]
        tv[x-1] = TotVarPrefix + str(x)[:AuxNumFormat]
        
    
    # Create the cvset data frame
    df_cv_set = df_poptotals
    df_cv_set[AuxVar] = av
    df_cv_set[TotVar] = tv
    df_cv_set = df_cv_set[[AuxVar,TotVar]]
    
    return df_cv_set, df_model_definition, AuxVar, TotVar, AuxVarCount, AuxNumFormat 
    

def do_ips_ges_weighting(df_input, SerialNumVarName, DesignWeightVarName, 
                         StrataDef, df_poptotals, TotalVar, MaxRuleLength, 
                         ModelGroup, Output, GWeightVar, CalWeightVar, GESBoundType, 
                         GESUpperBound, GESLowerBound, GESMaxDiff, GESMaxIter, GESMaxDist):
    """
    Author       : Thomas Mahoney
    Date         : 06 / 03 / 2018
    Purpose      : Calculates GES single stage, element weighting. This is used in the IPS
                   traffic and OOH weights.
    Parameters   : df_input - the IPS survey records for the period
                   SerialNumVarName - variable holding the record serial number (UID)                
                   DesignWeightVarName - Variable holding the design weights                            
                   StrataDef - List of classificatory variables          
                   df_poptotals - Population totals file               
                   TotalVar - Variable that holds the population totals 
                   MaxRuleLength - maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup - Variable that will hold the model group number                
                   Output - output file (holds weights)                                    
                   GWeightVar - Variable that will hold the output weights                    
                   CalWeightVar - Variable that will hold the calibration weights            
                   GESBoundType - GES parameter                         
                   GESUpperBound - GES parameter : upper bound for weights (can be null)    
                   GESLowerBound - GES parameter : lower bound for weights (can be null)
                   GESMaxDiff - GES parameter : maximum difference (e.g. 1E-8)
                   GESMaxIter - GES parameter : maximum number of iterations (e.g. 50)
                   GESMaxDist - GES parameter : maximum distance (e.g. 1E-8)
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    # Create ges_input data set from the relevant input data columns
    df_ges_input = df_input[StrataDef + [DesignWeightVarName] + ['SERIAL']]
    df_ges_input[ModelGroup] = 1
    df_ges_input = df_ges_input.rename(columns = {'SERIAL' : 'GES_SERIAL'})
    
    # Only include records where the 'DesignWeightVarName' value is above zero
    df_ges_input = df_ges_input[df_ges_input[DesignWeightVarName] > 0]
        
    # Check the ges totals
    ips_check_ges_totals(df_ges_input, df_poptotals, StrataDef, DesignWeightVarName, TotalVar)
    
    # Setup the ges auxiliary variables 
    df_cvset, df_moddef, auxvar, totvar, NumAuxVars, AuxNumForm = \
            ips_setup_ges_auxvars(df_poptotals, StrataDef, 'AUXVAR', 'TOTVAR', 'Y_', 'T_');


    sys.exit()
    pass
