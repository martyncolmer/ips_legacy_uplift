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
    

def do_ips_ges_weighting(input, SerialNumVarName, DesignWeightVarName, 
                         StrataDef, PopTotals, TotalVar, MaxRuleLength, 
                         ModelGroup, Output, GWeightVar, CalWeightVar, GESBoundType, 
                         GESUpperBound, GESLowerBound, GESMaxDiff, GESMaxIter, GESMaxDist):
    """
    Author       : Thomas Mahoney
    Date         : 06 / 03 / 2018
    Purpose      : Calculates GES single stage, element weighting. This is used in the IPS
                   traffic and OOH weights.
    Parameters   : Survey - the IPS survey records for the period
                   SerialNumVarName - variable holding the record serial number (UID)                
                   DesignWeightVarName - Variable holding the design weights                            
                   StrataDef - List of classificatory variables          
                   PopTotals - Population totals file               
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
    df_ges_input = input[StrataDef + [DesignWeightVarName] + ['SERIAL']]
    df_ges_input[ModelGroup] = 1
    df_ges_input = df_ges_input.rename(columns = {'SERIAL' : 'GES_SERIAL'})
    
    # Only include records where the 'DesignWeightVarName' value is above zero
    df_ges_input = df_ges_input[df_ges_input[DesignWeightVarName] > 0]
        
    # Calls the check_ges_totals function
    ips_check_ges_totals(df_ges_input, PopTotals, StrataDef, DesignWeightVarName, TotalVar)
    
    sys.exit()
    pass
