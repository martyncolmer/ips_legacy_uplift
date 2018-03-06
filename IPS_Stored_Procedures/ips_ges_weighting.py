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
    
    # Sort the sample data by strata
    SampleData = SampleData.sort_values(by = StrataDef)
    
    rsumsamp = SampleData.fillna('NOTHING')
    
    # Summarise the OOHDesignWeight over the strata
    rsumsamp = rsumsamp.groupby(StrataDef)[var_sample].agg({var_sample : 'sum'})
    rsumsamp.reset_index(inplace = True)
    
    rsumsamp = rsumsamp.replace('NOTHING', np.NaN)


    # Sort the population totals by strata
    SummarisedPopulation = SummarisedPopulation.sort_values(by = StrataDef)
    
    # Merge the two and identify any areas that can't be aligned
    
    
    sys.exit()
    

def do_ips_ges_weighting(input, SerialNumVarName, DesignWeightVarName, 
                         StrataDef, PopTotals, TotalVar, MaxRuleLength, 
                         ModelGroup, Output, GWeightVar, CalWeightVar, GESBoundType, 
                         GESUpperBound, GESLowerBound, GESMaxDiff, GESMaxIter, GESMaxDist):


    # Create ges_input data set from the relevant input data columns
    df_ges_input = input[StrataDef + [DesignWeightVarName] + ['SERIAL']]
    df_ges_input[ModelGroup] = 1
    df_ges_input = df_ges_input.rename(columns = {'SERIAL' : 'GES_SERIAL'})
    
    # Only include records where the 'DesignWeightVarName' value is above zero
    df_ges_input = df_ges_input[df_ges_input[DesignWeightVarName] >0]
    
    compare_dfs("ges_input", 'GESInput_1.sas7bdat', df_ges_input)
    
    # Check Ges Totals
    ips_check_ges_totals(df_ges_input, PopTotals, StrataDef, DesignWeightVarName, TotalVar)
    
    sys.exit()
    pass
