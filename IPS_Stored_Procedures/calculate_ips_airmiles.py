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
    
    sas_root = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Air Miles"
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
    print("") 


# def do_ips_unsampled_weight_calculation(df_surveydata, summary, var_serialNum, var_shiftWeight, var_NRWeight,
#                                     var_minWeight, var_trafficWeight, OOHStrataDef, df_ustotals, 
#                                     var_totals, MaxRuleLength, var_modelGroup, output, 
#                                     var_OOHWeight, var_caseCount, var_priorWeightSum, 
#                                     var_OOHWeightSum, GESBoundType, GESUpperBound, GESLowerBound, 
#                                     GESMaxDiff, GESMaxIter, GESMaxDist, minCountThresh):  
#     """
#     Author       : Thomas Mahoney
#     Date         : 16 / 02 / 2018
#     Purpose      : Performs calculations to determine the unsampled weight values
#                    of the imported dataset.
#     Parameters   : df_surveydata - the IPS df_surveydata records for the period                                
#                    summary - Oracle table to hold the summary data                            
#                    var_serialNum - variable holding the record serial number (UID)            
#                    var_shiftWeight - variable holding the shift weight field name                
#                    var_NRWeight - variable holding the non-response weight field name        
#                    var_minWeight - variable holding the minimum weight field name            
#                    var_trafficWeight - variable holding the traffic weight field name        
#                    OOHStrataDef - List of classificatory variables                            
#                    df_ustotals - Population totals file                                         
#                    var_totals - Variable that holds the population totals                    
#                    MaxRuleLength - maximum length of an auxiliary rule (e.g. 512)            
#                    var_modelGroup - Variable that will hold the model group number            
#                    output - output dataset                                                 
#                    var_OOHWeight - Variable that will hold the OOH weights                    
#                    var_caseCount -  Variable holding the name of the case count field        
#                    var_priorWeightSum - Variable that will hold the prior weight summary    
#                    var_OOHWeightSum - Variable that will hold the post weight sum            
#                    GESBoundType - GES parameter : 'G' => cal. weights bound, 'F' => final  
#                                                    weights bound                            
#                    GESUpperBound - GES parameter : upper bound for weights (can be null)    
#                    GESLowerBound - GES parameter : lower bound for weights (can be null)    
#                    GESMaxDiff - GES parameter : maximum difference (e.g. 1E-8)                
#                    GESMaxIter - GES parameter : maximum number of iterations (e.g. 50)        
#                    GESMaxDist - GES parameter : maximum distance (e.g. 1E-8)                
#                    minCountThresh - The minimum cell count threshold                        
#     Returns      : df_summary(dataframe containing random sample of rows)
#                    df_output(dataframe containing serial number and calculated unsampled weight)
#     Requirements : NA
#     Dependencies : NA
#     
#     
#     NOTES        : Currently GES weighing has not been written. Therefore the current solution
#                    does not generate the output data frame. Once the function is written and we
#                    are aware of what is being returned from the GES weighting function as well
#                    as what is actually needed to be *sent* passed to the function we will rewrite the 
#                    function call and implement its return functionality 
#                    be rewriting the 
#     """
# 
#     # Create new column for design weights (Generate the design weights)
#     OOHDesignWeight = 'OOHDESIGNWEIGHT'
#     df_surveydata[OOHDesignWeight] = \
#         df_surveydata[var_shiftWeight] * df_surveydata[var_NRWeight] * df_surveydata[var_minWeight] * df_surveydata[var_trafficWeight];
#     
#     
#     # Sort the unsampled data frame ready to be summarised
#     df_ustotals = df_ustotals.sort_values(by = OOHStrataDef)    
#     
#     # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
#     popTotals = df_ustotals.fillna('NOTHING')
#     
#     # Summarise the uplift totals over the strata
#     popTotals = popTotals.groupby(OOHStrataDef)[var_totals].agg({'UPLIFT' : 'sum'})
#     popTotals.reset_index(inplace = True)
#     
#     # Replace the previously added 'NOTHING' values with their original blank values  
#     popTotals = popTotals.replace('NOTHING', np.NaN)
#         
#     # Summarise the previous totals over the strata
#     # Only use values where the OODesignWeight is greater than zero
#     df_surveydata = df_surveydata.sort_values(by = OOHStrataDef)
#     
#     prevTotals = df_surveydata.loc[df_surveydata[OOHDesignWeight] > 0]
#     
#     # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
#     prevTotals = prevTotals.fillna('NOTHING')
#     prevTotals = prevTotals.groupby(OOHStrataDef)[OOHDesignWeight].agg({'PREVTOTAL' : 'sum'}) 
#     prevTotals.reset_index(inplace = True)
#     
#     # Replace the previously added 'NOTHING' values with their original blank values  
#     prevTotals = prevTotals.replace('NOTHING', np.NaN)
#     
#         
#     popTotals = popTotals.sort_values(by = OOHStrataDef)
#         
#     # Generate the lifted totals data set from the two sets created
#     liftedTotals = pd.merge(prevTotals,popTotals,on = OOHStrataDef, how = 'left')
#     
#     # Fill blank uplift and prevtotal values with zero
#     liftedTotals['UPLIFT'].fillna(0, inplace=True)
#     liftedTotals['PREVTOTAL'].fillna(0, inplace=True)
#     
#     # Calculate the totals column from the prevtotal and uplift values
#     liftedTotals[var_totals] = liftedTotals['PREVTOTAL'] + liftedTotals['UPLIFT']
#     
#     # Remove any records where var_totals value is not greater than zero
#     liftedTotals = liftedTotals[liftedTotals[var_totals] > 0]
#             
#     # Call the GES weighting macro
#     ges_dataframes = do_ips_ges_weighting(df_surveydata, var_serialNum, OOHDesignWeight, 
#                                           OOHStrataDef, liftedTotals, var_totals, MaxRuleLength, 
#                                           var_modelGroup, output, var_OOHWeight, 'CalWeight', GESBoundType, 
#                                           GESUpperBound, GESLowerBound, GESMaxDiff, GESMaxIter, GESMaxDist)
#     
#     df_survey = ges_dataframes[0]
#     df_output = ges_dataframes[1]
#     # Sort df_surveydata dataframe before merge
#     df_survey = df_survey.sort_values(by = var_serialNum)
#     df_output = df_output.sort_values(by = var_serialNum)
# 
# 
#     # Merge the df_surveydata and output data frame to generate the summary table
#     df_survey[var_OOHWeight] = df_output[var_OOHWeight]
# 
# 
#     # Fill blank UNSAMP_TRAFFIC_WT values with 1.0
#     df_survey[var_OOHWeight].fillna(1.0, inplace=True)
#         
#     # Generate POSTWEIGHT values from the UNSAMP_TRAFFIC_WT and OOHDesignWeight values
#     df_survey['POSTWEIGHT'] = df_survey[var_OOHWeight] * df_survey[OOHDesignWeight]
#     
#     # Sort the data ready for summarising    
#     df_survey = df_survey.sort_values(by = OOHStrataDef)
#     
#     # Create the summary data frame from the sample with OOHDesignWeight not equal to zero
#     df_summary = df_survey[df_survey[OOHDesignWeight] != 0]
#     
#     # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.  
#     df_summary = df_summary.fillna('NOTHING')
#     
#     # Generate a dataframe containing the count of each evaluated group
#     df_cases = df_summary.groupby(OOHStrataDef)[var_OOHWeight].agg({
#             'CASES' : 'count'
#     })
#     
#     
#     # Flattens the column structure after adding the new 'CASES' column
#     df_cases = df_cases.reset_index()
#     
#     # Summarise the data across the OOHStrataDef
#     df_summary = df_summary.groupby(OOHStrataDef).agg({\
#             OOHDesignWeight : 'sum' ,
#             'POSTWEIGHT' : 'sum',
#             var_OOHWeight : 'mean'
#     })
# 
#     # Flattens the column structure after adding the new OOHDesignWeight and POSTWEIGHT columns
#     df_summary = df_summary.reset_index()
#     df_summary = df_summary.rename(columns = {OOHDesignWeight : var_priorWeightSum,
#                                               'POSTWEIGHT' : var_OOHWeightSum})
#     
#     # Merge the cases dataframe into our summary dataframe
#     df_summary = pd.merge(df_summary,df_cases,on = OOHStrataDef, how = 'right')
#     
#     # Replace the previously added 'NOTHING' values with their original blank values  
#     df_summary = df_summary.replace('NOTHING', np.NaN)
#     
#     
#     # Identify groups where the total has been uplifted but the
#     # respondent count is below the threshold.
#     
#     # Create unsampled data set for rows outside of the threshold
#     df_unsampled_thresholds_check = \
#         df_summary[(df_summary[var_OOHWeightSum] > df_summary[var_priorWeightSum]) \
#                    & (df_summary[var_caseCount] < minCountThresh)]
#     
#     # Collect data outside of specified threshold
#     threshold_string = ""
#     for index, record in df_unsampled_thresholds_check.iterrows():
#         threshold_string += "___||___" \
#                          + str(df_unsampled_thresholds_check.columns[0]) + " : " + str(record[0]) + " | "\
#                          + str(df_unsampled_thresholds_check.columns[1]) + " : " + str(record[1]) + " | "\
#                          + str(df_unsampled_thresholds_check.columns[2]) + " : " + str(record[2]) + " | "\
#                          + str(df_unsampled_thresholds_check.columns[3]) + " : " + str(record[3])
#                          
#     # Output the values outside of the threshold to the logger - COMMENTED OUT DUE TO SIZE ISSUE?
#     if len(df_unsampled_thresholds_check) > 0:
#         cf.database_logger().warning('WARNING: Respondent count below minimum threshold for: ')# + str(threshold_string))
#     
#     
#     # Return the generated data frames to be appended to oracle
#     return (df_output, df_summary)


def calculate(inputtablename, outputtablename, responsetable, key_id, dist1, 
              dist2, dist3):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 02 / 2018
    Purpose      : Imports the required data sets for performing the unsampled
                   weight calculation. This function also triggers the unsmapled
                   weight calculation function using the imported data. Once 
                   complete it will append the newly generated data frames to the 
                   specified oracle database tables. 
    Parameters   : SurveyData - the IPS df_surveydata records for the period                             
                   SummaryData - the weighting summary output table                        
                   responseTable - Oracle table to hold response information (status etc.)    
                   var_serialNum - variable holding the record serial number (UID)            
                   var_shiftWeight - variable holding the shift weight field name                
                   var_NRWeight - variable holding the non-response weight field name        
                   var_minWeight - variable holding the minimum weight field name            
                   var_trafficWeight - variable holding the traffic weight field name        
                   var_designWeight - Variable holding the design weight field name         
                   OOHStrataDef - List of classificatory variables                            
                   PopTotals - OOH population totals file                                    
                   var_totals - Variable that holds the population totals                    
                   MaxRuleLength - maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup - Model definition file                                    
                   var_modelGroup - Variable that will hold the model group number            
                   OutputData - output file (holds weights)                                
                   var_OOHWeight - Variable that will hold the OOH weight                     
                   var_caseCount - Variable that will hold the number of cases by strata     
                   var_priorWeightSum - Variable that will hold the prior weight summary    
                   var_OOHWeightSum - Variable that will hold the post weight sum            
                   GESBoundType - GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                    weights bound                            
                   GESUpperBound - GES parameter : upper bound for weights (can be null)    
                   GESLowerBound - GES parameter : lower bound for weights (can be null)    
                   GESMaxDiff - GES parameter : maximum difference (e.g. 1E-8)                
                   GESMaxIter - GES parameter : maximum number of iterations (e.g. 50)        
                   GESMaxDist - GES parameter : maximum distance (e.g. 1E-8)                
                   minCountThresh - The minimum cell count threshold                        
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    
    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')
    
    #global df_surveydata
    #global df_ustotals  

    
    # Import data via SAS
    path_to_survey_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Air Miles\airmiles.sas7bdat"
    df_surveydata = pd.read_sas(path_to_survey_data)
    
    # Import data via SQL
    #df_surveydata = cf.get_table_values(inputtablename)
    
    # Set all of the columns imported to uppercase
    df_surveydata.columns = df_surveydata.columns.str.upper()
    
    
    airmiles_columns = [key_id, 
                        'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC', 'APORTLATNS', 
                        'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC', 'APORTLONEW',
                        'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC', 'CPORTLATNS',
                        'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC', 'CPORTLONEW',
                        'FLOW',
                        'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC', 'PROUTELATNS',
                        'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC', 'PROUTELONEW']
    
    # Select rows from the imported data that have the correct 'FLOW' value
    df_airmiles = df_surveydata[df_surveydata['FLOW'].isin((1,2,3,4))]

    # Extract the airmiles columns from the imported data
    df_airmiles = df_airmiles[airmiles_columns]

    
    
    # Setup air extract 1 data
    
    # Create data frame from the specified column sets
    air_1_columns = [key_id,
                     'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC', 
                     'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC', 
                     'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC', 
                     'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC', 
                     'PROUTELATNS', 'PROUTELONEW',
                     'APORTLATNS', 'APORTLONEW']
    
    df_air_ext1 =  df_airmiles[air_1_columns]
    
    # Rename the dataframe's columns
    df_air_ext1 = df_air_ext1.rename(columns = {'PROUTELATDEG':'START_LAT_DEGREE', 
                                                'PROUTELATMIN':'START_LAT_MIN', 
                                                'PROUTELATSEC':'START_LAT_SEC', 
                                                'PROUTELONDEG':'START_LON_DEGREE', 
                                                'PROUTELONMIN':'START_LON_MIN', 
                                                'PROUTELONSEC':'START_LON_SEC', 
                                                'APORTLATDEG':'END_LAT_DEGREE', 
                                                'APORTLATMIN':'END_LAT_MIN', 
                                                'APORTLATSEC':'END_LAT_SEC', 
                                                'APORTLONDEG':'END_LON_DEGREE', 
                                                'APORTLONMIN':'END_LON_MIN', 
                                                'APORTLONSEC':'END_LON_SEC', 
                                                'PROUTELATNS':'START_LAT_DIR',
                                                'APORTLATNS':'END_LAT_DIR',
                                                'PROUTELONEW':'START_LON_DIR',
                                                'APORTLONEW':'END_LON_DIR'})
    
    
    # Setup air extract 2 data
    
    # Create data frame from the specified column sets
    air_2_columns = [key_id,
                     'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC', 
                     'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC',
                     'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC', 
                     'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC', 
                     'CPORTLATNS', 'CPORTLONEW',
                     'APORTLATNS', 'APORTLONEW']    

    df_air_ext2 =  df_airmiles[air_2_columns]
    
    # Rename the dataframe's columns
    df_air_ext2 = df_air_ext2.rename(columns = {'CPORTLATDEG':'START_LAT_DEGREE', 
                                                'CPORTLATMIN':'START_LAT_MIN', 
                                                'CPORTLATSEC':'START_LAT_SEC', 
                                                'CPORTLONDEG':'START_LON_DEGREE', 
                                                'CPORTLONMIN':'START_LON_MIN', 
                                                'CPORTLONSEC':'START_LON_SEC', 
                                                'APORTLATDEG':'END_LAT_DEGREE', 
                                                'APORTLATMIN':'END_LAT_MIN', 
                                                'APORTLATSEC':'END_LAT_SEC', 
                                                'APORTLONDEG':'END_LON_DEGREE', 
                                                'APORTLONMIN':'END_LON_MIN', 
                                                'APORTLONSEC':'END_LON_SEC', 
                                                'CPORTLATNS':'START_LAT_DIR',
                                                'APORTLATNS':'END_LAT_DIR',
                                                'CPORTLONEW':'START_LON_DIR',
                                                'APORTLONEW':'END_LON_DIR'})
    
    
    # Setup air extract 3 data

    # Create data frame from the specified column sets
    air_3_columns = [key_id,
                     'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC', 
                     'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC', 
                     'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC', 
                     'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC', 
                     'PROUTELATNS', 'PROUTELONEW',
                     'CPORTLATNS', 'CPORTLONEW']
    
    df_air_ext3 =  df_airmiles[air_3_columns]

    # Rename the dataframe's columns
    df_air_ext3 = df_air_ext3.rename(columns = {'PROUTELATDEG':'START_LAT_DEGREE', 
                                                'PROUTELATMIN':'START_LAT_MIN', 
                                                'PROUTELATSEC':'START_LAT_SEC', 
                                                'PROUTELONDEG':'START_LON_DEGREE', 
                                                'PROUTELONMIN':'START_LON_MIN', 
                                                'PROUTELONSEC':'START_LON_SEC', 
                                                'CPORTLATDEG':'END_LAT_DEGREE', 
                                                'CPORTLATMIN':'END_LAT_MIN', 
                                                'CPORTLATSEC':'END_LAT_SEC', 
                                                'CPORTLONDEG':'END_LON_DEGREE', 
                                                'CPORTLONMIN':'END_LON_MIN', 
                                                'CPORTLONSEC':'END_LON_SEC', 
                                                'PROUTELATNS':'START_LAT_DIR',
                                                'CPORTLATNS':'END_LAT_DIR',
                                                'PROUTELONEW':'START_LON_DIR',
                                                'CPORTLONEW':'END_LON_DIR'})
    
    # Calculate the airmiles of all the datasets created
    
    def airmiles (row):
        """
        This will actually calculate the airmiles for each dataset passed to it
        """
        return row
    
    air = airmiles(df_air_ext1);
    air2 = airmiles(df_air_ext1);
    air3 = airmiles(df_air_ext1);
    

    df_airmiles1 = air[[key_id,'AIRMILES']]
    df_airmiles1 = df_airmiles1.rename(columns = {'AIRMILES':dist1})
    
    df_airmiles2 = air[[key_id,'AIRMILES']]
    df_airmiles2 = df_airmiles2.rename(columns = {'AIRMILES':dist2})
    
    df_airmiles3 = air[[key_id,'AIRMILES']]
    df_airmiles3 = df_airmiles3.rename(columns = {'AIRMILES':dist3})

    # Merge the airmiles datasets
    
    sys.exit()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    compare_dfs('Air_Ext3', 'air_ext3.sas7bdat', df_air_ext3)
    
    sys.exit()

    # Calculate the unsampled weights of the imported dataset.
    print("Start - Calculate UnSampled Weight.")     
#     weight_calculated_dataframes = do_ips_unsampled_weight_calculation(df_surveydata, 'summary', var_serialNum, var_shiftWeight, var_NRWeight, 
#                                         var_minWeight, var_trafficWeight, OOHStrataDef, df_ustotals, 
#                                         var_totals, MaxRuleLength, var_modelGroup, 'output', 
#                                         var_OOHWeight, var_caseCount, var_priorWeightSum, 
#                                         var_OOHWeightSum, GESBoundType, GESUpperBound, GESLowerBound, 
#                                         GESMaxDiff, GESMaxIter, GESMaxDist, minCountThresh)
    weight_calculated_dataframes = []
    # Extract the two data sets returned from do_ips_nrweight_calculation
    output_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]
    
    
    sys.exit()
    
    # Ensure the 'UNSAMP_REGION_GRP_PV' values are read in as strings before 
    # appending to the database
    def num_to_string(row):
        row['UNSAMP_REGION_GRP_PV'] = str(row['UNSAMP_REGION_GRP_PV'])
        if(row['UNSAMP_REGION_GRP_PV'] == 'nan'):
            row['UNSAMP_REGION_GRP_PV'] = ' '
        return row
    
    summary_dataframe = summary_dataframe.apply(num_to_string, axis = 1)
    
    # Append the generated data to output tables
    cf.insert_into_table_many(outputtablename, output_dataframe)
    cf.insert_into_table_many(inputtablename, summary_dataframe)
     
    # Retrieve current function name using inspect:
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    audit_message = "Load UnSampled Weight calculation: %s()" % function_name
     
    # Log success message in SAS_RESPONSE and AUDIT_LOG
    cf.database_logger().info("SUCCESS - Completed UnSampled weight calculation.")
    cf.commit_to_audit_log("Create", "UnSampled", audit_message)
    print("Completed - Calculate UnSampled Weight")
    

if __name__ == '__main__':
    calculate(inputtablename = 'SAS_SURVEY_SUBSAMPLE', 
              outputtablename = 'sas_air_miles', 
              responsetable = 'SAS_RESPONSE', 
              key_id = 'SERIAL', 
              dist1 = 'UKLEG', 
              dist2 = 'OVLEG', 
              dist3 = 'DIRECTLEG')
    
    
    