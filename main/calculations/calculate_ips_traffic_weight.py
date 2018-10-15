import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from main.io import CommonFunctions as cf

PATH_TO_DATA = r"tests/data/calculations/october_2017/traffic_weight"

OUTPUT_TABLE_NAME = 'SAS_TRAFFIC_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_TRAFFIC' 
STRATA = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
MAX_RULE_LENGTH = '512'
MODEL_GROUP = 'C_group'
GES_BOUND_TYPE = 'G'
GES_UPPER_BOUND = ''
GES_LOWER_BOUND = '1.0'
GES_MAX_DIFFERENCE = '1E-8'
GES_MAX_ITERATIONS = '50'
GES_MAX_DISTANCE = '1E-8'
COUNT_COLUMN = 'CASES'
TRAFFIC_TOTAL_COLUMN = 'TRAFFICTOTAL'
POST_SUM_COLUMN = 'SUM_TRAFFIC_WT'.upper()
TRAFFIC_DESIGN_WEIGHT_COLUMN = 'TRAFDESIGNWEIGHT'
POST_WEIGHT_COLUMN = 'POSTWEIGHT'

SURVEY_TRAFFIC_AUX_TABLE = "[dbo].[survey_traffic_aux]"
POP_PROWVEC_TABLE = "[dbo].[poprowvec_traffic]"

def convert_dataframe_to_sql_format(table_name, dataframe):
    cf.insert_dataframe_into_table(table_name, dataframe)
    return cf.get_table_values(table_name)

def calculate(SurveyData, var_serialNum, var_shiftWeight, var_NRWeight, var_minWeight, PopTotals,
              GWeightVar, minCountThresh):
    """
    Author       : Nassir Mohammad
    Date         : April 2018
    Purpose      : Calculates the IPS traffic weight using GES
    Parameters   : SurveyData = the IPS survey records for the period                        
                   summaryData = Oracle table to hold the summary data                        
                   responseTable = Oracle table to hold response information (status etc.)    
                   var_serialNum = variable holding the record serial number (UID)            
                   var_shiftWeight     = variable holding the shift weight field name             
                   var_NRWeight = variable holding the non-response weight field name        
                   var_minWeight = variable holding the minimum weight field name
                   PopTotals = Population totals file
                   minCountThresh = The minimum cell count threshold
    Returns      : tuple of dataframes - (df_output_merge_final, df_summary_merge_sum_traftot)
    Requirements : TODO
    Dependencies : do_ips_trafweight_calculation(), 
    """

    # Call JSON configuration file for error logger setup
    # survey_support.setup_logging('IPS_logging_config_debug.json')

    # following code only required when connecting to Oracle database
    # Connect to Oracle and unload parameter list
    # conn = cf.get_sql_connection()
    # global parameters
    # parameters = cf.unload_parameters(205)

    # Load SAS files into dataframes (this data will come from Oracle eventually)
    path_to_SurveyData = PATH_TO_DATA + r"/survey_input.pkl"
    path_to_PopTotals = PATH_TO_DATA + r"/trtotals.pkl"

    df_survey = pd.read_pickle(path_to_SurveyData)
    df_trtotals = pd.read_pickle(path_to_PopTotals)

    # *********************  remove in future **********************
    # path_to_OutputData = root_data_path + r"\surveydata_1.sas7bdat" # not used
    # path_to_SummaryData = root_data_path + r"\surveydata_1.sas7bdat" # not used

    # ##########################################
    #
    # create a SAS dataset from the survey data
    #
    # ##########################################

    # Import data via SAS
    # df_survey = SAS7BDAT(path_to_SurveyData).to_data_frame()
    # df_survey = pd.read_sas(path_to_SurveyData)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)

    # upper case all columns 
    df_survey.columns = df_survey.columns.str.upper()

    # ##############################################
    #
    # Create SAS dataset from the population totals
    #
    # ##############################################

    # Import data via SAS
    # df_trtotals = SAS7BDAT(path_to_PopTotals).to_data_frame()
    # df_trtotals = pd.read_sas(path_to_PopTotals)

    # Import data via SQL
    # df_trtotals = cf.get_table_values(PopTotals)

    # upper case all column names as column names are case sensitive
    df_trtotals.columns = df_trtotals.columns.str.upper()

    (df_output_merge_final_rounded, df_summary_merge_sum_traftot) = do_ips_trafweight_calculation(df_survey,
                                                                                                  var_serialNum,
                                                                                                  var_shiftWeight,
                                                                                                  var_NRWeight,
                                                                                                  var_minWeight,
                                                                                                  df_trtotals,
                                                                                                  GWeightVar,
                                                                                                  minCountThresh)

    # TODO - remove following code when refactoring main()
    # test code for deleting table data before insertion
    # cf.delete_from_table('sas_traffic_wt'.upper())
    # cf.delete_from_table('sas_ps_traffic'.upper())

    # append the weights to the output table
    # cf.insert_into_table_many(OutputData, df_output_merge_final)

    # append summary information to summary table
    # cf.insert_into_table_many(SummaryData, df_summary_merge_sum_traftot)

    return df_output_merge_final_rounded, df_summary_merge_sum_traftot


def do_ips_trafweight_calculation(df_survey, var_serialNum, var_shiftWeight, var_NRWeight,
                                  var_minWeight, PopTotals, GWeightVar, minCountThresh):
    """
    Author       : Nassir Mohammad
    Date         : April 2018
    Purpose      : Calculates the IPS traffic weight using GES
    Parameters   : in = the IPS survey records for the period                                
                   summary = Oracle table to hold the summary data                            
                   var_serialNum = variable holding the record serial number (UID)            
                   var_shiftWeight     = variable holding the shift weight field name             
                   var_NRWeight = variable holding the non-response weight field name        
                   var_minWeight = variable holding the minimum weight field name            
                   StrataDef = List of classificatory variables                            
                   PopTotals = Population totals file                                         
                   TotalVar = Variable that holds the population totals                    
                   MaxRuleLength = maximum length of an auxiliary rule (e.g. 512)            
                   ModelGroup = Variable that will hold the model group number                
                   output = output dataset                                                 
                   GWeightVar = Variable that will hold the traffic weights                
                   GESBoundType = GES parameter : 'G' => cal. weights bound, 'F' => final  
                                                   weights bound                            
                   GESUpperBound = GES parameter : upper bound for weights (can be null)    
                   GESLowerBound = GES parameter : lower bound for weights (can be null)    
                   ESMaxDiff = GES parameter - maximum difference (e.g. 1E-8)                
                   GESMaxIter = GES parameter - maximum number of iterations (e.g. 50)        
                   GESMaxDist = GES parameter - maximum distance (e.g. 1E-8)                
                   var_count =  Variable holding the name of the case count field            
                   var_trafficTotal = Variable holding the name of the traffic total output
                   var_postSum = Variable holding the name of the post traffic weight sum     
                   minCountThresh = The minimum cell count threshold
    Returns      : a dataframe tuple: (df_output_merge_final, df_summary_merge_sum_traftot)
    Dependencies : do_ips_ges_weighting(), generate_ips_tw_summary
    """

    # perform calculation
    df_survey[TRAFFIC_DESIGN_WEIGHT_COLUMN] = df_survey[var_shiftWeight] * df_survey[var_NRWeight] * df_survey[var_minWeight]

    # test code start - turn on when testing/refactoring intermediate steps
    # df_test = pd.read_pickle(path_to_data + r"/in_1.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_survey, df_test, check_dtype=False)
    # test code end

    # Summarise the population totals over the strata
    df_PopTotals = PopTotals.sort_values(STRATA)

    # Re-index the data frame
    df_PopTotals.index = range(df_PopTotals.shape[0])

    # test code start - dataframes not equal due to nan v empty - turn on when testing/refactoring intermediate steps
    # df_test = pd.read_pickle(path_to_data + r"\trtotals_stratadef_sort.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_PopTotals, df_test, check_dtype=False)
    # test code end

    df_popTotals = df_PopTotals.groupby(STRATA)[TRAFFIC_TOTAL_COLUMN] \
        .agg([(TRAFFIC_TOTAL_COLUMN, 'sum')]) \
        .reset_index()

    # test code start - turn on when testing/refactoring intermediate steps
    # df_test = pd.read_pickle(path_to_data + r"/poptotals_summary_1.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_popTotals, df_test, check_column_type=False)
    # test code end    

    # Call the GES weighting macro        
    CalWeight = None  # this is passed in by SAS, but probably should not be in future code

    (df_output_merge_final, df_survey_serialNum_sort) = do_ips_ges_weighting(df_survey, var_serialNum,
                                                                             df_popTotals, GWeightVar, CalWeight)
    # test start - turn on when testing/refactoring intermediate steps
    # df_test = pd.read_pickle(path_to_data + r"\output_merge_final.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_output_merge_final, df_test)
    #
    # df_test = pd.read_pickle(path_to_data + r"\survey_serialNum_sort.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_survey_serialNum_sort, df_test)
    # test end

    # Generate the summary table    
    df_summary_merge_sum_traftot = generate_ips_tw_summary(df_survey, df_output_merge_final,
                                                           var_serialNum, GWeightVar,
                                                           df_popTotals, minCountThresh)

    # test start - turn on when testing/refactoring intermediate steps
    # df_test = pd.read_pickle(path_to_data + r"\summary_merge_sum_traftot.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_summary_merge_sum_traftot, df_test)
    # test end

    # Round the weights to 3dp    
    df_output_merge_final[GWeightVar] = df_output_merge_final[GWeightVar].apply(lambda x: round(x, 3))

    df_output_merge_final_rounded = df_output_merge_final

    # test start - turn on when testing/refactoring intermediate steps
    # df_test = pd.read_pickle(path_to_data + r"\output_rounded.pkl")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_output_merge_final_rounded, df_test)
    # test end

    return df_output_merge_final_rounded, df_summary_merge_sum_traftot


def do_ips_ges_weighting(df_survey, var_serialNum, df_popTotals, GWeightVar, CalWeight):
    """
    Author       : Nassir Mohammad
    Date         : 08 Mar 2018
    Purpose      : Calculates GES single stage, element weighting. This is used in the IPS
                   traffic and OOH weights.
    Parameters   : Survey = the IPS survey records for the period                            
                   SerialNumVarName = variable holding the record serial number (UID)
                   PopTotals = Population totals file
                   GWeightVar = Variable that will hold the output weights                    
                   CalWeightVar = Variable that will hold the calibration weights
    Returns      : (df_output_merge_final, df_survey_serialNum_sort)
    Requirements : TODO
    Dependencies : ips_check_ges_totals(), ips_setup_ges_auxvars(), ips_assign_ges_auxiliaries(),ips_get_population_totals()
    """

    # for now we read the output dataset from disk and return the dataframe result
    #df_output_merge_final = pd.read_pickle(PATH_TO_DATA + r"\output_merge_final.pkl")
    df_output_merge_final = pd.read_pickle(PATH_TO_DATA+"/output_merge_final.pkl")
    df_output_merge_final.columns = df_output_merge_final.columns.str.upper()

    #df_survey_serialNum_sort = pd.read_pickle(PATH_TO_DATA + r"\survey_serialnum_sort.pkl")
    df_survey_serialNum_sort = pd.read_pickle(PATH_TO_DATA+"/survey_serialnum_sort.pkl")
    df_survey_serialNum_sort.columns = df_survey_serialNum_sort.columns.str.upper()

    return (df_output_merge_final, df_survey_serialNum_sort)

# Prepare survey data
def r_survey_input(survey_data):
    """
    Author       : David Powell / edits by Nassir Mohammad
    Date         : 07/06/2018
    Purpose      : Creates input data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Load SAS files into dataframes (this data will come from Oracle eventually)
    # path_to_SurveyData = PATH_TO_DATA + r"/survey_input.pkl"

    # df_survey_input = pd.read_pickle(path_to_SurveyData)

    df_survey_input = survey_data

    # Sort input values
    sort1 = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['SAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['SAMP_PORT_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Merge lookup data in to source dataframe
    df_aux_variables = pd.merge(df_survey_input_sorted, lookup_dataframe, on=['SAMP_PORT_GRP_PV',
                                                                              'ARRIVEDEPART'], how='left')

    # Create traffic design weight used within GES weighting
    values = df_aux_variables.SHIFT_WT * df_aux_variables.NON_RESPONSE_WT * df_aux_variables.MINS_WT
    df_aux_variables['trafDesignWeight'] = values
    df_aux_variables = df_aux_variables.sort_values(['SERIAL'])

    # Create input to pass into GES weighting
    df_r_ges_input = df_aux_variables[~df_aux_variables['T1'].isnull()]
    df_r_ges_input['SERIAL'] = df_r_ges_input.SERIAL.astype(np.float64)
    df_r_ges_input = df_r_ges_input[['SERIAL', 'ARRIVEDEPART', 'PORTROUTE', 'SAMP_PORT_GRP_PV', 'SHIFT_WT',
                                     'NON_RESPONSE_WT', 'MINS_WT', 'trafDesignWeight', 'T1']]

    # Export dataframes to CSV - for testing purposes: TODO: delete
    #df_r_ges_input.to_csv(r"tests/data/r_setup/October_2017/unsampled_weight/df_r_ges_input_traffic.csv", index=False)
    #cf.insert_dataframe_into_table(SURVEY_TRAFFIC_AUX_TABLE, df_r_ges_input)

    df_r_ges_input_imported = convert_dataframe_to_sql_format(SURVEY_TRAFFIC_AUX_TABLE, df_r_ges_input)

    return df_r_ges_input_imported

# Prepare population totals to create AUX lookup variables
def r_population_input(survey_input, trtotals):
    """
    Author       : David Powell / edits by Nassir Mohammad
    Date         : 07/06/2018
    Purpose      : Creates population data that feeds into the R GES weighting
    Parameters   : df_survey_input - A data frame containing the survey data for
                   processing month
                   trtotals - A data frame containing population information for
                   processing year
    Returns      : A data frame containing the information needed for GES weighting
    Requirements : NA
    Dependencies : NA
    """

    # Load SAS files into dataframes (this data will come from Oracle eventually)
    # path_to_SurveyData = PATH_TO_DATA + r"/survey_input.pkl"
    # path_to_PopTotals = PATH_TO_DATA + r"/trtotals.pkl"

    # df_survey_input = pd.read_pickle(path_to_SurveyData)
    # df_tr_totals = pd.read_pickle(path_to_PopTotals)

    df_survey_input = survey_input
    df_tr_totals = trtotals

    # Sort input values
    sort1 = ['SAMP_PORT_GRP_PV', 'ARRIVEDEPART']
    df_survey_input_sorted = df_survey_input.sort_values(sort1)

    # Cleanse data
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['SAMP_PORT_GRP_PV'].isnull()]
    df_survey_input_sorted = df_survey_input_sorted[~df_survey_input_sorted['ARRIVEDEPART'].isnull()]

    # df_tr_totals['SAMP_PORT_GRP_PV'] = df_tr_totals['SAMP_PORT_GRP_PV'].str.decode("utf-8")

    # Sort input values
    df_pop_totals = df_tr_totals.sort_values(sort1)

    # Cleanse data
    df_pop_totals = df_pop_totals[~df_pop_totals['SAMP_PORT_GRP_PV'].isnull()]
    df_pop_totals = df_pop_totals[~df_pop_totals['ARRIVEDEPART'].isnull()]

    # Create unique list of items from survey input
    items = df_survey_input_sorted['SAMP_PORT_GRP_PV'].tolist()
    unique = []
    [unique.append(x) for x in items if x not in unique]

    df_pop_totals_match = df_pop_totals[df_pop_totals['SAMP_PORT_GRP_PV'].isin(unique)]

    # Create traffic totals
    df_pop_totals_match = df_pop_totals_match.sort_values(['ARRIVEDEPART','SAMP_PORT_GRP_PV'])
    df_traffic_totals = df_pop_totals_match.groupby(['SAMP_PORT_GRP_PV',
                                                    'ARRIVEDEPART']).agg({"TRAFFICTOTAL": 'sum'}).reset_index()

    # Create lookup. Group by and aggregate
    lookup_dataframe = df_survey_input_sorted.copy()
    lookup_dataframe["count"] = ""
    lookup_dataframe = lookup_dataframe.groupby(['SAMP_PORT_GRP_PV',
                                                 'ARRIVEDEPART']).agg({"count": 'count'}).reset_index()

    # Cleanse data
    lookup_dataframe.drop(["count"], axis=1)
    lookup_dataframe["T1"] = range(len(lookup_dataframe))
    lookup_dataframe["T1"] = lookup_dataframe["T1"] + 1

    # Create population totals for current survey data - Cleanse data and merge
    lookup_dataframe_aux = lookup_dataframe[['SAMP_PORT_GRP_PV','ARRIVEDEPART', 'T1']]
    lookup_dataframe_aux['T1'] = lookup_dataframe_aux.T1.astype(np.int64)

    df_mod_totals = pd.merge(df_traffic_totals, lookup_dataframe_aux, on=['SAMP_PORT_GRP_PV',
                                                                          'ARRIVEDEPART'], how='left')

    df_mod_totals['C_group'] = 1
    df_mod_totals = df_mod_totals.drop(columns=['ARRIVEDEPART', 'SAMP_PORT_GRP_PV'])
    df_mod_pop_totals = df_mod_totals.pivot_table(index='C_group',
                                                  columns='T1',
                                                  values='TRAFFICTOTAL')
    df_mod_pop_totals = df_mod_pop_totals.add_prefix('T_')

    df_mod_pop_totals['C_group'] = 1
    cols = ['C_group'] + [col for col in df_mod_pop_totals if col != 'C_group']
    df_mod_pop_totals = df_mod_pop_totals[cols]

    df_mod_pop_totals = df_mod_pop_totals.reset_index(drop=True)

    # recreate proc_vec table
    con = create_engine('mssql+pyodbc://ips_dev:ips_dev@CR1VWSQL14-D-01/ips_test2?driver=SQL+Server+Native+Client+10.0')

    # note the index gets added so needs to be removed when re-read from SQL
    df_mod_pop_totals.to_sql('test', con, if_exists='replace')

    # Export dataframes to CSV: TODO; delete as used for testing before
    #df_mod_pop_totals.to_csv(r"tests/data/r_setup/October_2017/unsampled_weight/df_pop_totals.csv", index=False)

    df_mod_pop_totals_import = cf.get_table_values('test')
    df_mod_pop_totals_import = df_mod_pop_totals_import.drop('index', axis=1)

    return df_mod_pop_totals_import

def run_r_ges_script():
    """
    Author       : David Powell
    Date         : 25 Sept 2018
    Purpose      : Calculate Traffic GES Weights
    Parameters   :
    Returns      :
    Requirements :
    Dependencies :
    """
    """
    Author       : David Powell
    Date         : 07/06/2018
    Purpose      : Calls R Script to run GES Weighting
    Parameters   : 
    Returns      : Writes GES output to SQL Database
    Requirements : NA
    Dependencies : NA
    """

    print("Starting R script.....")

    retcode = subprocess.call(["C:/Program Files/R/R-3.4.0patched/bin/Rscript",
                           "--vanilla",
                           "NSdata3?:/CASPA/IPS/Testing/Q3 2017/traffic weight/ges_r_step4.r"])

    print("R processed finished.")


def generate_ips_tw_summary(df_survey, df_output_merge_final,
                            var_serialNum, var_trafficWeight,
                            df_popTotals, minCountThresh):
    """
    Author       : Nassir Mohammad
    Date         : 08 Mar 2018
    Purpose      : Calculates IPS Traffic Weight summary
    Parameters   : Survey = survey data set                                                
                   var_serialNum = Variable holding the name of the serial number field
                   var_trafficWeight = Variable holding the name of the traffic wght field    
                   var_priorWeight = Variable holding the name of the prior (design) weight
                   TrafficTotals = Traffic (population) totals dataset
                   minCountThresh = The minimum cell count threshold
    Returns      : dataframe - df_summary_merge_sum_traftot
    Requirements : TODO
    Dependencies : TODO
    """

    # #####################################################
    #
    # calculate the post weight
    # add the traffic weight to the survey data
    #
    # #####################################################

    df_survey_sorted = df_survey.sort_values(var_serialNum)

    # drop duplicate column (with 'None' values) across both tables before merging
    df_survey_sorted_dropped = df_survey_sorted.drop(var_trafficWeight, 1)

    # merge tables
    df_summary_tmp = df_survey_sorted_dropped.merge(df_output_merge_final, on=var_serialNum, how='outer')

    # only keep rows where var_priorWeight > 0
    df_summary_tmp = df_summary_tmp[df_summary_tmp[TRAFFIC_DESIGN_WEIGHT_COLUMN] > 0]

    # calculate and add the post weight column
    df_summary_tmp[POST_WEIGHT_COLUMN] = df_summary_tmp[TRAFFIC_DESIGN_WEIGHT_COLUMN] * df_summary_tmp[var_trafficWeight]

    keep_list = [var_serialNum, STRATA[1],
                 STRATA[0], TRAFFIC_DESIGN_WEIGHT_COLUMN,
                 var_trafficWeight, POST_WEIGHT_COLUMN]

    # only keep the selected columns
    df_summary = df_summary_tmp[keep_list]

    # test code start - either change pandas column types or for the test just ignore the column type - prefer the latter
    # root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    # #df_test = SAS7BDAT(root_data_path + r"\_summary.sas7bdat").to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\_summary.sas7bdat")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_summary, df_test, check_column_type=False)
    # # test code end

    # Summarise the results by strata
    df_summary_sorted = df_summary.sort_values(STRATA)

    # Re-index the data frame
    df_summary_sorted.index = range(df_summary_sorted.shape[0])

    # test code start
    # root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    # # df_test = SAS7BDAT(root_data_path + r"\_summary_stratadef_sort.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\_summary_stratadef_sort.sas7bdat")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_summary_sorted, df_test, check_column_type=False)
    # test code end

    # method will possibly be deprecated - may not be an issue
    df_tmp5 = df_summary_sorted.groupby(STRATA) \
        .agg({POST_WEIGHT_COLUMN: {COUNT_COLUMN: 'count', POST_SUM_COLUMN: 'sum'},
              var_trafficWeight: {var_trafficWeight: 'mean'},
              })

    # drop the additional column indexes
    df_tmp5.columns = df_tmp5.columns.droplevel()

    # reset indexes to keep them aligned
    df_tmp5 = df_tmp5.reset_index()

    # reorder columns for SAS comparison
    col_order = [STRATA[0], STRATA[1], COUNT_COLUMN, POST_SUM_COLUMN, var_trafficWeight]
    df_summary_varpostweight = df_tmp5[col_order]

    # test code start
    # root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Traffic_Weight"
    # # df_test = SAS7BDAT(root_data_path + r"\summary_varpostweight.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\summary_varpostweight.sas7bdat")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_summary_varpostweight, df_test, check_column_type=False, check_dtype=False)
    # test code end    

    # add in the traffic totals
    df_popTotals_stratadef_sort = df_popTotals.sort_values(STRATA)

    # Re-index the data frame
    df_popTotals_stratadef_sort.index = range(df_popTotals_stratadef_sort.shape[0])

    # test code start
    # # df_test = SAS7BDAT(root_data_path + r"\popTotals_stratadef_sort.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\popTotals_stratadef_sort.sas7bdat")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_popTotals_stratadef_sort, df_test, check_column_type=False)
    # test code end

    df_merged = pd.merge(df_popTotals_stratadef_sort, df_summary_varpostweight, on=STRATA, how='outer')

    df_merged[var_trafficWeight] = df_merged[var_trafficWeight].apply(lambda x: round(x, 3))
    df_merged[POST_SUM_COLUMN] = df_merged[POST_SUM_COLUMN].apply(lambda x: round(x, 3))

    # # reorder columns for SAS comparison
    col_order = [STRATA[0], STRATA[1], COUNT_COLUMN, TRAFFIC_TOTAL_COLUMN, POST_SUM_COLUMN, var_trafficWeight]
    df_summary_merge_sum_traftot = df_merged[col_order]
    # df_summary_merge_sum_traftot

    # test code start
    # # df_test = SAS7BDAT(root_data_path + r"\summary_merge_sum_traftot.sas7bdat" ).to_data_frame()
    # df_test = pd.read_sas(root_data_path + r"\summary_merge_sum_traftot.sas7bdat")
    # df_test.columns = df_test.columns.str.upper()
    # assert_frame_equal(df_summary_merge_sum_traftot, df_test, check_column_type=False)
    # test code end

    # perform checks and log
    df_sum = df_summary_merge_sum_traftot
    df_sum_check = df_sum[(df_sum[COUNT_COLUMN].isnull()) | (df_sum[COUNT_COLUMN] < minCountThresh)]
    df_sum_check = df_sum_check[STRATA]

    if len(df_sum_check) > 0:
        threshold_string_cap = 4000
        warningStr = "Respondent count below minimum threshold for"

        # Loop over classificatory variables
        threshold_string = ""
        for index, record in df_sum_check.iterrows():
            threshold_string += \
                warningStr + " " + STRATA[0] + " = " + str(record[0]) \
                + " " + STRATA[1] + "=" + str(record[1]) + "\n"

        threshold_string_capped = threshold_string[:threshold_string_cap]

        cf.database_logger().warning("WARNING: " + threshold_string_capped)

    return df_summary_merge_sum_traftot


def do_ips_trafweight_calculation_with_R(survey_data, trtotals):

    df_r_ges_input = r_survey_input(survey_data)
    df_mod_pop_totals = r_population_input(survey_data, trtotals)

    run_r_ges_script()

    # grab the data from the SQL table and return
    #r_traffic

    return 0