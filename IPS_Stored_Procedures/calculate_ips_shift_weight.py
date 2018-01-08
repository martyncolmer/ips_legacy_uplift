import sys
import os
import logging
import numpy as np
import pandas as pd
from sas7bdat import SAS7BDAT
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict
import survey_support
from IPSTransformation import CommonFunctions as cf

def calculate_factor(row,flag):
    """
    Author       : Thomas Mahoney
    Date         : 02 Jan 2018
    Purpose      : Calculates the factor of the given row's values.
    Parameters   : row  - This parameter represents the row being manipulated
                          from the dataframe calling the function.
                   flag - Used to filter the rows being manipulated. If the
                          flag is true for the given row, the calculation would
                          be made to determine the factor.
    Returns      : The calculated factor value, or a None value.
    Requirements : NA
    Dependencies : NA
    """
    
    if(row[flag] != 0):
        try:
            result = row['NUMERATOR']/row['DENOMINATOR']
        except ZeroDivisionError:
            return None
        else:
            return result
        
    else:
        return None


def calculate_ips_shift_factor():
    """
    Author       : Thomas Mahoney
    Date         : 28 Dec 2017
    Purpose      : Uses the imported surveydata and shiftsdata to calculate the 
                   data sets records' shift factors. This calculated value is 
                   then appended to the original survey data set and used further
                   in the process.
    Parameters   : NA
    Returns      : Three data frames that used to calculate the overall shift 
                   weight and build the final output data set.
                       - df_totalsampledshifts,
                       - df_possibleshifts, 
                       - df_surveydata_sf
    Requirements : NA
    Dependencies : NA
    """

    # Get survey records that are shift based
    df_sampledshifts = df_surveydata[df_surveydata[parameters['var_shiftFlag']] == 1]
    df_sampledshifts.dropna()
    
    # Create a new data frame using the columns specified for the sampled shifts without duplicates
    df_sample_noduplicates = df_sampledshifts[parameters['ShiftsStratumDef'] + ['SHIFTNO']].drop_duplicates()
    
    # Sort the sample data by the 'ShiftsStratumDef' column list
    df_sample_sorted = df_sample_noduplicates.sort_values(parameters['ShiftsStratumDef'])
    
    # Calculate the number of sampled shifts by strata
    df_totalsampledshifts = df_sample_sorted.groupby(parameters['ShiftsStratumDef'])['SHIFTNO'].agg({'DENOMINATOR':'count'})
    
    # Flattens the column structure after adding the new denominator column
    df_totalsampledshifts = df_totalsampledshifts.reset_index()
    
    # Sort the shifts data by the 'ShiftsStratumDef' column list
    df_possibleshifts_temp = df_shiftsdata.sort_values(parameters['ShiftsStratumDef'])

    # Calculate the number of possible shifts by strata
    df_possibleshifts = df_possibleshifts_temp.groupby(parameters['ShiftsStratumDef'])['TOTAL'].agg({'NUMERATOR':'sum'})
    
    # Flattens the column structure after adding the new numerator column
    df_possibleshifts = df_possibleshifts.reset_index()
    
    # Sort the sampled shift data by the 'ShiftsStratumDef' column list
    df_sampledshifts_sorted = df_sampledshifts.sort_values(parameters['ShiftsStratumDef'])
    
    # Merge the dataframes generated above into the sample, by the 'ShiftsStratumDef' column list
    mergedDF = pd.merge(df_sampledshifts_sorted,df_possibleshifts,on = parameters['ShiftsStratumDef'])
    mergedDF = pd.merge(mergedDF,df_totalsampledshifts,on = parameters['ShiftsStratumDef'])
    
    # Calculate the shift factor for each row of the merged dataframe    
    mergedDF[parameters['var_shiftFactor']] = mergedDF.apply(calculate_factor, axis=1,args = (parameters['var_shiftFlag'],))
    
    # Merge the sampled dataframe into the full survey dataframe
    df_surveydata_sf = pd.merge(df_surveydata,mergedDF,'outer')
    
    # Drop the numerator and denominator columns as they're no longer needed
    df_surveydata_sf = df_surveydata_sf.drop(['NUMERATOR','DENOMINATOR'],axis = 1)
    
    # Sort the dataframe by the record's serial number
    df_surveydata_sf = df_surveydata_sf.sort_values(by=['SERIAL'])
    
    # Return the three dataframes produced
    return (df_totalsampledshifts,df_possibleshifts, df_surveydata_sf)


def calculate_ips_crossing_factor(df_surveydata_sf):
    
    print("START - calculate_ips_crossing_factor")
    
       
    
    # -----------------------------------------------------
    # assign all the variable values and make upper case
    # as all table column names are upper case
    # -----------------------------------------------------
    
    # ShiftsStratumDef = Variable holding the shift weight stratum definition
    StratumDef_temp = ["shift_port_grp_pv", "arrivedepart", "weekday_end_pv", "am_pm_night_pv"] #ShiftsStratumDef
    
    # table fields are upper case, so require conversion to upper case
    StratumDef = list(map(lambda x: x.upper(), StratumDef_temp))
    
    # crossingFlag = variable that indicates that this record is crossing 
    crossingFlag = "crossings_flag_pv".upper() #var_crossingFlag CROSSINGS_FLAG_PV
    
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
    
    df_crossingsData = df_shiftsdata # (334, 9)
    df_outputData = df_surveydata_sf # shape=(26347, 212)
    
    
    # -----------------------------------------------------
    # Get survey records that are crossing based
    # -----------------------------------------------------
    
    df_sampled_crossings = df_surveydata.loc[df_surveydata[crossingFlag] == 1] # shape=(3084, 212)
    
    
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
                                                     .agg(OrderedDict([('_FREQ_', 'count'),('DENOMINATOR', 'count')]))\
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
                                                     .agg(OrderedDict([('_FREQ_', 'count'),('NUMERATOR', 'sum')]))\
                                                     .reset_index() # shape = (36, 6)        
    
    df_totalCrossings.index = range(df_totalCrossings.shape[0])
    
    # insert the constant class type in this case as no class specified in SAS proc
    df_totalCrossings.insert(4, "_TYPE_", 0)
    
    # -----------------------------------------------------
    # now compute the crossings factor 
    # -----------------------------------------------------
    
    df_sorted_outputData = df_outputData.sort_values(StratumDef)
    
    df_sorted_outputData #keep all
    df_totalCrossings  = df_totalCrossings[StratumDef + ['NUMERATOR']] # keep &StratumDef numerator
    df_totalSampledCrossings = df_totalSampledCrossings[StratumDef + ['DENOMINATOR']] # (keep = &StratumDef denominator);
    
    left_join_1 = df_sorted_outputData.merge(df_totalCrossings, on=StratumDef, how='left')\
                                      .merge(df_totalSampledCrossings, on=StratumDef, how='left')
        
    # calculate crossings factor    
    left_join_1[crossingsFactor] = left_join_1.apply(calculate_factor, axis=1,args = ('CROSSINGS_FLAG_PV',))

    # drop numerator and denominator columns
    df_surveydata_merge = left_join_1.drop(['NUMERATOR', 'DENOMINATOR'], 1)
    
    print("DONE - calculate_ips_crossing_factor")

    return (df_totalSampledCrossings, df_surveydata_merge)


def do_ips_shift_weight_calculation():
        
    # Calculate the Shift Factor for the given data sets
    shift_factor_dfs = calculate_ips_shift_factor()
    
    # Extract the data frames returned by calculate_ips_shift_factor()
    df_totsampshifts = shift_factor_dfs[0]
    df_possshifts = shift_factor_dfs[1]
    df_surveydata_sf = shift_factor_dfs[2]
    
    # Calculate the Crossings Factor for the given data sets  
    crossings_factor_dfs = calculate_ips_crossing_factor(df_surveydata_sf)
    
    # Extract the data frames returned by calculate_ips_crossing_factor()
    df_totsampcrossings = crossings_factor_dfs[0]
    df_surveydata_merge = crossings_factor_dfs[1]

    # Column sets we are working with
    cols1 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV',
             'AM_PM_NIGHT_PV', 'MIGSI']
    cols2 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 
             'AM_PM_NIGHT_PV']
    cols3 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART']
    cols4 = ['WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS']
    cols5 = ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV',
             'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS', 'SAMP_SHIFT_CROSS',
             'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT']
    cols6 = ['SERIAL', 'SHIFT_WT']
    
    # Make all column headers upper case
    df_surveydata_merge.columns = df_surveydata_merge.columns.str.upper()
    df_possshifts.columns = df_possshifts.columns.str.upper()
    df_totsampcrossings.columns = df_totsampcrossings.columns.str.upper()
    df_totsampshifts.columns = df_totsampshifts.columns.str.upper()
    
    
    # Check for any missing shift factors 
    df_shift_flag = df_surveydata_merge[df_surveydata_merge['SHIFT_FLAG_PV'] == 1]
    
    if(len(df_shift_flag[df_shift_flag.SHIFT_FACTOR.isnull()])>0):
        logger.error('Error: Case(s) contain no shift factor(s)')
    else:
        df_surveydata_merge.loc[df_surveydata_merge.SHIFT_FACTOR.isnull() & (df_surveydata_merge.SHIFT_FLAG_PV != 1), 'SHIFT_FACTOR'] = 1
        logger.info('Success: Contains shift factor(s)')
    
    
    
    # Check for missing crossings factor
    df_crossings_flag = df_surveydata_merge[df_surveydata_merge['CROSSINGS_FLAG_PV'] == 1]
    
    if(len(df_crossings_flag[df_crossings_flag.CROSSINGS_FACTOR.isnull()])>0):
        logger.error('Error: Case(s) contain no crossings factor(s)')
        print("Error: Case(s) contain no crossings factor(s)")
    else:
        df_surveydata_merge.loc[df_surveydata_merge.CROSSINGS_FACTOR.isnull() & (df_surveydata_merge.CROSSINGS_FLAG_PV != 1), 'CROSSINGS_FACTOR'] = 1
        print("Success: Contains crossings factor(s)")
        logger.info('Success: Contains crossings factor(s)')
    
    
    # Check for invalid shift data
    df_invalid_shifts = df_surveydata_merge[df_surveydata_merge['SHIFT_FACTOR'] < 0]
    
    if len(df_shift_flag)>0 & len(df_invalid_shifts)>0:
        logger.error('Error: Case(s) has an invalid number of possible shifts')
        print("ERROR - Case(s) has an invalid number of possible shifts")
        
    # Check for invalid crossings data
    df_invalid_crossings = df_surveydata_merge[df_surveydata_merge['CROSSINGS_FACTOR'] < 0]
    
    if len(df_crossings_flag)>0 & len(df_invalid_crossings)>0:
        logger.error('Error: Case(s) has an invalid number of total crossings')
        print("ERROR - Case(s) has an invalid number of total crossings")   
           
       
    # Check for missing migration sampling intervals     
    df_missing_migsi = df_surveydata_merge['MIGSI'].isnull().sum()
    
    if df_missing_migsi > 0:
        logger.error('Error: Case(s) missing migration sampling interval')
        print("ERROR - Case(s) missing migration sampling interval")        
          
         
    # Calculate shift weight
    df_surveydata_merge['SHIFT_WT'] = df_surveydata_merge.SHIFT_FACTOR \
        * df_surveydata_merge.CROSSINGS_FACTOR \
        * df_surveydata_merge.MIGSI
    
    # Sort surveydata
    df_surveydata_merge = df_surveydata_merge.sort_values(cols1)
    
    # Groupby and aggregate summary
    data_summary = df_surveydata_merge.groupby(cols1)['SHIFT_WT'].agg({
        'COUNT_RESPS': 'count',
        'SUM_SH_WT': 'sum',
        'MIN_SH_WT': 'min',
        'MEAN_SH_WT': 'mean',
        'MAX_SH_WT': 'max'
    })
    
    # Flatten summary
    data_summary = data_summary.reset_index()
    
    # Merge possible shifts to summary
    data_summary = pd.merge(data_summary, df_possshifts, on = cols2, how = 'outer')
    data_summary = data_summary.rename(columns = {'NUMERATOR': 'POSS_SHIFT_CROSS'})
    
    # Merge totsampcrossings to summary
    data_summary = pd.merge(data_summary, df_totsampcrossings, on = cols2, how = 'outer')
    data_summary = data_summary.rename(columns = {'DENOMINATOR': 'SAMP_SHIFT_CROSS'})
    
    # Merge totsampshifts to summary
    data_summary = pd.merge(data_summary, df_totsampshifts, on = cols2, how = 'outer')
    data_summary = data_summary.rename(columns = {'DENOMINATOR': 'SAMP_SHIFT_CROSS_TEMP'})
    
    # Merge total sample crossings and total sample shifts to single column via addition
    data_summary['SAMP_SHIFT_CROSS'] = data_summary.SAMP_SHIFT_CROSS.fillna(0) \
        + data_summary.SAMP_SHIFT_CROSS_TEMP.fillna(0)
    data_summary = data_summary.drop(['SAMP_SHIFT_CROSS_TEMP'], 1)
    
    # Sort summary
    data_summary = data_summary.sort_values(cols2)
    
    # Sort survey data
    df_surveydata_merge = df_surveydata_merge.sort_values(cols3)
    
    # Groupby and aggregate summary high
    data_summary_high = df_surveydata_merge.groupby(cols3)['SHIFT_WT'].agg({
        'COUNT_RESPS': 'count',
        'SUM_SH_WT': 'sum',
        'MIN_SH_WT': 'min',
        'MEAN_SH_WT': 'mean',
        'MAX_SH_WT': 'max'
    })
    
    # Flatten summary high
    data_summary_high = data_summary_high.reset_index()
    
    # Append total sample crossings and total sample shifts
    df_totsampshifts.append(df_totsampcrossings)
    
    # Sort total sample shifts
    df_totsampshifts = df_totsampshifts.sort_values(cols3)
    
    # Groupby and aggregate summary high sampled
    data_summary_high_sampled = df_totsampshifts.groupby(cols3)['DENOMINATOR'].agg({
        'SAMP_SHIFT_CROSS': 'sum'
    })
    
    # Flatten summary high sampled
    data_summary_high_sampled = data_summary_high_sampled.reset_index()
    
    # Left merge summary high with summary high sampled
    data_summary_high = pd.merge(data_summary_high, data_summary_high_sampled,
        on = cols3, how = 'left')
    
    # Append summary and summary high
    data_summary = data_summary.append(data_summary_high)
    
    # Set summary columns
    data_summary = data_summary[cols5]
    
    # Sort summary SUMMARY
    data_summary = data_summary.sort_values(cols5)
    
    # Create shift weight threshold data sets
    df_min_sw_check = data_summary[data_summary['SAMP_SHIFT_CROSS'].notnull() & (data_summary['MIN_SH_WT'] < 50)]
    df_max_sw_check = data_summary[data_summary['SAMP_SHIFT_CROSS'].notnull() & (data_summary['MAX_SH_WT'] > 5000)]
    
    # Merge shift weight threshold data sets
    df_sw_thresholds_check = pd.merge(df_min_sw_check, df_max_sw_check, on = cols1, how = 'outer')
    
    # Collect data outside of specified thresh 
    threshold_string = ""
        
    for index, record in df_sw_thresholds_check.iterrows():
        threshold_string += "___||___" \
                         + df_sw_thresholds_check.columns[0] + " : " + str(record[0]) + " | "\
                         + df_sw_thresholds_check.columns[1] + " : " + str(record[1]) + " | "\
                         + df_sw_thresholds_check.columns[2] + " : " + str(record[2]) + " | "\
                         + df_sw_thresholds_check.columns[3] + " : " + str(record[3])
    
    print(threshold_string)
    
    
    if len(df_sw_thresholds_check) > 0:
        logger.warning('WARNING: Shift weight outside thresholds for: ' + threshold_string)
        print("WARNING - Shift weight outside thresholds for: " + threshold_string) #df_sw_thresholds_check
    
    # Set surveydata columns
    df_surveydata_merge = df_surveydata_merge[cols6]
    
    # Sort surveydata OUT
    df_surveydata_merge = df_surveydata_merge.sort_values(cols6)
    
    
    return (df_surveydata_merge , data_summary)


""""""  

# Calls json configuration file for error logger setup
survey_support.setup_logging('IPS_logging_config_debug.json')  
# Creates logger object   
logger = logging.getLogger(__name__)                            

# Connect to oracle and unload parameter list
conn = cf.get_oracle_connection()
parameters = cf.unload_parameters()

root_data_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Calculate_IPS_Shift_Weight"

# Load sas files into dataframes (This will get data from oracle eventually)
path_to_survey_data = root_data_path + r"\surveydata.sas7bdat"
path_to_shifts_data = root_data_path + r"\shiftsdata.sas7bdat"



# This method works for all data sets but is slow
#df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()
#df_shiftsdata = SAS7BDAT(path_to_shifts_data).to_data_frame()

# This method is untested with other data sets but is very fast
df_surveydata = pd.read_sas(path_to_survey_data)
df_shiftsdata = pd.read_sas(path_to_shifts_data)


cf.ips_error_check()


weight_calculated_dataframes = do_ips_shift_weight_calculation()
 
# Extract the two data sets returned from do_ips_shift_weight_calculation
surveydata_dataframe = weight_calculated_dataframes[0]   
summary_dataframe = weight_calculated_dataframes[1]


# Append the generated data to output tables
cf.insert_into_table_many('SAS_SHIFT_WT', surveydata_dataframe)
cf.insert_into_table_many('SAS_PS_SHIFT_DATA', summary_dataframe)


# Need to ask dave about this      
for x in range(0,10):
    print("ASK DAVE ABOUT commit_ips_response!!!")     
             
#cf.commit_ips_response()

print("done")