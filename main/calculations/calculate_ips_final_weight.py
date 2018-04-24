import pandas as pd
import survey_support

path_to_data = r"../../tests/data/final_weight"


OUTPUT_TABLE_NAME = 'SAS_FINAL_WT'
SUMMARY_TABLE_NAME = 'SAS_PS_FINAL'
NUMBER_RECORDS_DISPLAYED = 20


def do_ips_final_wt_calculation(df_surveydata, var_serialNum, var_shiftWeight, var_NRWeight
                                , var_minWeight, var_trafficWeight, var_unsampWeight
                                , var_imbWeight, var_finalWeight):
    """
    Author       : James Burr / Nassir Mohammad
    Date         : 17 Apr 2018
    Purpose      : Generates the IPS Final Weight value
    Parameters   : df_surveydata - the IPS survey records for the relevant period
                   var_serialNum - Variable holding the serial number for the record
                   var_shiftWeight - Variable holding the name of the shift weight field
                   var_NRWeight - Variable holding the name of the nr weight field
                   var_minWeight - Variable holding the name of the min weight field
                   var_trafficWeight - Variable holding the name of the traffic weight field
                   var_unsampWeight - Variable holding the name of the unsampled weight field
                   var_imbWeight - Variable holding the name of the imbalance weight field
                   var_finalWeight - Variable holding the name of the final weight field
                   var_recordsDisplayed - Number of records to display
    Returns      : Dataframes - df_summary(dataframe containing random sample of rows)
                   ,df_output(dataframe containing serial number and calculated final weight)
    Requirements : NA
    Dependencies : NA
    """

    # Calculate the final weight value in a new column

    df_final_weight = df_surveydata

    df_final_weight[var_finalWeight] = df_final_weight[var_shiftWeight] * \
                                       df_final_weight[var_NRWeight] * \
                                       df_final_weight[var_minWeight] * \
                                       df_final_weight[var_trafficWeight] * \
                                       df_final_weight[var_unsampWeight] * \
                                       df_final_weight[var_imbWeight]

    # Generate summary output
    df_summary = df_final_weight[[var_serialNum, var_shiftWeight, var_NRWeight
        , var_minWeight, var_trafficWeight, var_unsampWeight
        , var_imbWeight, var_finalWeight]]

    # Sort summary, then select var_recordsDisplayed number of random rows for
    # inclusion in the summary dataset
    df_summary = df_summary.sample(NUMBER_RECORDS_DISPLAYED)

    df_summary = df_summary.sort_values(var_serialNum)

    # Condense output dataset to the two required variables
    df_output = df_final_weight[[var_serialNum, var_finalWeight]]

    return (df_output, df_summary)


def calculate(SurveyData, var_serialNum, var_shiftWeight, var_NRWeight,
              var_minWeight, var_trafficWeight, var_unsampWeight, var_imbWeight,
              var_finalWeight):
    """
    Author       : James Burr / Nassir Mohammad
    Date         : 17 Apr 2018
    Purpose      : Calculates the IPS Final Weight
    Parameters   : SurveyData = the IPS survey records for the period
				   var_SerialNum = Variable holding the record serial number (UID)
				   var_shiftWeight = Variable holding the name of the shift weight field
				   var_NRWeight = Variable holding the name of the NR weight field
				   var_minWeight = Variable holding the name of the min weight field
				   var_trafficWeight = Variable holding the name of the traffic wht field
			       var_OOHWeight = Variable holding the name of the OOH weight field
				   var_imbWeight = Variable holding the name of the imbalance weight field
				   var_finalWeight = Variable holding the name of the final weight field
				   var_recordsDisplayed = number of records to display in summary output
    Returns      : dataframe tuple: (surveydata_dataframe, summary_dataframe)
    Requirements : do_ips_final_wt_calculation()
    Dependencies :
    """

    print("Start - Calculate Final Weight")

    # import data set

    # Call JSON configuration file for error logger setup
    survey_support.setup_logging('IPS_logging_config_debug.json')

    # Setup path to the base directory containing data files
    path_to_survey_data = path_to_data + r"/surveydata.pkl"

    df_surveydata = pd.read_pickle(path_to_survey_data)

    # Import data via SAS
    # This method works for all data sets but is slower
    #df_surveydata = SAS7BDAT(path_to_survey_data).to_data_frame()

    # This method is untested with a range of data sets but is faster
    # df_surveydata = pd.read_sas(path_to_survey_data)

    # Import data via SQL
    # df_surveydata = cf.get_table_values(SurveyData)

    df_surveydata.columns = df_surveydata.columns.str.upper()

    print("Start - do_ips_final_wt_calculation()")
    weight_calculated_dataframes = do_ips_final_wt_calculation(df_surveydata
                                                               , var_serialNum
                                                               , var_shiftWeight
                                                               , var_NRWeight
                                                               , var_minWeight
                                                               , var_trafficWeight
                                                               , var_unsampWeight
                                                               , var_imbWeight
                                                               , var_finalWeight)

    # Extract the two data sets returned from do_ips_shift_weight_calculation
    surveydata_dataframe = weight_calculated_dataframes[0]
    summary_dataframe = weight_calculated_dataframes[1]

    # re-index
    surveydata_dataframe.index = range(surveydata_dataframe.shape[0])
    summary_dataframe.index = range(summary_dataframe.shape[0])

    return (surveydata_dataframe, summary_dataframe)

    # TODO - move code below when main refactored
    # # Append the generated data to output tables
    # cf.insert_dataframe_into_table(OutputData, surveydata_dataframe)
    # cf.insert_dataframe_into_table(SummaryData, summary_dataframe)
    #
    # # Retrieve current function name using inspect:
    # # 0 = frame object, 3 = function name.
    # # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    # function_name = str(inspect.stack()[0][3])
    # audit_message = "Load Final Weight calculation: %s()" %function_name
    #
    # # Log success message in SAS_RESPONSE and AUDIT_LOG
    # cf.database_logger().info("SUCCESS - Completed Final weight calculation.")
    # cf.commit_to_audit_log("Create", "FinalWeight", audit_message)

    #print("Completed - Calculate Final Weight")

