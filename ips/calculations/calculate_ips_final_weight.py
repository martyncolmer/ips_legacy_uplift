from typing import Tuple

from pandas import DataFrame

NUMBER_RECORDS_DISPLAYED = 20


def do_ips_final_wt_calculation(df_surveydata: DataFrame, serial_num, shift_weight, non_response_weight
                                , min_weight, traffic_weight, unsampled_weight
                                , imbalance_weight, final_weight) -> Tuple[DataFrame, DataFrame]:
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

    df_final_weight[final_weight] = df_final_weight[shift_weight] * df_final_weight[non_response_weight] * \
                                    df_final_weight[min_weight] * df_final_weight[traffic_weight] * \
                                    df_final_weight[unsampled_weight] * df_final_weight[imbalance_weight]

    # Generate summary output
    df_summary = df_final_weight[[serial_num, shift_weight, non_response_weight, min_weight, traffic_weight,
                                  unsampled_weight, imbalance_weight, final_weight]]

    # Sort summary, then select var_recordsDisplayed number of random rows for
    # inclusion in the summary dataset
    df_summary = df_summary.sample(NUMBER_RECORDS_DISPLAYED)

    df_summary = df_summary.sort_values(serial_num)

    # Condense output dataset to the two required variables
    df_output = df_final_weight[[serial_num, final_weight]]

    return df_output, df_summary
