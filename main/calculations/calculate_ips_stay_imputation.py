import pandas as pd

from main.calculations import ips_impute

# import survey_support

OUTPUT_TABLE_NAME = 'SAS_STAY_IMP'
DONOR_VARIABLE = 'NUMNIGHTS'
OUTPUT_VARIABLE = 'STAY'
ELIGIBLE_FLAG_VARIABLE = 'STAY_IMP_ELIGIBLE_PV'
IMPUTATION_FLAG_VARIABLE = 'STAY_IMP_FLAG_PV'
IMPUTATION_LEVEL_VARIABLE = 'STAYK'
STRATA_BASE_LIST = [['STAYIMPCTRYLEVEL4_PV', 'STAY_PURPOSE_GRP_PV']]
THRESHOLD_BASE_LIST = [1]


def do_ips_stay_imputation(df_input, var_serial, num_levels, measure):
    """
    Author       : James Burr
    Date         : 12 Feb 2018
    Purpose      : Imputes stay for the IPS system.
    Parameters   : df_input - the IPS survey dataset.
                   var_serial - the serial number field name
                   num_levels - number of imputation levels
                   measure - This variable should contain a function name which can only be used in lower case
    Returns      : Dataframe - df_output_final
    Requirements : NA
    Dependencies : NA
    """
    # Ensure imputation only occurs on eligible rows
    df_eligible = df_input.where(df_input[ELIGIBLE_FLAG_VARIABLE] == 1)
    df_eligible.dropna(inplace=True, how='all')

    df_output_final = ips_impute.ips_impute(df_eligible, var_serial,
                                            STRATA_BASE_LIST, THRESHOLD_BASE_LIST,
                                            num_levels, DONOR_VARIABLE, OUTPUT_VARIABLE,
                                            measure, IMPUTATION_FLAG_VARIABLE,
                                            IMPUTATION_LEVEL_VARIABLE)

    # Round output column to nearest integer
    # Amended to cast object dtype to float. 24/09/2018. ET
    df_output_final[OUTPUT_VARIABLE] = pd.to_numeric(df_output_final[OUTPUT_VARIABLE], errors='coerce').round()

    return df_output_final

