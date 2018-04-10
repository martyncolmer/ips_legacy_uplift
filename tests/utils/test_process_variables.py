import pandas as pd
import sys
import unittest
import numpy as np
from IPS_XML import shift_weight
from main.utils import process_variables
from main.io import CommonFunctions as cf


def test_shift_wt_pvs():
    """
    Author       : Thomas Mahoney
    Date         : 06 / 04 / 2018
    Purpose      : Tests the process variables step of the IPS calculation process using shift wt step data.
    Parameters   : NA
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """
    # Exit as testing will commence during
    return True

    conn = cf.get_oracle_connection()

    # Clears tables ready for processing
    cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
    cf.delete_from_table('SAS_PROCESS_VARIABLE')

    # Copies the process variable statements into the SAS_PROCESS_VARIABLE table
    shift_weight.copy_shift_wt_pvs_for_survey_data('IPSSeedRunFR02', conn)

    # loads in the subsample and writes it to the table
    df_input = pd.read_csv(r'../data/traffic_wt_pv_test_data.csv')
    df_input = df_input.where((pd.notnull(df_input)), None)
    cf.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', df_input)

    # Applies the process variable statements to the dataset
    process_variables.process(in_table_name='SAS_SURVEY_SUBSAMPLE',
                              out_table_name='SAS_SHIFT_SPV',
                              in_id='SERIAL',
                              dataset='survey',)

    # Extracts and checks the process variables result
    df_out = cf.get_table_values('SAS_SHIFT_SPV')


if __name__ == '__main__':
    test_shift_wt_pvs()
