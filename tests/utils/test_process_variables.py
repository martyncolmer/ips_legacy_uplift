import pandas as pd
import sys
import unittest
import numpy as np
from IPS_XML import shift_weight
from main.utils import process_variables
from main.io import CommonFunctions as cf

class pv_test(unittest.TestCase):
    def test_shift_wt_pvs(self):
        """
        Author       : Thomas Mahoney
        Date         : 06 / 04 / 2018
        Purpose      : Tests the process variables step of the IPS calculation process using shift wt step data.
        Parameters   : NA
        Returns      : NA
        Requirements : NA
        Dependencies : NA
        """
        conn = cf.get_oracle_connection()

        cf.delete_from_table('SAS_SURVEY_SUBSAMPLE')
        cf.delete_from_table('SAS_PROCESS_VARIABLE')
        shift_weight.copy_shift_wt_pvs_for_survey_data('IPSSeedRunFR02', conn)

        df_input = pd.read_csv(r'../data/traffic_wt_pv_test_data.csv')
        df_input = df_input.head(2)

        cf.insert_dataframe_into_table_rbr('SAS_SURVEY_SUBSAMPLE', df_input)
        #cf.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', df_input)

        process_variables.process(in_table_name='SAS_SURVEY_SUBSAMPLE',
                                  out_table_name='SAS_SHIFT_SPV',
                                  in_id='SERIAL',
                                  dataset='survey',)

        df_out = cf.get_table_values('SAS_SHIFT_SPV')
        df_out.to_csv("OUTPUT_PV_FILE.csv", index=False)

    def test_something_else(self):
        self.assertEqual(True, True)




if __name__ == '__main__':
    unittest.main()
