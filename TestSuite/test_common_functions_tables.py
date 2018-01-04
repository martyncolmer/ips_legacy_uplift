'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import cx_Oracle
import sys

import import_traffic_data
from IPSTransformation import CommonFunctions as cf

class TestCommonFunctions(unittest.TestCase):
    def setUp(self):
        self.real_table_name = "TRAFFIC_DATA"            # Real
        self.empty_table_name = "EMPTY_TEST_TABLE"       # Empty
        self.fake_table_name = "THIS_IS_FAKE"            # Fake/Non-existent        
        self.create_table_name = "ELS_TEST_TABLE"        # Generic
        
      
    def test_create_table(self):
        self.assertTrue(cf.create_table(self.create_table_name, ("test1 varchar2(40)", "test2 number(4)", "test3 number(2)")))
        self.assertFalse(cf.create_table(self.create_table_name, ("test1 varchar2(40)", "test2 number(4)", "test3 number(2)")))
    

    def test_check_table(self):
        self.assertTrue(cf.check_table(self.real_table_name))
        self.assertFalse(cf.check_table(self.fake_table_name))            


    def test_drop_table(self):
        cf.create_table(self.create_table_name, ("test1 varchar2(40)", "test2 number(4)", "test3 number(2)"))        
        self.assertTrue(cf.drop_table(self.create_table_name))        
        self.assertFalse(cf.drop_table(self.fake_table_name))
    
    
    def test_delete_from_table(self):
        print(import_traffic_data.import_data(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"))
        
        cols = ("Test_Name_One varchar2(40)", "Test_Name_Two number(4)")
        cf.create_table(self.empty_table_name, cols)
        
        print(import_traffic_data.import_data(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"))
        
        self.assertTrue(cf.delete_from_table(self.real_table_name))     # Real
        self.assertTrue(cf.delete_from_table(self.empty_table_name))    # Empty
        self.assertFalse(cf.delete_from_table(self.fake_table_name))    # Fake
        
        # Additional parameters 
        self.assertTrue(cf.delete_from_table(self.real_table_name, "PORTROUTE", "=", "651"))                  # Real '='
        self.assertTrue(cf.delete_from_table(self.real_table_name, "TRAFFICTOTAL", "!=", "0"))                # Real '!='
        self.assertTrue(cf.delete_from_table(self.real_table_name, "TRAFFICTOTAL", "BETWEEN", "631", "731"))  # Real 'BETWEEN'
        
        self.assertFalse(cf.delete_from_table(self.real_table_name, "TRAFFICTOTAL", "", "631", "731"))           # Empty
        self.assertFalse(cf.delete_from_table(self.real_table_name, "FAKE_CONDITION", "BETWEEN", "631", "731"))  # Fake
            
    
    def test_select_data(self):
        self.assertEqual(cf.select_data("DATA_SOURCE_ID"
                                        , "DATA_SOURCE"
                                        , "DATA_SOURCE_NAME"
                                        , "Sea"), "1")          # Real table: DATA_SOURCE
        self.assertEqual(cf.select_data("DISPLAY_VALUE"
                                , "COLUMN_LOOKUP"
                                , "LOOKUP_KEY"
                                , "3"), "'Deleted'")            # Real table: COLUMN_LOOKUP
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.select_data("x"
                           , "DATA_SOURCE"
                           , "DATA_SOURCE_NAME"
                           , "SEA")                             # Incorrect column_name
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.select_data("DATA_SOURCE_ID"
                                , "x"
                                , "DATA_SOURCE_NAME"
                                , "SEA")                        # Incorrect table_name
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.select_data("DATA_SOURCE_ID"
                                , "DATA_SOURCE"
                                , "x"
                                , "SEA")                        # Incorrect condition1        
        self.assertFalse(cf.select_data("DATA_SOURCE_ID"
                                , "DATA_SOURCE"
                                , "DATA_SOURCE_NAME"
                                , "x"))                         # Incorrect condition2        
    
  
    def test_unload_parameters(self):
        expected_dict = {'SUBSTRATA': 'shift_port_grp_pv arrivedepart'
                            , 'SUMMARYDATA': 'sas_ps_shift_data'
                            , 'VAR_SHIFTWEIGHT': 'shift_wt'
                            , 'RESPONSETABLE': 'sas_response'   
                            , 'VAR_MAXWEIGHT': 'max_sh_wt'
                            , 'VAR_SHIFTFLAG': 'shift_flag_pv'
                            , 'VAR_TOTALS': 'total'
                            , 'VAR_SERIALNUM': 'serial'
                            , 'SHIFTSSTRATUMDEF': 'shift_port_grp_pv arrivedepart weekday_end_pv am_pm_night_pv'
                            , 'VAR_CROSSINGSFACTOR': 'crossings_factor'
                            , 'VAR_CROSSINGFLAG': 'crossings_flag_pv'
                            , 'VAR_MINWEIGHT': 'min_sh_wt'  
                            , 'VAR_SAMPLEDCOUNT': 'samp_shift_cross'
                            , 'VAR_WEIGHTSUM': 'sum_sh_wt'
                            , 'MAXWEIGHTTHRESH': '5000'
                            , 'MINWEIGHTTHRESH': '50'
                            , 'VAR_SHIFTNUMBER': 'shiftno'
                            , 'VAR_CROSSINGNUMBER': 'shuttle'
                            , 'VAR_AVGWEIGHT': 'mean_sh_wt'
                            , 'SURVEYDATA': 'sas_survey_subsample'
                            , 'VAR_POSSIBLECOUNT': 'poss_shift_cross'
                            , 'SHIFTSDATA': 'sas_shift_data'
                            , 'VAR_SUMMARYKEY': 'shift_port_grp_pv'
                            , 'VAR_COUNT': 'count_resps'
                            , 'VAR_SI': 'migSI'
                            , 'PROCESS_NAME': 'Foundation/ips/calculate_ips_shift_weight_sp'
                            , 'OUTPUTDATA': 'sas_shift_wt'
                            , 'VAR_SHIFTFACTOR': 'shift_factor'}
        
        
        self.assertEqual(cf.unload_parameters(), expected_dict )    # Real
        self.assertEqual(cf.unload_parameters(52), expected_dict)   # Real ID
        self.assertFalse(cf.unload_parameters(990))                 # Non-existent ID
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.unload_parameters("Hello World")                     # Wrong ID
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.unload_parameters(True)                              # Wrong ID

    
    def tearDown(self):
        if cf.check_table(self.create_table_name) == True:
            cf.drop_table(self.create_table_name)
            
        if cf.check_table(self.real_table_name) == True:
            cf.delete_from_table(self.real_table_name)
        
        if cf.check_table(self.empty_table_name) == True:
            cf.drop_table(self.empty_table_name)
            

if __name__ == '__main__':
    unittest.main()