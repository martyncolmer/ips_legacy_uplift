'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import pyodbc

from main.io import import_traffic_data
from main.io import CommonFunctions as cf


class TestCommonFunctions(unittest.TestCase):
    def setUp(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Creates global variables for file locations
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : None
        """
        self.real_table_name = "TRAFFIC_DATA"            # Real
        self.empty_table_name = "EMPTY_TEST_TABLE"       # Empty
        self.fake_table_name = "THIS_IS_FAKE"            # Fake/Non-existent        
        self.create_table_name = "ELS_TEST_TABLE"        # Generic

    def test_create_table(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.create_table() by creating a 
                        : table.  It cannot be done a second time as table
                        : already exists
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.create_table()
        """
        
        self.assertTrue(cf.create_table(self.create_table_name,
                                        ("test1 varchar(40)"
                                           , "test2 int"
                                           , "test3 int")))
        self.assertFalse(cf.create_table(self.create_table_name,
                                         ("test1 varchar(40)"
                                          , "test2 int"
                                          , "test3 int")))

    def test_check_table(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.check_table() 
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.check_table()
        """
        # Run tests with real and fake tables
        self.assertTrue(cf.check_table(self.real_table_name))
        self.assertFalse(cf.check_table(self.fake_table_name))            

    def test_drop_table(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.drop_table() 
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.drop_table()
        """
        
        # Creates table and then run tests with real and fake tables
        cf.create_table(self.create_table_name, ("test1 varchar(40)", "test2 int", "test3 int"))
        self.assertTrue(cf.drop_table(self.create_table_name))        
        self.assertFalse(cf.drop_table(self.fake_table_name))

    def test_delete_from_table(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.delete_from_table() 
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.delete_from_table()
        """
        
        # Creates real table with real data
        import_traffic_data.import_traffic_data(run_id='', filename="\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv")
        
        # Creates empty table
        cols = ("Test_Name_One varchar(40)", "Test_Name_Two int")
        cf.create_table(self.empty_table_name, cols)
        
        # Runs test with real, empty and fake tables
        self.assertTrue(cf.delete_from_table(self.real_table_name))     
        self.assertTrue(cf.delete_from_table(self.empty_table_name))    
        self.assertFalse(cf.delete_from_table(self.fake_table_name))    
        
        # Runs test with real tables but with additional 
        # parameters and conditional operators for 'WHERE' clause 
        self.assertTrue(cf.delete_from_table(self.real_table_name, "PORTROUTE", "=", "651"))                  
        self.assertTrue(cf.delete_from_table(self.real_table_name, "TRAFFICTOTAL", "!=", "0"))                
        self.assertTrue(cf.delete_from_table(self.real_table_name, "TRAFFICTOTAL", "BETWEEN", "631", "731"))  
        self.assertFalse(cf.delete_from_table(self.real_table_name, "TRAFFICTOTAL", "", "631", "731"))           
        self.assertFalse(cf.delete_from_table(self.real_table_name, "FAKE_CONDITION", "BETWEEN", "631", "731"))

    def test_select_data(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.select_data() 
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.select_data()
        """
        
        # Runs test with real tables and incorrect parameters
        self.assertEqual(cf.select_data("DATA_SOURCE_ID", "DATA_SOURCE", "DATA_SOURCE_NAME", "Sea"), 1)
        self.assertEqual(cf.select_data("DISPLAY_VALUE", "COLUMN_LOOKUP", "LOOKUP_KEY", "3"), "Deleted")
        self.assertFalse(cf.select_data("x", "DATA_SOURCE", "DATA_SOURCE_NAME", "SEA"))
        self.assertFalse(cf.select_data("DATA_SOURCE_ID", "x", "DATA_SOURCE_NAME", "SEA"))
        self.assertFalse(cf.select_data("DATA_SOURCE_ID", "DATA_SOURCE", "x", "SEA"))
        self.assertFalse(cf.select_data("DATA_SOURCE_ID", "DATA_SOURCE", "DATA_SOURCE_NAME", "x"))

    @unittest.expectedFailure
    def test_unload_parameters(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.unload_parameters() 
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.unload_parameters()
        """
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

        # Runs tests with real, non-existent and wrong IDs
        self.assertEqual(cf.unload_parameters(), expected_dict )    
        self.assertEqual(cf.unload_parameters(52), expected_dict)   
        self.assertFalse(cf.unload_parameters(990))                 
        with self.assertRaises(pyodbc.DatabaseError):
            cf.unload_parameters("Hello World")
        with self.assertRaises(pyodbc.DatabaseError):
            cf.unload_parameters(True)                              

    def tearDown(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Drops or deletes from test tables as applicable
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : None
        """
        if cf.check_table(self.create_table_name) == True:
            cf.drop_table(self.create_table_name)
            
        if cf.check_table(self.real_table_name) == True:
            cf.delete_from_table(self.real_table_name)
        
        if cf.check_table(self.empty_table_name) == True:
            cf.drop_table(self.empty_table_name)
            

if __name__ == '__main__':
    unittest.main()
