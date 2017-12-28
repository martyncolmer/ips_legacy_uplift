'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import os
import cx_Oracle

from IPSTransformation import CommonFunctions as cf


class TestCommonFunctions(unittest.TestCase):
    def setUp(self):
        self.dave_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
        self.rich_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials2.txt"
        self.empty_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\EmptyIPSCredentials.txt"
        self.fake_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\FakeCredentials.txt"
        self.non_existent_creds_file = r"\\hello\this_is\a\fake\directory"
        
        self.real_table_name = "TRAFFIC_DATA"            # Real
        self.empty_table_name = "EMPTY_TEST_TABLE"       # Empty
        self.fake_table_name = "THIS_IS_FAKE"            # Fake/Non-existent
        
        self.create_table_name = "ELS_TEST_TABLE"
        
      
#    def test_validate_file(self):
#        print "1: test_validate_file"
#        self.assertTrue(cf.validate_file(self.dave_creds_file))               # Dave's creds
#        self.assertTrue(cf.validate_file(self.rich_creds_file))               # Rich's creds file
#        self.assertFalse(cf.validate_file(self.empty_creds_file))             # Empty creds file
#        self.assertTrue(cf.validate_file(self.fake_creds_file))               # Fake creds file
#        self.assertFalse(cf.validate_file(self.non_existent_creds_file))      # Non-existent creds file
#            
#    
#    def test_oracle_connection(self): 
#        print "2: test_oracle_connection"       
#        self.assertIsNotNone(cf.get_oracle_connection(self.dave_creds_file))     # Dave's creds file
#        self.assertIsNotNone(cf.get_oracle_connection(self.rich_creds_file))     # Rich's creds file        
#        with self.assertRaises(TypeError):
#            cf.get_oracle_connection(self.empty_creds_file)                      # Empty creds file
#        with self.assertRaises(cx_Oracle.DatabaseError):
#            cf.get_oracle_connection(self.fake_creds_file)                       # Fake creds file
#        with self.assertRaises(TypeError):
#            cf.get_oracle_connection(self.non_existent_creds_file)               # Non-existent creds file
#    
#    
#    def test_get_credentials(self):
#        print "3: test_get_credentials"
#        self.assertIsNotNone(cf.get_credentials(self.dave_creds_file))      # Dave's creds file        
#        self.assertIsNotNone(cf.get_credentials(self.rich_creds_file))      # Rich's creds file        
#        self.assertFalse(cf.get_credentials(self.empty_creds_file))         # Empty creds file
#        self.assertIsNotNone(cf.get_credentials(self.fake_creds_file))      # Fake creds file
#        self.assertFalse(cf.get_credentials(self.non_existent_creds_file))  # Non-existent creds file
#       
#        
#    def test_extract_zip_true(self):
#        print "4: test_extract_zip_true"
#        filename = "testdata.zip"
#        correct_dir = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
#        empty_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Empty Folder"
#        no_zip_values = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor"
#        non_existent_dir = r"\\hello\this_is\a\fake\directory"
#        
#        self.assertTrue(cf.extract_zip(correct_dir, filename))       # Real values
#        self.assertFalse(cf.extract_zip(empty_file, filename))       # Empty file
#        self.assertFalse(cf.extract_zip(no_zip_values, filename))    # No zip values
#        self.assertFalse(cf.extract_zip(non_existent_dir, filename)) # Non-existent file
#        
#
#    def test_import_csv(self):
#        print "5: test_import_csv"
#        real_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
#                
#        self.assertIsNotNone(cf.import_csv(real_filename))              # Real
#        self.assertFalse(cf.import_csv(self.empty_creds_file))          # Empty
#        self.assertFalse(cf.import_csv(self.non_existent_creds_file))   # Non-existent
#        
#    
#    def test_import_SAS(self):
#        print "6: test_import_SAS"
#        sas_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\testdata.sas7bdat" # Real = IsNotNone
#       
#        self.assertIsNotNone(cf.import_SAS(sas_file))                   # Real
#        self.assertFalse(cf.import_SAS(self.empty_creds_file))          # Empty
#        with self.assertRaises(TypeError):
#            cf.import_SAS(self.fake_creds_file)                         # Fake
#        self.assertFalse(cf.import_SAS(self.non_existent_creds_file))   # Non-existent
#        
#    
#    def test_create_table(self):
#        print "7: test_create_table"
#        self.assertTrue(cf.create_table(self.create_table_name, ("test1 varchar2(40)", "test2 number(4)", "test3 number(2)")))
#        self.assertFalse(cf.create_table(self.create_table_name, ("test1 varchar2(40)", "test2 number(4)", "test3 number(2)")))
#    
#
#    def test_check_table(self):
#        print "8: test_check_table"
#        self.assertTrue(cf.check_table(self.real_table_name))
#        self.assertFalse(cf.check_table(self.fake_table_name))            
#
#
#    def test_drop_table(self):
#        print "9: test_drop_table"
#        cf.create_table(self.create_table_name, ("test1 varchar2(40)", "test2 number(4)", "test3 number(2)"))        
#        self.assertTrue(cf.drop_table(self.create_table_name))
#        
#        self.assertFalse(cf.drop_table(self.fake_table_name))
#    
#    
#    def test_delete_from_table(self):
#        print "10: test_delete_from_table"
#        
#        cols = ("Test_Name_One varchar2(40)", "Test_Name_Two number(4)")
#        cf.create_table(self.empty_table_name, cols)
#        
#        self.assertTrue(cf.delete_from_table(self.real_table_name))
#        self.assertTrue(cf.delete_from_table(self.empty_table_name))
#        self.assertFalse(cf.delete_from_table(self.fake_table_name))
            
    
    def test_select_data(self):
        print "11: test_select_data"
        
        self.assertEqual(cf.select_data("DATA_SOURCE_ID"
                                        , "DATA_SOURCE"
                                        , "DATA_SOURCE_NAME"
                                        , "Sea"), "1")
        
        self.assertEqual(cf.select_data("DISPLAY_VALUE"
                                , "COLUMN_LOOKUP"
                                , "LOOKUP_KEY"
                                , "3"), "'Deleted'")
        
        # Incorrect column_name
        self.assertFalse(cf.select_data("x"
                                , "DATA_SOURCE"
                                , "DATA_SOURCE_NAME"
                                , "SEA"))
        
        # Incorrect table_name
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.select_data("DATA_SOURCE_ID"
                                , "x"
                                , "DATA_SOURCE_NAME"
                                , "SEA")
        
        # Incorrect condition1
        self.assertFalse(cf.select_data("DATA_SOURCE_ID"
                                , "DATA_SOURCE"
                                , "x"
                                , "SEA"))
        
    
#    def test_ips_error_check(self):
#        print "test_ips_error_check"
#        pass
#    
#    
#    def test_commit_ips_response(self):
#        print "test_commit_ips_response"
#        pass
#    
#    
#    def test_unload_parameters(self):
#        print "test_unload_parameters"
#        pass

    
    def tearDown(self):
        if cf.check_table(self.create_table_name) == True:
            cf.drop_table(self.create_table_name)
            
        if cf.check_table(self.real_table_name) == True:
            cf.delete_from_table(self.real_table_name)
        
        if cf.check_table(self.empty_table_name) == True:
            cf.drop_table(self.empty_table_name)
            
        print "\n"
        

if __name__ == '__main__':
    unittest.main()