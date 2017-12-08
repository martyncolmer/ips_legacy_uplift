'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import os
import cx_Oracle

from IPSTransformation import CommonFunctions as cf


class ConvolutedTestWriteToLog(unittest.TestCase):
    def setUp(self):
        self.dave_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
        self.rich_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials2.txt"
        self.empty_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\EmptyIPSCredentials.txt"
        self.fake_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\FakeCredentials.txt"
        self.non_existent_creds_file = r"\\hello\this_is\a\fake\directory"
      
      
    def test_validate_file(self):
        self.assertTrue(cf.validate_file(self.dave_creds_file))               # Dave's creds
        self.assertTrue(cf.validate_file(self.rich_creds_file))               # Rich's creds file
        self.assertFalse(cf.validate_file(self.empty_creds_file))             # Empty creds file
        self.assertTrue(cf.validate_file(self.fake_creds_file))               # Fake creds file
        self.assertFalse(cf.validate_file(self.non_existent_creds_file))      # Non-existent creds file
            
    
    def test_oracle_connection(self):        
        self.assertIsNotNone(cf.get_oracle_connection(self.dave_creds_file))     # Dave's creds file
        self.assertIsNotNone(cf.get_oracle_connection(self.rich_creds_file))     # Rich's creds file        
        with self.assertRaises(TypeError):
            cf.get_oracle_connection(self.empty_creds_file)                      # Empty creds file
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.get_oracle_connection(self.fake_creds_file)                       # Fake creds file
        with self.assertRaises(TypeError):
            cf.get_oracle_connection(self.non_existent_creds_file)               # Non-existent creds file
    
    
    def test_get_credentials(self):
        # assertDictContainsSubset did pass for Rich and Dave's credentials however could not upload this code to GitLab
        self.assertIsNotNone(cf.get_credentials(self.dave_creds_file))      # Dave's creds file        
        self.assertIsNotNone(cf.get_credentials(self.rich_creds_file))      # Rich's creds file        
        self.assertFalse(cf.get_credentials(self.empty_creds_file))         # Empty creds file
        self.assertIsNotNone(cf.get_credentials(self.fake_creds_file))      # Fake creds file
        self.assertFalse(cf.get_credentials(self.non_existent_creds_file))  # Non-existent creds file
       
        
    def test_extract_zip_true(self):
        filename = "testdata.zip"
        correct_dir = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
        empty_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Empty Folder"
        no_zip_values = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\crossingfactor"
        non_existent_dir = r"\\hello\this_is\a\fake\directory"
        
        self.assertTrue(cf.extract_zip(correct_dir, filename))       # Real values
        self.assertFalse(cf.extract_zip(empty_file, filename))       # Empty file
        self.assertFalse(cf.extract_zip(no_zip_values, filename))    # No zip values
        self.assertFalse(cf.extract_zip(non_existent_dir, filename)) # Non-existent file
        

    def test_import_csv(self):
        real_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
                
        self.assertIsNotNone(cf.import_csv(real_filename))              # Real
        self.assertFalse(cf.import_csv(self.empty_creds_file))          # Empty
        self.assertFalse(cf.import_csv(self.non_existent_creds_file))   # Non-existent
        
    
    def test_import_SAS(self):
        sas_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\testdata.sas7bdat" # Real = IsNotNone
       
        self.assertIsNotNone(cf.import_SAS(sas_file))                   # Real
        self.assertFalse(cf.import_SAS(self.empty_creds_file))          # Empty
        with self.assertRaises(TypeError):
            cf.import_SAS(self.fake_creds_file)                         # Fake
        self.assertFalse(cf.import_SAS(self.non_existent_creds_file))   # Non-existent
        

#    @unittest.expectedFailure
#    def test_import_traffic_data_false(self):
#        """
#        Currently returns false because data has not been validated
#        Once data is validated, switch off @unittest.expectedFailure
#        """
#        caa_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017.csv"
#        non_response = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response Q1 2017.csv"
#        possible_shifts = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts Q1 2017.csv"
#        sea_traffic = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
#        tunnel_traffic = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"
#        unsampled_traffic = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv"
#        
#        
#        self.assertTrue(cf.import_traffic_data(sea_traffic))
#        self.assertTrue(cf.import_traffic_data(non_response))
#        self.assertTrue(cf.import_traffic_data(tunnel_traffic))


#    def test_get_survey_type(self):
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017.csv"
#        self.assertEqual(cf.get_survey_type(filename), "CAA")
#                
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response QA 2017.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Non Response")
#
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response QA 2017_migRoute_NormalRoute.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Non Response")
#        
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts QA 2017.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Possible")
#        
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Sea")
#        
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Sea")
#        
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Tunnel")
#        
#        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv"
#        self.assertEqual(cf.get_survey_type(filename), "Unsampled")
#        
#        filename = r"\\hello\this_is\a\fake\directory"
#        self.assertEqual(cf.get_survey_type(filename), "directory")


if __name__ == '__main__':
    unittest.main()