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
        # Real, empty, fake and non-existent file locations
        self.dave_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
        self.rich_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials2.txt"
        self.empty_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\EmptyIPSCredentials.txt"
        self.fake_creds_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\FakeCredentials.txt"
        self.non_existent_creds_file = r"\\hello\this_is\a\fake\directory"
        
        # Create empty and fake creds file
        empty_creds_file = open(self.empty_creds_file, "w") 
        empty_creds_file.close()
        
        fake_creds_file = open(self.fake_creds_file, "w")
        fake_creds_file.write("User: Fake\n")
        fake_creds_file.write("Password: Fake\n")
        fake_creds_file.write("Database: Fake")        
        fake_creds_file.close()
                                  
      
    def test_validate_file(self):
        self.assertTrue(cf.validate_file(self.dave_creds_file))               # Dave's real creds file
        self.assertTrue(cf.validate_file(self.rich_creds_file))               # Rich's real creds file
        self.assertFalse(cf.validate_file(self.empty_creds_file))             # Empty creds file
        self.assertTrue(cf.validate_file(self.fake_creds_file))               # Fake creds file
        self.assertFalse(cf.validate_file(self.non_existent_creds_file))      # Non-existent creds file
        

    def test_oracle_connection(self): 
        self.assertIsNotNone(cf.get_oracle_connection(self.dave_creds_file))     # Dave's real creds file
        self.assertIsNotNone(cf.get_oracle_connection(self.rich_creds_file))     # Rich's real creds file        
        with self.assertRaises(TypeError):
            cf.get_oracle_connection(self.empty_creds_file)                      # Empty creds file
        with self.assertRaises(cx_Oracle.DatabaseError):
            cf.get_oracle_connection(self.fake_creds_file)                       # Fake creds file
        with self.assertRaises(TypeError):
            cf.get_oracle_connection(self.non_existent_creds_file)               # Non-existent creds file
    
    
    def test_get_credentials(self):
        self.assertIsNotNone(cf.get_credentials(self.dave_creds_file))      # Dave's real creds file        
        self.assertIsNotNone(cf.get_credentials(self.rich_creds_file))      # Rich's real creds file        
        self.assertFalse(cf.get_credentials(self.empty_creds_file))         # Empty creds file
        self.assertIsNotNone(cf.get_credentials(self.fake_creds_file))      # Fake creds file
        self.assertFalse(cf.get_credentials(self.non_existent_creds_file))  # Non-existent creds file

                
    def tearDown(self):
        # tear down all non-required creds files
        if os.path.exists(self.empty_creds_file):
            os.remove(self.empty_creds_file)
        
        if os.path.exists(self.fake_creds_file):
            os.remove(self.fake_creds_file)


if __name__ == '__main__':
    unittest.main()