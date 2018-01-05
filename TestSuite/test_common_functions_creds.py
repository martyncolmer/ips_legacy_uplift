'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import os
import json

from IPSTransformation import CommonFunctions as cf

import survey_support as ss

class TestCommonFunctions(unittest.TestCase):
    def setUp(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Creates global variables for file locations and
                        : creates test files as applicable
        Paramaters      : None
        Returns         : None
        Requirements    : survey_support
        Dependencies    : survey_support.encrypt_keyvalue_to_json()
        """
        # Real, empty, fake and non-existent file locations
        root_dir_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS"
        self.dave_creds_file = root_dir_path + "\IPSCredentials.json"
        self.rich_creds_file = root_dir_path + "\IPSCredentials2.json"
        self.empty_creds_file = root_dir_path + "\EmptyIPSCredentials.json"
        self.fake_creds_file = root_dir_path + "\FakeCredentials.json"
        self.non_existent_creds_file = r"\\hello\this_is\a\fake\directory"
        
        # Create empty and fake json creds file
        empty_data = {}        
        with open(self.empty_creds_file, "w") as empty_creds_file:
            json.dump(empty_data, empty_creds_file)
        empty_creds_file.close()
        
        # Encrypt values within fake_creds_file
        ss.write_keyvalue_to_json("User", "Fake", self.fake_creds_file, True)
        ss.write_keyvalue_to_json("Password", "Fake", self.fake_creds_file, True)
        ss.write_keyvalue_to_json("Database", "Fake", self.fake_creds_file)
                                  
      
    def test_validate_file(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.validate_file()
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.validate_file()
        """
        # Run tests with real, empty, fake and non-existent files
        self.assertTrue(cf.validate_file(self.dave_creds_file))
        self.assertTrue(cf.validate_file(self.rich_creds_file))
        self.assertTrue(cf.validate_file(self.empty_creds_file))
        self.assertTrue(cf.validate_file(self.fake_creds_file))
        self.assertFalse(cf.validate_file(self.non_existent_creds_file))    
        
    
    def test_get_credentials(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.get_credentials()    
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.get_credentials()
        """
        
        # Run tests with real, empty, fake and non-existent files
        self.assertIsNotNone(cf.get_credentials(self.dave_creds_file))      
        self.assertIsNotNone(cf.get_credentials(self.rich_creds_file))      
        self.assertFalse(cf.get_credentials(self.empty_creds_file))         
        self.assertIsNotNone(cf.get_credentials(self.fake_creds_file))      
        self.assertFalse(cf.get_credentials(self.non_existent_creds_file))  
        

    def test_oracle_connection(self): 
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.oracle_connection()
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.oracle_connection()
        """
        
        # Run tests with real, empty, fake and non-existent files
        self.assertIsNotNone(cf.get_oracle_connection(self.dave_creds_file))     
        self.assertIsNotNone(cf.get_oracle_connection(self.rich_creds_file))     
        self.assertFalse(cf.get_oracle_connection(self.fake_creds_file))         
        self.assertFalse(cf.get_oracle_connection(self.non_existent_creds_file)) 
    
    
    def test_oracle_connection_with_survey_support(self): 
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.oracle_connection() 
                        : which is using another function which is failing.
                        : Once MC has implemented validation check to 
                        : survey_support, '@unittest.expectedFailure' can be removed    
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : CommonFunctions.oracle_connection()
        """
        # Empty creds file
        self.assertFalse(cf.get_oracle_connection(self.empty_creds_file))          
  
    
    def tearDown(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Removes test files as applicable
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : None
        """
        # tear down all non-required creds files
        if os.path.exists(self.empty_creds_file):
            os.remove(self.empty_creds_file)
        
        if os.path.exists(self.fake_creds_file):
            os.remove(self.fake_creds_file)


if __name__ == '__main__':
    unittest.main()