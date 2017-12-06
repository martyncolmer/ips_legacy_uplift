'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import os
import cx_Oracle
from IPSTransformation import CommonFunctions as cf


class TestWriteToLog(unittest.TestCase):  
    """ Test with and without empty credentials file """
    def test_get_credentials(self):
        credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
        
        # If credentials_file is empty, dictionary should be correct
        if os.path.getsize(credentials_file) != 0:
            self.assertDictContainsSubset({'User': 'IPS_1_POWELD2_DATA', 'Password': 'IPS_1_POWELD2', 'Database': 'DEVCON'}
                                          , cf.get_credentials())
        # Else will return False
        else:
            self.assertFalse(cf.get_credentials())    

            
    """ Test will skip if credentials_file is empty """
    @unittest.skipIf(os.path.getsize(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt") == 0, "skipperdy")
    def test_oracle_connection(self):
        self.assertIsNotNone(cf.get_oracle_connection())
    
    
    """ Test will skip if credentials_file is NOT empty """
    @unittest.skipIf(os.path.getsize(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt") != 0, "skipperdy")
    def test_oracle_connection_raises_error(self):
        self.assertFalse(cf.get_oracle_connection())
           
    
    """ Test will skip if credentials_file is empty """
    @unittest.skipIf(os.path.getsize(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt") == 0, "skipperdy")
    def test_get_password(self):
        self.assertEqual('IPS_1_POWELD2', cf.get_password())     
    
    
    def test_extract_zip_true(self):
        # Real values
        dir_name = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
        file_extension = ".sas7bdat"        
        self.assertTrue(cf.extract_zip(dir_name))                   # Extract everything
        self.assertTrue(cf.extract_zip(dir_name, file_extension))   # Extract .sas7bdat
        
        # Fake values
        dir_name = r"\\hello\this_is\a\fake\directory"
        file_extension = ".txt"
        with self.assertRaises(WindowsError):
            cf.extract_zip(dir_name)                  # Extract everything
        with self.assertRaises(WindowsError):
            cf.extract_zip(dir_name, file_extension) 
        

    """ Test will skip if credentials_file is empty """
    @unittest.skipIf(os.path.getsize(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt") == 0, "skipperdy")
    def test_import_traffic_data(self):
        # Real values
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
        self.assertTrue(cf.import_traffic_data(filename))
        
        # Wrong file type
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\IPSCredentials.txt"
        with self.assertRaises(KeyError):
            cf.import_traffic_data(filename)
        
        # Fake values
        filename = r"\\hello\this_is\a\fake\directory"
        with self.assertRaises(IOError):
            cf.import_traffic_data(filename)
            
        filename = ""
        with self.assertRaises(IOError):
            cf.import_traffic_data(filename)

        
    @unittest.expectedFailure
    def test_import_traffic_data_false(self):
        """
        Currently returns false because data has not been validated
        Once data is validated, switch off @unittest.expectedFailure
        """
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
        self.assertTrue(cf.import_traffic_data(filename))
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response Q1 2017.csv"
        self.assertTrue(cf.import_traffic_data(filename))
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"
        self.assertTrue(cf.import_traffic_data(filename))


    def test_get_survey_type(self):
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017.csv"
        self.assertEqual(cf.get_survey_type(filename), "CAA")
                
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response QA 2017.csv"
        self.assertEqual(cf.get_survey_type(filename), "Non Response")

        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response QA 2017_migRoute_NormalRoute.csv"
        self.assertEqual(cf.get_survey_type(filename), "Non Response")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts QA 2017.csv"
        self.assertEqual(cf.get_survey_type(filename), "Possible")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
        self.assertEqual(cf.get_survey_type(filename), "Sea")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
        self.assertEqual(cf.get_survey_type(filename), "Sea")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"
        self.assertEqual(cf.get_survey_type(filename), "Tunnel")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv"
        self.assertEqual(cf.get_survey_type(filename), "Unsampled")
        
        filename = r"\\hello\this_is\a\fake\directory"
        self.assertEqual(cf.get_survey_type(filename), "directory")


    def test_import_SAS(self):
        self.assertIsNotNone(cf.import_SAS(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\shiftsdata.sas7bdat"))
        
        with self.assertRaises(TypeError):
            cf.import_SAS(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt")
        
        with self.assertRaises(IOError):
            cf.import_SAS(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\quarter12017.zip\quarter12017.sas7bdat")
        


if __name__ == '__main__':
    unittest.main()