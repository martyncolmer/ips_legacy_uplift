'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
from IPSTransformation.CommonFunctions import IPSCommonFunctions
 

class TestWriteToLog(unittest.TestCase):  
    def setUp(self):
        self.IPS = IPSCommonFunctions()

                  
    def test_get_credentials(self):
        self.assertDictContainsSubset({'User': 'IPS_1_POWELD2_DATA', 'Password': 'IPS_1_POWELD2', 'Database': 'DEVCON'}
                                      , self.IPS.get_credentials())
    
    
    @unittest.expectedFailure
    def test_get_credentials_raises_ioerror(self):
        with self.assertRaises(IOError):
            self.IPS.get_credentials()
            
    
    def test_oracle_connection(self):
        self.assertIsNotNone(self.IPS.get_oracle_connection())
    
    
    @unittest.expectedFailure
    def test_oracle_connection_raises_error(self):
        with self.assertRaises(cx_Oracle.DatabaseError):
            self.IPS.get_oracle_connection()
           
    
    def test_get_password(self):
        self.assertEqual('IPS_1_POWELD2', self.IPS.get_password())     
    
    
    def test_extract_zip_true(self):
        dir_name = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
        file_extension = ".sas7bdat"
        
        # Extract everything
        self.assertTrue(self.IPS.extract_zip(dir_name))
    
        # Extract .sas7bdat
        self.assertTrue(self.IPS.extract_zip(dir_name, file_extension))
    
    
    @unittest.expectedFailure
    def test_extract_zip_false(self):
        dir_name = r"\\hello\this_is\a\fake\directory"
        file_extension = ".txt"
        
        # Extract everything
        self.assertFalse(self.IPS.extract_zip(dir_name))
        
        # Extract .txt
        self.assertFalse(self.IPS.extract_zip(dir_name, file_extension))
        
        
    def test_extract_zip_raises_windowserror(self):
        dir_name = r"\\hello\this_is\a\fake\directory"
        with self.assertRaises(WindowsError):
            self.IPS.extract_zip(dir_name)    
        

    def test_import_traffic_data(self):
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
        self.assertTrue(self.IPS.import_traffic_data(filename))
        
        
    @unittest.expectedFailure
    def test_import_traffic_data_false(self):
        """
        Currently returns false because data has not been validated
        Once data is validated, switch off @unittest.expectedFailure
        """
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
        self.assertTrue(self.IPS.import_traffic_data(filename))
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response Q1 2017.csv"
        self.assertTrue(self.IPS.import_traffic_data(filename))
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"
        self.assertTrue(self.IPS.import_traffic_data(filename))


    def test_get_survey_type(self):
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\CAA Q1 2017.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "CAA")
                
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response QA 2017.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Non Response")

        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Non Response QA 2017_migRoute_NormalRoute.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Non Response")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Possible shifts QA 2017.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Possible")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017 - Copy.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Sea")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Sea")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Tunnel Traffic Q1 2017.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Tunnel")
        
        filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Unsampled Traffic Q1 2017.csv"
        self.assertEqual(self.IPS.get_survey_type(filename), "Unsampled")


if __name__ == '__main__':
    unittest.main()