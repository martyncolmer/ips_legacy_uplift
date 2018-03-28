'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import os

from main.io import CommonFunctions as cf

class TestCommonFunctions(unittest.TestCase):
    def test_extract_zip_true(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.extract_zip()
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : None
        """
        
        root_dir_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
        filename = "testdata.zip"
        empty_file = root_dir_path + "\Empty Folder"
        no_zip_values = root_dir_path + "\crossingfactor"
        non_existent_dir = r"\\hello\this_is\a\fake\directory"
        
        # Real, empty, no zip and non-existen values
        self.assertTrue(cf.extract_zip(root_dir_path, filename))     
        self.assertFalse(cf.extract_zip(empty_file, filename))       
        self.assertFalse(cf.extract_zip(no_zip_values, filename))    
        self.assertFalse(cf.extract_zip(non_existent_dir, filename)) 
        

    def test_import_csv(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.import_csv()
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : None
        """
        
        # File locations: Real, empty, fake and non-existent files
        root_dir_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
        real_filename = root_dir_path + "\Sea Traffic Q1 2017.csv" 
        empty_csv_file = root_dir_path + "\EmptyFile.csv"            
        fake_csv_file = root_dir_path + "\FakeFile.csv"              
        non_existent_csv_file = root_dir_path + "\NEFile.csv"        
        
        # Create empty and fake CSV files
        empty_file = open(empty_csv_file, "w")
        empty_file.close()
        
        fake_file = open(fake_csv_file, "w")
        fake_file.write("Hello world")
        fake_file.close()
                
        # Run tests with real, empty, fake and non-existent files
        self.assertIsNotNone(cf.import_csv(real_filename))       
        self.assertFalse(cf.import_csv(empty_csv_file))          
        self.assertFalse(cf.import_csv(fake_csv_file))           
        self.assertFalse(cf.import_csv(non_existent_csv_file))   
        
        # Remove unwanted CSV files
        os.remove(empty_csv_file)
        os.remove(fake_csv_file)
        
        
    def test_import_sas(self):
        """
        Author          : thorne1
        Date            : 4 Jan 2018
        Purpose         : Tests CommonFunctions.import_sas()
        Paramaters      : None
        Returns         : None
        Requirements    : None
        Dependencies    : None
        """
        
        # File locations: Real, empty, fake and non-existent files
        root_dir_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
        real_sas_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\testdata.sas7bdat" 
        empty_sas_file = root_dir_path + "\EmptyFile.sas"            
        non_existent_sas_file = root_dir_path + "\NEFile.sas"        
        
        # Create empty and fake SAS files
        empty_file = open(empty_sas_file, "w")
       
        # Run tests with real, empty, fake and non-existent files
        self.assertIsNotNone(cf.import_sas(real_sas_file))         
        self.assertFalse(cf.import_sas(empty_sas_file))            
        self.assertFalse(cf.import_sas(non_existent_sas_file))     
        
        # Remove unwanted SAS files
        empty_file.close()
        os.remove(empty_sas_file)

                
if __name__ == '__main__':
    unittest.main()