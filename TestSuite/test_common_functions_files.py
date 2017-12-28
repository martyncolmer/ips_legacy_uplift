'''
Created on 5 Dec 2017

@author: thorne1
'''
import unittest
import os
import sys

from IPSTransformation import CommonFunctions as cf

class TestCommonFunctions(unittest.TestCase):
    def test_extract_zip_true(self):
#        print "4: test_extract_zip_true"
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
#        print "5: test_import_csv"
        
        # File locations
        real_filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv" # Real
        empty_csv_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\EmptyFile.csv"            # Empty
        fake_csv_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\FakeFile.csv"              # Fake
        non_existent_csv_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\NEFile.csv"        # Non-existent
        
        # Create empty and fake CSV files
        empty_file = open(empty_csv_file, "w")
        empty_file.close()
        
        fake_file = open(fake_csv_file, "w")
        fake_file.write("Hello world")
        fake_file.close()
                
        # Run tests
        self.assertIsNotNone(cf.import_csv(real_filename))       # Real
        self.assertFalse(cf.import_csv(empty_csv_file))          # Empty
        self.assertFalse(cf.import_csv(fake_csv_file))           # Fake
        self.assertFalse(cf.import_csv(non_existent_csv_file))   # Non-existent
        
        # Remove unwanted CSV files
        os.remove(empty_csv_file)
        os.remove(fake_csv_file)
        
        
    
    def test_import_SAS(self):
#        print "6: test_import_SAS"
        
        # File locations
        real_sas_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\testdata.sas7bdat" # Real = IsNotNone
        empty_sas_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\EmptyFile.sas"            # Empty
        non_existent_sas_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\NEFile.sas"        # Non-existent
        
        # Create empty and fake SAS files
        empty_file = open(empty_sas_file, "w")
       
        # Run tests
        self.assertIsNotNone(cf.import_SAS(real_sas_file))         # Real
        self.assertFalse(cf.import_SAS(empty_sas_file))            # Empty
        self.assertFalse(cf.import_SAS(non_existent_sas_file))     # Non-existent
        
        # Remove unwanted SAS files
        empty_file.close()
        
        os.remove(empty_sas_file)

                

if __name__ == '__main__':
    unittest.main()