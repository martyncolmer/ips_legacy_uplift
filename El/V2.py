'''
Created on 24 Nov 2017

@author: thorne1
'''

import os
import zipfile
import cx_Oracle

import pandas as pandas

from sas7bdat import SAS7BDAT

class IPSCommonFunctions():
    
    def __init__(self):
        """
        Author :     thorne1
        Date :       27 Nov 2017
        Purpose :    Cannot upload credentials to GitLab.  __init__ to initialise variables into
                     dictionary from local file
        Params :    
        """
        
        credentials_file = "P:\\SocialSurveySupport\\IPSCredentials.txt"
        
        file = open(credentials_file, "r")
        credentials_string = file.read()        
        
        self.credentials_dict = {}
        for line in credentials_string.split('\n'):
            if not line: continue
            pair = line.split(":")
            self.credentials_dict[pair[0].strip()] = pair[1].strip() 
            
    
    def extract_zip(self, dir_name, file_extension = ""):
        """
        Author     : thorne1
        Date       : 24 Nov 2017
        Purpose    : Extracts either a specific file from zip, or entire file
        Params     : dir_name        =    directory containing .zip file
                     file_extension     =    Specify a file type to extract one file 
                                             (assuming there is only one file type in zip)
                                             or leave empty to extract all
        """
        
        # Change directory from working directory to directory with files
        os.chdir(dir_name)
                                
        try:
            # Find and create zipfile object
            for item in os.listdir(dir_name):
                if item.endswith(".zip"):
                    file_name = os.path.abspath(item)
                    zip_file = zipfile.ZipFile(file_name)
        except IOError:
            # Failed to find zip
            print "IOError: %s does not exist." %(file_name)
            raise
            # Return False to indicate failure
            return False
        else:
            # If file_extension not specified, extract everything
            if file_extension == "":
                zip_file.extractall(dir_name)
                print "File successfully exported to: %s" %(dir_name)
            else:
                # Find and extract a specified file
                for each_file in zip_file.namelist():
                    if each_file.endswith(file_extension):
                        zip_file.extract(each_file, dir_name)
                        print "File successfully exported to: %s" %(dir_name)
            # Clean up
            os.chdir(os.path.dirname(os.path.realpath(__file__)))
            zip_file.close()
        
        # Return True to indicate success
        return True
    
    
    def import_CSV(self, file_name):
        """
        Author : thorne1
        Date : 27 Nov 2017
        Purpose : Opens and reads a CSV dataset   
        Params : file_name    =    directory path to CSV
        https://chrisalbon.com/python/pandas_dataframe_importing_csv.html
        """
        try:
            dataframe = pandas.read_csv(file_name)
        except IOError:
            print "IOError: %s does not exist." %(file_name)
            raise
            # Return False to indicate failure
            return False 
        else:           
            return dataframe    
    
    
    def import_SAS(self, file_name):
        """
        Author     : thorne1
        Date       : 23 Nov 2017
        Purpose    : Opens and reads a SAS dataset   
        https://pypi.python.org/pypi/sas7bdat    
        """

        # Create sas7bdat dataframe:
        with SAS7BDAT(file_name) as file_object:
            dataframe = file_object.to_data_frame()
        
        print dataframe
        
    
    def transpose(self):
        # Tom's unpivot()
        pass
    
    
    def get_oracle_connection(self):
        """
        Author :    mahont
        Date :      27 Nov 2017
        Purpose :   Connect to Oracle database and return cursor object
        Params : 
        REQUIREMENTS:   pip install cx_Oracle 
                        32-bit Oracle Client required
        """
        
        conn = cx_Oracle.connect(self.credentials_dict['User']
                                 , self.credentials_dict['Password']
                                 , self.credentials_dict['Database'])
        cur = conn.cursor()

        return cur

    
    def oralib_access(self, schema, dbase, max_text):
        """
        Author :     thorne1
        Date :       27 Nov 2017
        Purpose :    Uses SAS Access to Oracle to assign an Oracle schema, 
                     using the supplied schema (password is retrieved by function get_password.
        Params :     schema    -    The name of the Oracle schema to connect to.
                     dbase     -    The name of the Oracle databse the schema is located in.
                     max_text  -    Allows access to greater than default size for CLOB (if blank defaults to quite small value).
        """
        
        pwd = self.get_password()
    
    
    def get_password(self, credentials_file):
        """
        Author :     thorne1
        Date :       27 Nov 2017
        Purpose :    Retrieves user password for database (Oracle)
        Params :     
        """
        
#        file = open(credentials_file, "r")
#        credentials_string = file.read()        
#        
#        self.credentials_dict = {}
#        for line in credentials_string.split('\n'):
#            if not line: continue
#            pair = line.split(":")
#            self.credentials_dict[pair[0].strip()] = pair[1].strip() 
#        return self.credentials_dict
        pass
        
    
    
    def error_check(self):
        pass
    

IPSCommonFunctions = IPSCommonFunctions()

# extract_zip
#dir_name = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift"
#IPSCommonFunctions.extract_zip(dir_name)

## import sas
file_name = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift\\testdata.sas7bdat"
print IPSCommonFunctions.import_SAS(file_name)

# import CSV
#dir_path = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift\\Sea Traffic Q1 2017.csv"
#print IPSCommonFunctions.import_CSV(dir_path)




#target_file = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift\\Test Target"
## file_extension = ".sas7bdat"
#IPSCommonFunctions.extract_zip(sas_zip, target_file)

# establish connections with oracle
#IPSCommonFunctions.get_oracle_connection()

# get password and credentials
#credentials_file = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift\\Credentials.txt"
#print IPSCommonFunctions.get_password(credentials_file)

#print IPSCommonFunctions.get_oracle_connection()