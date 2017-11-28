'''
Created on 24 Nov 2017

@author: thorne1
'''

import os
import zipfile
import cx_Oracle
import re 

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
        
        # IPSCredentials file location
        credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredentials.txt"
        
        # Open and read file, and assign to string variable 
        file_object = open(credentials_file, "r")
        credentials_string = file_object.read()        
        
        # Create dictionary
        self.credentials_dict = {}
        
        # Parse string to dictionary
        for line in credentials_string.split('\n'):
            if not line: break
            pair = line.split(":")
            self.credentials_dict[pair[0].strip()] = pair[1].strip()
    
    
    def get_password(self, credentials_file):
        """
        Author :     thorne1
        Date :       27 Nov 2017
        Purpose :    Retrieves user password for database (Oracle)
                     Data currently retrieved from .txt file.  Process to be determined.
        Params :     
        """
        
        file = open(credentials_file, "r")
        credentials_string = file.read()        
        
        for line in credentials_string.split('\n'):
            if not line: break      # File is broken to two paragraphs - this only reads first paragraph.  Replace 'break' with 'continue' to read last paragraph 
            if re.match("PASSWORD", line.upper()) is not None:
                pwd = line.split()[-1] 

        return pwd
    
    
    def get_oracle_connection(self):
        """
        Author :    mahont1 & thorne1
        Date :      27 Nov 2017
        Purpose :   Connect to Oracle database and return cursor object
        Params : 
        REQUIREMENTS:   pip install cx_Oracle 
                        32-bit Oracle Client required
        """
        
        # Connect
        conn = cx_Oracle.connect(self.credentials_dict['User']
                                 , self.credentials_dict['Password']
                                 , self.credentials_dict['Database'])
        
        # Create and return cursor object
        cur = conn.cursor()
        
        return cur
            
    
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
        Purpose : Opens a CSV and returns a dataset   
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

        
        try:
            # Create sas7bdat dataframe:
            with SAS7BDAT(file_name) as file_object:
                return file_object.to_data_frame()
        except TypeError as err:
            print "%s is not a SAS file" %(file_name)
           

x = IPSCommonFunctions()