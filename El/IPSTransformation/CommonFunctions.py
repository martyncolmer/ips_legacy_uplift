'''
Created on 24 Nov 2017

@author: thorne1
'''

import os
import zipfile
import cx_Oracle    # pip install this
import pandas as pandas     # pip install this
import logging
import datetime
import traceback

from sas7bdat import SAS7BDAT   # pip install this

class IPSCommonFunctions():    
    def get_credentials(self):
        """
        Author :     thorne1
        Date :       27 Nov 2017
        Purpose :    Retrieves credentials from local text file
        Returns :    Dictionary        
        """
        
        # IPSCredentials file location
        credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\PretendIPSCredentials.txt"
        # credentials_file = r"PAH!"
        
        try:
            # Open and read file, and assign to string variable 
            file_object = open(credentials_file, "r")
            credentials_string = file_object.read()
        except IOError as err:
            self.commit_ips_response(0, err)
            return False
        
        try:
            # Create dictionary
            credentials_dict = {}
            # Parse string to dictionary
            for line in credentials_string.split('\n'):
                pair = line.split(":")
                credentials_dict[pair[0].strip()] = pair[1].strip()
        except IndexError as err:
            self.commit_ips_response(0, err)
        else:
            return credentials_dict


    def get_oracle_connection(self):
        """
        Author :    mahont1 & thorne1
        Date :      27 Nov 2017
        Purpose :   Connect to Oracle database and return cursor object
        Returns :    CONNECTION (Object) 
                (cannot return cursor object as DDL statements are implicitly committed
                whereas DML statements are not)
        REQUIREMENTS:   pip install cx_Oracle 
                    32-bit Oracle Client required
        """
        
        # Retrieve credentials dictionary 
        creds = self.get_credentials()
        
        # Connect
        try:
            return cx_Oracle.connect(creds['User']
                                     , creds['Password']
                                     , creds['Database'])
        except TypeError as err:
            self.commit_ips_response(0, err)
            
            


    def get_password(self):
        """
        Author :     thorne1
        Date :       27 Nov 2017
        Purpose :    Retrieves user password for database (Oracle)
                 Data currently retrieved from .txt file.  Process to be determined.
        Returns :    Password (String)
        """
        
        pwd = self.get_credentials()
        return pwd['Password']


    def extract_zip(self, dir_name, file_extension=""):
        """
        Author     : thorne1
        Date       : 24 Nov 2017
        Purpose    : Extracts either a specific file from zip, or entire file
        Params     : dir_name        =    directory containing .zip file
                 file_extension     =    Specify a file type to extract one file 
                                         (assuming there is only one file type in zip)
                                         or leave empty to extract all
        Returns    : True/False
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
            # Failed to find zip and return False to indicate failure
            raise
            print "IOError: %s does not exist." % (file_name)            
            return False
        else:
            # If file_extension not specified, extract everything
            if file_extension == "":
                zip_file.extractall(dir_name)
                print "File successfully exported to: %s" % (dir_name)
                # Return True to indicate success
                return True
            else:
                # Find and extract a specified file
                for each_file in zip_file.namelist():
                    if each_file.endswith(file_extension):
                        zip_file.extract(each_file, dir_name)
                        print "File successfully exported to: %s" % (dir_name)
                        # Return True to indicate success
                        return True
        # Clean up
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        zip_file.close()        


    def import_CSV(self, file_name):
        """
        Author : thorne1
        Date : 27 Nov 2017
        Purpose : Opens a CSV and returns a dataset   
        Params : file_name    =    directory path to CSV
        Returns : Dataframe (object) or False
        https://chrisalbon.com/python/pandas_dataframe_importing_csv.html
        """
        try:
            dataframe = pandas.read_csv(file_name)
        except IOError:
            # File not found, return False to indicate failure
            raise            
            print "IOError: %s does not exist." % (file_name)
            return False 
        else:           
            return dataframe
    
    
    def import_SAS(self, file_name):
        """
        Author     : thorne1
        Date       : 23 Nov 2017
        Purpose    : Opens and reads a SAS dataset
        Returns    : SAS File (object) or False   
        https://pypi.python.org/pypi/sas7bdat    
        """
        
        try:
            # Create and return sas7bdat dataframe:
            with SAS7BDAT(file_name) as file_object:
                return file_object
        except TypeError:
            # File not found, return False to indicate failure
            raise
            print "%s is not a SAS file" % (file_name)
        return False
    

    def commit_ips_response(self, level, err):
        """
        Author : thorne1
        Date : 29 Nov 2017
        Purpose : Writes response code and warnings to response table   
        """
        
        # Database variables
        # Level will need to be plugged in from error_check()...?
        time_now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")
        level_dict = {50: "CRITICAL"
              , 40: "ERROR"
              , 30: "WARNING"
              , 20: "INFO"
              , 10: "DEBUG"
              , 0: "NOTSET"}             
        
        response_message = "%s   |   %s: %s" %(time_now
                                                   , level_dict[level]
                                                   , err)        
        print response_message
        

x = IPSCommonFunctions()
x.get_oracle_connection()