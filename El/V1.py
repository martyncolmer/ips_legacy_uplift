'''
Created on 23 Nov 2017

@author: thorne1
'''

# import pandas as pandas
import os
import zipfile

from os import walk

class IPSCommonFunctions():
    """
    Author     : thorne1
    Date       : 23 Nov 2017
    Purpose    : Reads a SAS dataset, transposes and appends the data to an Oracle table     
    """
    
    def __init__(self):
        """
        Author     : thorne1
        Date       : 23 Nov 2017
        Purpose    : Initialise variables    
        """
               
        # Currently hard-coding variables
        # Set up directories        
        self.source_folder = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift\\LegacyUplift.zip"
        self.target_folder = "H:\\My Documents\\Documents\\Git Repo\\Misc and Admin\\LegacyUplift\\Target Archive"
        
        self.file_extension = ".txt"     # Use file_extension to specify your file
        
        # Call function to unzip source file 
        self.unzip_file()
                       
                    
    def unzip_file(self):
        """
        Author : thorne1
        Date : 24 Nov 2017
        Purpose : Extract file from zip in to target dir 
        """
        
        zip_folder = zipfile.ZipFile(self.source_folder)
                
        for each_file in zip_folder.namelist():
            # find and extract your specified file
            if each_file.endswith(self.file_extension):
                zip_folder.extract(each_file, self.target_folder)
                
        
    def read_dataset(self):
        #===============================================================================
        # #        Pandas
        #===============================================================================
        
        # data = pandas.read_sas(self.source_file_name)
        # pandas.DataFrame.to_csv(data, self.target_file_name)
        """
        Author     : thorne1
        Date       : 23 Nov 2017
        Purpose    : Opens and reads a SAS dataset       
        """
        
        # Assign local variables
        all_files = []
        delimiter = "\n"
        
        # Extract list of all_files from folder 
        for(dirpath, dirnames, filenames) in walk(self.target_folder):
            all_files.extend(filenames)
            break
        
        # Load specified file
        for each_file in all_files:
            if each_file.endswith(self.file_extension):
                target_file = open(os.path.join(self.target_folder, each_file), "r")
        
        # Pass delimiter and file to be validated
        output = self.validate(delimiter, target_file.read()) 
        return output
    
    
    def validate(self, delimiter, file_object):
        """
        Author : thorne1
        Date : 23 Nov 2017
        Purpose : Validates data and removes delimiter     
        Params :    
        """
        
        new_string = file_object
                
        for delimiter in delimiter:
            new_string = new_string.replace(delimiter, " ",)
        return ' '.join(new_string.split())
           

IPSCommonFunctions = IPSCommonFunctions()
# IPSCommonFunctions.unzip_file()
IPSCommonFunctions.unzip_file()
    
    