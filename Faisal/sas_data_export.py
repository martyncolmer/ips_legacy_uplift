from IPSTransformation import CommonFunctions
import numpy as np
from sas7bdat import SAS7BDAT
import os
from collections import OrderedDict
import datetime
import cx_Oracle
 

def sas_data_export():
    
    path_to_export_data = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\export_data\export_data_out.sas7bdat"
    
    filename, ext = os.path.splitext(path_to_export_data)
    # check file format
    if (ext.lower() != ".sas7bdat" and ext.lower() != ".csv"):
        raise Exception("Abort: File format not supported")
     
    # open and read file as bytes
    content = ''
    with open (path_to_export_data,'rb') as file:
        content = file.read()

    conn = CommonFunctions.get_oracle_connection(credentials_file = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\IPSCredens.json")
    db = conn.cursor()
    db.setinputsizes (blobData = cx_Oracle.BLOB)
    
    sqlStr = "insert into  sas_data_export VALUES(9992,'subsample', :blobData)"
    db.execute (sqlStr, blobData= content)
    db.execute ('commit')
     
    # # close the Oracle database connection
    db.close()
    