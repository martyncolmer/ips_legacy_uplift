from sas7bdat import SAS7BDAT
from main.io import CommonFunctions as cf
import pandas as pd
import pandas.util.testing as tm; tm.N = 3
import numpy as np   
import sys

   
def get_sas_column_properties(sasFile):
    
    global numbers
    global versionid
    global types
    global lengths
    global formats
    global labels
    
    columnProperties = [['Num', 'Name', 'Type', 'Length', 'Format', 'Label']]
    
    for i, col in enumerate(sasFile.columns, 1):
        tmp = [i, col.name, col.type, col.length,col.format, col.label]
        columnProperties.append(tmp)
    
    for x in columnProperties:
        numbers.append(x[0])
        types.append(x[2])
        lengths.append(x[3])
        formats.append(x[4])
        labels.append(x[5])
    
    numbers.pop(0)
    vid = 9999
    versionid = [float(vid)]* len(numbers)
    types.pop(0)
    lengths.pop(0)
    formats.pop(0)
    labels.pop(0)
    return   
    

def process_column_dataframe(df,conn):
    
    # Generate the survey_column data from the generated column properties
    data = {
            'VERSION_ID'    : np.asarray(versionid).repeat(1),
            'COLUMN_NO'     : np.asarray(numbers).repeat(1),
            'COLUMN_DESC'   : np.asarray(df.columns).repeat(1),
            'COLUMN_LENGTH' : np.asarray(lengths).repeat(1),
            'COLUMN_TYPE'   : np.asarray(types).repeat(1)
            }
    
    # Create the survey_column data frame
    surColDF = pd.DataFrame(data, columns=['VERSION_ID','COLUMN_NO','COLUMN_DESC','COLUMN_TYPE','COLUMN_LENGTH'])  
    
    # Write the transposed data to the oracle database
    cf.insert_into_table_many('SURVEY_COLUMN', surColDF, conn)
    
    return


def process_value_dataframe(df,conn):
    
    # Get the data frame's shape
    N, K = df.shape
    
    # Generate the survey_value data from the imported data frame
    data = {
            'VERSION_ID'    : np.asarray(versionid).repeat(N),
            'COLUMN_VALUE'  : df.values.ravel('F').astype(str),
            'COLUMN_NO'     : np.asarray(numbers).repeat(N),
            'SERIAL_NO'     : np.tile(np.asarray(df['Serial']), K)
            }
    
    # Create the survey_column data frame
    surValDF = pd.DataFrame(data, columns=['VERSION_ID','SERIAL_NO','COLUMN_NO','COLUMN_VALUE'])  
    
    # Replace unwanted data with np.nan values
    surValDF['COLUMN_VALUE'].replace(['None',"",".",'nan'],np.nan,inplace=True)
    
    # Remove the records containing the unwanted data
    surValDF.dropna(subset=['COLUMN_VALUE'], inplace=True)
    
    # Write the transposed data to the oracle database
    cf.insert_into_table_many('SURVEY_VALUE', surValDF, conn)
      

def transpose(file_to_transpose):
        
    # Setup a reference to the imported file (this will be done differently)
    path_to_test_data = file_to_transpose
    
    # Read the SAS file into a variable as a SAS file data type
    sasFile = SAS7BDAT(path_to_test_data)
    
    # Load the SAS file into a pandas data frame
    df = sasFile.to_data_frame()
    
    
    # Create a number of lists to hold the imported data's column properties.
    global numbers
    global versionid
    global types
    global lengths
    global formats
    global labels
    
    numbers, versionid, types, lengths,formats, labels = ([] for i in range(6))
    
    # Extract the column properties from the imported data.
    get_sas_column_properties(sasFile)
    
    # Establish a connection to the oracle database.
    connection = cf.get_oracle_connection()
    
    # Generate and export the value data
    process_value_dataframe(df,connection)
    
    # Generate and export the column data
    process_column_dataframe(df,connection)
     
    # Close the oracle connection
    connection.close()
    
    
if __name__ == '__main__':
    transpose()