from sas7bdat import SAS7BDAT
from IPSTransformation import CommonFunctions as cf
import pandas as pd
import pandas.util.testing as tm; tm.N = 3
import numpy as np   
import time


##Used to Transpose the Dataset into a vertical format.


def unpivot(frame):
    N, K = frame.shape
    data = {'value'   : frame.values.ravel('F'),
            'variable': np.asarray(frame.columns).repeat(N),
            'Type'    : np.asarray(types).repeat(N),
            'Length'  : np.asarray(lengths).repeat(N),
            'label'   : np.asarray(labels).repeat(N),
            'format'  : np.asarray(formats).repeat(N),
            'type'    : np.asarray(types).repeat(N),
            'number'  : np.asarray(numbers).repeat(N),
            'Serial'  : np.tile(np.asarray(frame['Serial']), K)}
    return pd.DataFrame(data, columns=['Serial','number','variable','label','Length', 'value','format','type'])    


def create_survey_column(frame):
    data = {
            'VERSION_ID'    : np.asarray(versionid).repeat(1),
            'COLUMN_NO'     : np.asarray(numbers).repeat(1),
            'COLUMN_DESC'   : np.asarray(frame.columns).repeat(1),
            'COLUMN_LENGTH' : np.asarray(lengths).repeat(1),
            'COLUMN_TYPE'   : np.asarray(types).repeat(1)
            }
    return pd.DataFrame(data, columns=['VERSION_ID','COLUMN_NO','COLUMN_DESC','COLUMN_TYPE','COLUMN_LENGTH'])  
    

def create_survey_value(frame):
    N, K = frame.shape
    data = {
            'VERSION_ID'    : np.asarray(versionid).repeat(N),
            'COLUMN_VALUE'  : frame.values.ravel('F').astype(str),
            'COLUMN_NO'     : np.asarray(numbers).repeat(N),
            'SERIAL_NO'     : np.tile(np.asarray(frame['Serial']), K)#.astype(int)
            }
    return pd.DataFrame(data, columns=['VERSION_ID','SERIAL_NO','COLUMN_NO','COLUMN_VALUE'])  


def remove_decimal(text):
    varType = type(text)
    if(varType == float):
        if(text%1 == 0):
            return int(text)
    return text
   
   
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


def write_to_survey_column(connection,dataFrame):
    
    rows = [tuple(x) for x in dataFrame.values]
    
    print(type(rows))
    cur = connection.cursor()
    
    cur.executemany("""INSERT into SURVEY_COLUMN
         (VERSION_ID,COLUMN_NO,COLUMN_DESC,COLUMN_TYPE,COLUMN_LENGTH)
         VALUES (:p1,:p2,:p3,:p4,:p5)""",
         rows
         )
    
    print("Records added to SURVEY_COLUMN table - " + str(len(rows)))     
    connection.commit()


def write_to_survey_value(connection,dataFrame):

    rows = [tuple(x) for x in dataFrame.values]
       
    
    cur = connection.cursor()
         
    cur.executemany("""INSERT into SURVEY_VALUE
         (VERSION_ID,SERIAL_NO,COLUMN_NO,COLUMN_VALUE)
         VALUES (:a,:b,:c,:d)""",
         rows
         )
    
    print("Records added to SURVEY_VALUE table - " + str(len(rows)))   
      
    connection.commit()


def delete_from_table(connection,table,variable,value):
    
    statement = "DELETE from " + table + " where " + variable + " = " + value 
    
    cur = connection.cursor()
    cur.execute(statement)
    
    connection.commit()
     

def process_column_dataframe(conn,df):
    
    # Create the dataframes
    surColDF = create_survey_column(df)
    
    # Write the transposed data to the oracle database
    write_to_survey_column(conn, surColDF)
    pass


def process_value_dataframe(conn,df):
    
    # Create the dataframes
    surValDF = create_survey_value(df)
    
    # Filter blanks
    print("Value count pre-filter - " + str(len(surValDF)))

    surValDF['COLUMN_VALUE'].replace(['None',"",".",'nan'],np.nan,inplace=True)
    surValDF.dropna(subset=['COLUMN_VALUE'], inplace=True)
    
    #surValDF.to_csv("PostFilter.csv")
    print("Value count post-filter - " + str(len(surValDF)))
    
    # Write the transposed data to the oracle database
    write_to_survey_value(conn, surValDF)
    pass
     
     
def process_value_dataframes_split_version(conn, dataFrame):
    
    print("Splitting sas data")
    #SplitDataframe
    frames = np.array_split(dataFrame,100)
    
    valDFs = []
    
    
    
    for f in frames:
        print('Creating Dataframes')
        #Create the data frames
        valDFs.append(create_survey_value(f))
    
    for vdf in valDFs:
        #filter blanks
        vdf['COLUMN_VALUE'].replace([None,"","."],np.nan,inplace=True)
        vdf.dropna(subset=['COLUMN_VALUE'], inplace=True)
        
        #Write the transposed data to the oracle database
        write_to_survey_value(conn, vdf)
    
    
""""""    
#----------------------------------------------------------------#   
#Instance of the common functions module 
#cf = IPSCommonFunctions()
#Load in the sas file
fileName = 'InputFiles/testdata.sas7bdat'
sasFile = SAS7BDAT(fileName)


#Extract the column properties
numbers, versionid, types, lengths,formats, labels = ([] for i in range(6))
get_sas_column_properties(sasFile)

start = time.time()

#Load the SAS file into a dataframe
print("Getting sas data")
df = sasFile.to_data_frame()

#Connect to oracle
print("Connecting to Oracle")
connection = cf.get_oracle_connection()
## Delete the data from the table
print("DELETING")
#delete_from_table(connection, "SURVEY_VALUE", "VERSION_ID", "9999")
#delete_from_table(connection, "SURVEY_COLUMN", "VERSION_ID", "9999")

process_value_dataframe(connection,df)
#process_column_dataframe(connection,df)

fin = time.time()

print("TIME - " + str(fin-start))

 
connection.close()
print(".")
