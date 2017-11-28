from sas7bdat import SAS7BDAT
import pandas as pd
import pandas.util.testing as tm; tm.N = 3
import numpy as np   

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
    N, K = frame.shape
    data = {
            #'value'   : frame.values.ravel('F'),
            'COLUMN_NO'  : np.asarray(numbers).repeat(1),
            'COLUMN_DESC': np.asarray(frame.columns).repeat(1),
            'COLUMN_LENGTH'  : np.asarray(lengths).repeat(1),
            #'label'   : np.asarray(labels).repeat(N),
            #'format'  : np.asarray(formats).repeat(N),
            'COLUMN_TYPE'    : np.asarray(types).repeat(1)
            }
    return pd.DataFrame(data, columns=['COLUMN_NO','COLUMN_DESC','COLUMN_TYPE','COLUMN_LENGTH'])  
    

def create_survey_value(frame):
    N, K = frame.shape
    data = {
            'VERSION_ID' : np.asarray(versionid).repeat(N),
            'COLUMN_VALUE'   : frame.values.ravel('F'),
            'COLUMN_NO'  : np.asarray(numbers).repeat(N),
            'SERIAL_NO'  : np.tile(np.asarray(frame['Serial']), K)#.astype(int)
            }
    return pd.DataFrame(data, columns=['VERSION_ID','SERIAL_NO','COLUMN_NO','COLUMN_VALUE'])  

def remove_decimal(text):
    varType = type(text)
    if(varType == float):
        if(text%1 == 0):
            return int(text)
    return text
        
sasFile = 'testdata_2.sas7bdat'

file = SAS7BDAT(sasFile)

columnProperties = [['Num', 'Name', 'Type', 'Length', 'Format', 'Label']]

for i, col in enumerate(file.columns, 1):
    tmp = [i, col.name, col.type, col.length,col.format, col.label]
    columnProperties.append(tmp)
    
types,lengths,formats,labels,numbers = []

for x in columnProperties:
    numbers.append(x[0])
    types.append(x[2])
    lengths.append(x[3])
    formats.append(x[4])
    labels.append(x[5])
    
numbers.pop(0)
versionid = ['9999']* len(numbers)
types.pop(0)
lengths.pop(0)
formats.pop(0)
labels.pop(0)   

df = file.to_data_frame()

pdf = create_survey_value(df).sort_values(by= ['SERIAL_NO','COLUMN_NO'])

pdf2 = pdf
pdf2['SERIAL_NO'] = pdf2['SERIAL_NO'].apply(remove_decimal)
pdf2['COLUMN_VALUE'] = pdf2['COLUMN_VALUE'].apply(remove_decimal)
csvOut = pdf2.to_csv(index = False,header = False)










import El.IPSTransformation.CommonFunctions as elStuff

func =  elStuff.IPSCommonFunctions()
cur = func.get_oracle_connection() 



















print(csvOut)


