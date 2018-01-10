
from IPSTransformation.CommonFunctions import get_table_values
import pandas as pd

values = get_table_values('SAS_SHIFT_DATA')

#print(values)

columns = pd.read_csv("columns2.csv")

for col in columns:
    print(col)
    
print(columns)

#print(columns)

#values_df = pd.DataFrame(values)

#print(values_df)