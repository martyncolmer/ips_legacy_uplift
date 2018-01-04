"""
    Included in TestSuite purely for testing purposes
"""
import pandas
from IPSTransformation import CommonFunctions as cf

def import_data(filename):
    """
    Author        : thorne1
    Date          : 27 Nov 2017
    Purpose       : Imports CSV (Sea, CAA, Tunnel Traffic, Possible Shifts,
                    Non Response or Unsampled) and inserts to Oracle   
    Parameters    : filename - directory path to CSV
    Returns       : True or False
    Requirements  : pip install pandas
    Dependencies  : cf.import_csv()
                    cf.validate_csv() 
                    cf.get_oracle_connection()                
                    select_data()
    """
    
    # run_id currently hard-coded due to
    # primary-key constraint on TRAFFIC_DATA (see RUN table)
    # THIS WILL NEED TO BE AMENDED ONCE run_id PROCESS IMPLEMENTED
    run_id = "IPSSeedRun"
    
    # Import CSV and validate
    if cf.validate_file(filename) == True:
        try:
            pandas.read_csv(filename)
        except Exception as err:
            # return False to indicate failure
            print(err)
            return False
        else:
            dataframe = cf.import_csv(filename)
            if dataframe.empty:
                return False
    else:
        print("File validation failed")
        return False
    
    # Data cleansing
    dataframe.columns = dataframe.columns.str.upper()                           # Change column names to upper case
    dataframe.columns = dataframe.columns.str.replace(' ', '')                  # Remove whitespaces within column names
    dataframe["RUN_ID"] = pandas.Series(run_id, index = dataframe.index)        # Insert "ROW_ID" column to dataframe
    dataframe.rename(columns={"DATASOURCE":"DATA_SOURCE_ID"}, inplace = True)   # Replace "DATASOURCE" column name with "DATA_SOURCE_ID"
    dataframe = dataframe.fillna('')                                            # Replace Nan values with empty string
    if "REGION" in dataframe.columns:                                           # Replace "REGION" values with 0 if not an expected value                           
        dataframe['REGION'].replace(['None',"",".",'nan'],0,inplace=True) 
    
    # Get datasource values i.e, "Sea", "Air", "Tunnel", etc
    return_amount = 1
    datasource = dataframe.at[return_amount, 'DATA_SOURCE_ID']  
    
    # Get datasource id i.e 1, 2, 3, etc as per DATA_SOURCE table
    # and replace current datasource values with new datasource_id
    datasource_id = cf.select_data("DATA_SOURCE_ID", "DATA_SOURCE", "DATA_SOURCE_NAME", datasource)
    dataframe['DATA_SOURCE_ID'].replace([datasource],datasource_id,inplace=True)
        
    # Oracle connection variables
    conn = cf.get_oracle_connection()
    cur = conn.cursor()
    
    # Key = datasource / Value = table name    
    table_name_dict = {"Sea": "TRAFFIC_DATA"
                  , "Air": "TRAFFIC_DATA"
                  , "Tunnel": "TRAFFIC_DATA"
                  , "Shift": "SHIFT_DATA"
                  , "Non Response": "NON_RESPONSE_DATA"
                  , "Unsampled": "UNSAMPLED_OOH_DATA"}    
    table_name = table_name_dict[datasource]
    
    # If table is not empty...
    sql = "SELECT * FROM " + table_name 
    row_count = cur.execute(sql)
    row_count = cur.fetchall()
    row_count = cur.rowcount 
    if  row_count != 0:
        # ...check if run_id currently exists...
        table_run_id = cf.select_data("RUN_ID", table_name, "RUN_ID", run_id)
        #...and delete if it does
        if ("'" + run_id + "'") == table_run_id:
            cf.delete_from_table(table_name, "RUN_ID", "=", table_run_id)
        
    # If table_name == "TRAFFIC_DATA" insert "VEHICLE" column with empty values
    if table_name == "TRAFFIC_DATA":
        dataframe["VEHICLE"] = pandas.Series("", index = dataframe.index) 
   
    # Insert dataframe to table
    cf.insert_into_table_many(table_name, dataframe)
       

if __name__ == "__main__":
    root_file_path = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing"
    print(import_data(root_file_path + "\Sea Traffic Q1 2017.csv"))