import cx_Oracle
from IPSTransformation import CommonFunctions as cf
import pandas.io.common


def import_traffic_data(filename):
    """
    Author    : thorne1
    Date      : 27 Nov 2017
    Purpose   : Imports CSV (Sea, CAA, Tunnel Traffic, Possible Shifts, Non Response or Unsampled) and inserts to Oracle   
    Params    : filename - directory path to CSV
    Returns   : True or False
    REQS      : pip install pandas
    DEPS      : cf.import_csv()
                cf.validate_csv() 
                cf.get_oracle_connection()                
                select_data()
    """
    
    # Import CSV and validate
    if cf.validate_file(filename) == True:
        try:
            pandas.read_csv(filename)
        except pandas.io.common.EmptyDataError as err:
            # return False to indicate failure
            print err
            return False
        except ValueError as err:
            # return False to indicate failure 
            print err
            return False
        except IOError as err:
            # return False to indicate failure 
            print err
            return False
        else:
            df = cf.import_csv(filename)
            if df.empty:
                return False
    else:
        print "File validation failed"
        return False
    
    # Data cleansing
    dataframe = df.fillna('')
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    if "REGION" in dataframe.columns:
        dataframe['REGION'].replace(['None',"",".",'nan'],0,inplace=True)
    
    # Get datasource values i.e, "Sea", "Air", "Tunnel", etc
    datasource = dataframe.at[1, 'DATASOURCE']  
    
    # Get datasource id i.e 1, 2, 3, etc as per DATA_SOURCE table
    datasource_id = cf.select_data("DATA_SOURCE_ID", "DATA_SOURCE", "DATA_SOURCE_NAME", datasource)
    
    # Oracle connection variables
    conn = cf.get_oracle_connection()
    cur = conn.cursor()    
    table_name_dict = {"Sea": "TRAFFIC_DATA"
                  , "Air": "TRAFFIC_DATA"
                  , "Tunnel": "TRAFFIC_DATA"
                  , "Shift": "SHIFT_DATA"
                  , "Non Response": "NON_RESPONSE_DATA"
                  , "Unsampled": "UNSAMPLED_OOH_DATA"}    
    table_name = table_name_dict[datasource]
    
    # Table cleansing
    run_id = "IPSSeedRunFR02"                           # Primary-key constraint on TRAFFIC_DATA. See RUN table
    table_run_id = cf.select_data("RUN_ID", table_name, "RUN_ID", run_id)
    
    if ("'" + run_id + "'") == table_run_id:
        cf.delete_from_table(table_name)
        
    # Table values   
    rows = [list(x) for x in dataframe.values]    
    for row in rows:
        row.insert(0,run_id)                            # Insert row_id value as first column
        row[row.index(datasource)] = datasource_id      # Replace datasource values with datasource_id
    
    # This also needs cleaning
    if table_name == "TRAFFIC_DATA":
        vehicle = None
        for row in rows:
            row.append(vehicle)                          # Insert vehicle value as last column
        sql = ("INSERT INTO " 
           + table_name 
           + " VALUES(:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)")
    if table_name == "SHIFT_DATA":
        sql = ("INSERT INTO " 
           + table_name 
           + " VALUES(:0, :1, :2, :3, :4, :5, :6, :7, :8)")
    if table_name == "NON_RESPONSE_DATA":        
        sql = ("INSERT INTO " 
           + table_name 
           + " VALUES(:0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10)")
    if table_name == "UNSAMPLED_OOH_DATA":  
        sql = ("INSERT INTO " 
           + table_name 
           + " VALUES(:0, :1, :2, :3, :4, :5, :6, :7)")   
    
    try:
        # Execute SQL statement
        cur.executemany(sql, rows)            
    except cx_Oracle.DatabaseError:
        # Return False to indicate error
        raise
        return False
    else:
        conn.commit()
    
    # Ensure table has values
    sql_validation = "SELECT count(*) FROM " + table_name
    cur.execute(sql_validation)
    
    if cur.fetchone() != (0,):
        return True
    else:
        return False
    

if __name__ == "__main__":
    print import_traffic_data(r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv")