import inspect
import os
import sys
import getpass
import datetime
import cx_Oracle

from IPSTransformation import CommonFunctions as cf
import import_traffic_data as itd

def function_name_here():
    # 0 the frame object, 
    # 1 the filename, 
    # 2 the line number of the current line, 
    # 3 the function name, 
    # 4 a list of lines of context from the source code, 
    # 5 and the index of the current line within that list.
    working_dir = str(inspect.stack()[1])
    
    print working_dir 
    sys.exit()
    
    line_number = str(inspect.stack()[0][2])
    function_name = str(inspect.stack()[0][3])
    msg = "Whatever you want"
    return 'File "' + working_dir + '", in ' + function_name + '(). ' + msg
    

def traceback_view():
    # Setup the parameters from the instance params object
    params = {}
    params['process_id'] = "Process ID"
    params['response_code'] = "Response code"
    
    msg = "I don't want this string (). This is what I want"
    msg = msg.split("(). ")
    
    print sys.exc_info()
    
    print msg[1]
    
    print params['process_id'] + params['response_code']
    

def returny_value():
    cf.database_logger().info("El's testing")
    
    return "Refresh table" 
    

def my_sum(**kwargs):
    return sum(kwargs)


def commit_to_audit_log(action, process_object, audit_msg, function_name):
    params = {}
    
    # AUDIT_LOG VARIABLES #
    # Assign 'audit_id' by returning max audit_id and increment by 1 
    conn = cf.get_oracle_connection()
    cur = conn.cursor()        
    sql = "SELECT MAX(AUDIT_ID) FROM AUDIT_LOG"
    sas_id = cur.execute(sql).fetchone()
    params['audit_id'] = sas_id[0] + 1
    
    # Assign 'actioned_by' 
    params['actioned_by'] = getpass.getuser()
    
    # Assign 'action'
    params['action'] = action
    
    # Assign 'process_object'
    params['object'] = process_object
    
    # Assign 'log_date and time'
    # Add date and time details to instance params dict
    # Format - 'YY-MM-DD HH:MM:SS'
    py_now = datetime.datetime.now()        
    params['log_date'] = cx_Oracle.Timestamp(py_now.year
                                             , py_now.month
                                             , py_now.day
                                             , int(py_now.hour)
                                             , int(py_now.minute)
                                             , int(py_now.second))
    
    params['audit_log_details'] = audit_msg
    
    params = (params['audit_id']
                  , params['actioned_by']
                  , params['action']    
                  , params['object']
                  , params['log_date']
                  , params['audit_log_details'])

    # Prepare SQL statement
    table_name = "AUDIT_LOG "
    sql = ("INSERT INTO " 
           + table_name 
           + """(AUDIT_ID
           , ACTIONED_BY
           , ACTION
           , OBJECT
           , LOG_DATE
           , AUDIT_LOG_DETAILS) 
           VALUES(:a, :b, :c, :d, :e, :f)""")
    
    # Execute SQL
    cur.execute(sql, params)
    conn.commit()
    

if __name__ == "__main__":
    filename = r"\\nsdata3\Social_Surveys_team\CASPA\IPS\Testing\Sea Traffic Q1 2017.csv"
    itd.import_traffic_data(filename)

#print my_sum(2,3,4)
