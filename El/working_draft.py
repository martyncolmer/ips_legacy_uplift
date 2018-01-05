import inspect
import os
import sys

from IPSTransformation import CommonFunctions as cf

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


print cf.validate_file("")
