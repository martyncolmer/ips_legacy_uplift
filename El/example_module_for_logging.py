import logging
import os
import inspect
import survey_support as ss

def basic_example():
    # Database logger setup
    ss.setup_logging(os.path.dirname(os.getcwd()) 
                     + "\\IPS_Logger\\IPS_logging_config_debug.json")   
    logger = logging.getLogger(__name__)
    
    # EXAMPLE 1. Log within 'if' statement
    if True:
        logger.error('uh-oh something failed')
        
    # EXAMPLE 2. Use of try, catch statement and records traceback 
    # and error message to database
    try:
        10/0
    except:
        logger.error('uh-oh something failed', exc_info = True)        


def database_logger():
    """
    Purpose    : Sets up and returns database logger object   
    """
    # Database logger setup
    ss.setup_logging(os.path.dirname(os.getcwd()) 
                     + "\\IPS_Logger\\IPS_logging_config_debug.json")   
    return logging.getLogger(__name__)


def standard_log_message(error_msg, function_name):
    """
    Purpose       : Creates a standard log message which includes the user's 
                  : error message, the filename and function name
    """
    # 0 = frame object, 1 = filename. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    filename = str(inspect.stack()[0][1])
    return error_msg + ' - File "' + filename + '", in ' + function_name + '()'


def module_level_function():
    # 0 = frame object, 3 = function name. 
    # See 28.13.4. in https://docs.python.org/2/library/inspect.html
    function_name = str(inspect.stack()[0][3])
    
    # EXAMPLE 1. Same as 'if True: print("uh-oh something failed")' 
    if True:
        error_msg = 'uh-oh something failed'
        database_logger().error(standard_log_message(error_msg, function_name))
        
    # EXAMPLE 2. Use of try, catch statement and records traceback and error message to database
    try:
        10/0
    except Exception as err:
        cf.database_logger().error("El's SAS_RESPONSE testing", exc_info = True)

if __name__ == "__main__":
    module_level_function()
