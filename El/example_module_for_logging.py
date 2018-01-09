'''
Created on 21 Dec 2017

@author: thorne1
'''
import survey_support
import logging
import inspect
from IPSTransformation import CommonFunctions as cf

def module_level_function():
##    # Following two lines to set up
#    survey_support.setup_logging('IPS_logging_config_debug.json')   # Calls json configuration file   
#    logger = logging.getLogger(__name__)    
    
    # EXAMPLE 1. Same as 'if True: print("uh-oh something failed")' 
    if True:
        cf.database_logger().error("El's SAS_RESPONSE testing")
        
    # EXAMPLE 2. Use of try, catch statement and records traceback and error message to database
    try:
        10/0
    except Exception as err:
        cf.database_logger().error("El's SAS_RESPONSE testing", exc_info = True)

if __name__ == "__main__":
    module_level_function()