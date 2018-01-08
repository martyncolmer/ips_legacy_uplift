'''
Created on 21 Dec 2017

@author: thorne1
'''
import survey_support
import logging

def module_level_function():
    # Following two lines to set up
    survey_support.setup_logging('IPS_logging_config_debug.json')   # Calls json configuration file   
    logger = logging.getLogger(__name__)                            # Creates logger object
    
    # EXAMPLE 1. Same as 'if True: print("uh-oh something failed")' 
    if True:
        logger.error('uh-oh something failed')
        
    # EXAMPLE 2. Use of try, catch statement and records traceback and error message to database
    try:
        10/0
    except:
        logger.info('Example, using try block', exc_info = True)        


if __name__ == "__main__":
    # cf.delete_from_table("SAS_RESPONSE")
    module_level_function()