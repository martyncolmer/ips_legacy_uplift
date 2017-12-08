'''
Created on 5 Dec 2017

@author: thorne1
'''
import logging
import survey_support

def sample_commit_ips_response():
    """
    Author    : thorne1
    Date      : 29 Nov 2017
    Purpose   : Writes response code and warnings to response table
    Params    : level - "WARNING", "ERROR" or "SUCCESS"
                err - Error message
    Returns   : True or False
    """
    
    survey_support.setup_logging('IPS_logging_config_debug.json')   # Calls json configuration file                   
    logger = logging.getLogger(__name__)                            # Creates logger object
    # Normal Logger examples
    logger.critical('Example 1, like a print statement but will log to db') 
    
    try:
        10/0
    except:
        logger.critical('Example 2, using try block', exc_info = True) 
    

sample_commit_ips_response()