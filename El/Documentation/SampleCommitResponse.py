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
    
#        try:
#            survey_support.setup_logging('IPS_logging_config_debug.json') 
#            print "success"           
#        except:
#            #This cane be altered to a different logger if required.
#            logging.basicConfig(level = logging.DEBUG)

    print "Start"
    survey_support.setup_logging('IPS_logging_config_debug.json')            
    #This cane be altered to a different logger if required.
    
    logger = logging.getLogger(__name__)
    
    # Normal Logger examples
    logger.critical('test ceel 2')
    logger.error("test ceel 2")
    logger.warn('yo 2')
    logger.info('yo 2')
    logger.debug('yo Debug')
    
    print "Middle"
    
    # Logger example with exception
    try:
        10/0
    except:
        logger.critical('err', exc_info = True)
    
    print "End"
    
sample_commit_ips_response()