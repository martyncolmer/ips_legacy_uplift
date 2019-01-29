import logging
import traceback
import getpass
import datetime
import cx_Oracle
import inspect

from main.io import CommonFunctions as cf

class IPS_Log_Handler(logging.Handler):
    def __init__(self, **kwargs):
        """
        Author        : thorne1
        Date          : 5 Dec 2017
        Purpose       : Sets up the IPS log handler defaults and 
                      : creates a database connection   
        Parameters    : 
        Returns       : None   
        Requirements  : None
        Dependencies  : None
        """
        
        # Setup DB connection
        self.conn = cf.get_sql_connection()
 
        # Set starting instance params
        self.params = {}

        # Mapping for logging level
        self.logging_level = {40: "ERROR", 30: "WARNING", 20: "SUCCESS"}
        
        # Call to super method
        logging.Handler.__init__(self, level = kwargs.get("level", logging.NOTSET))
        
        
    def emit(self, record):
        """
        Author        : thorne1
        Date          : 5 Dec 2017
        Purpose       : Overwritten logging handler method that  
                      : carries out the writing of the data to 
                      : the destination object.   
        Parameters    : Record - It is called when 
                      : you log a message using the logger.debug..etc method
        Returns       : None   
        Requirements  : None
        Dependencies  : None
        """
                
        # Assign 'process_id' (as per DP) by returning 
        # max process_id and incrementing by 1. 
        # To be amended once processes are in place.
        cur = self.conn.cursor()        
        sql = "SELECT MAX(SAS_PROCESS_ID) FROM SAS_RESPONSE"
        sas_id = cur.execute(sql).fetchone()
        self.params['process_id'] = sas_id[0] + 1
        
        # Assign response codes and log messages, i.e
        # error or warning, as per logging.level
        self.params["error_msg"] = ''
        self.params["warnings"] = ''
        self.params["audit_log_details"] = ''
        
        if record.levelno == logging.ERROR:
            self.params["response_code"] = 3
            self.params["error_msg"] = record.msg
        elif record.levelno == logging.WARNING:
            self.params["response_code"] = 2
            self.params["warnings"] = record.msg
        elif record.levelno == logging.INFO:
            self.params["response_code"] = 1
            self.params["warnings"] = record.msg
        
        # Assign traceback message
        if record.exc_info and record.exc_info[0]:
            self.params['error_msg'] = str(record.exc_info[1])
            self.params['stack_trace'] = traceback.format_exc().replace('\n','')
        else:
            self.params['stack_trace'] = ''
        
        self.commit_to_response(record)
    
    
    def commit_to_response(self, record):
        """
        Author        : thorne1
        Date          : 5 Dec 2017
        Purpose       : Writes response code and errors/warnings 
                      : to response table  
        Parameters    : record - This is automatically populated by the logger  
        Returns       : None  
        Requirements  : None
        Dependencies  : None
        """
        
        # Setup the parameters from the instance params object
        params = (self.params['process_id']
        		  , self.params['response_code']
        		  , self.params['error_msg']    
        		  , self.params['stack_trace']
        		  , self.params['warnings'])

        # Prepare SQL statement
        table_name = "SAS_RESPONSE "
        sql = ("INSERT INTO " 
        	   + table_name 
        	   + """(SAS_PROCESS_ID
        	   , RESPONSE_CODE
        	   , ERROR_MSG
        	   , STACK_TRACE
        	   , WARNINGS) 
        	   VALUES(:a, :b, :c, :d, :e)""")
        
        # Execute SQL
        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()