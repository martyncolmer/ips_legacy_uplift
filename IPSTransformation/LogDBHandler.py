import logging
import cx_Oracle
import datetime
import traceback

import CommonFunctions as cf

class IPS_Log_Handler(logging.Handler):
    def __init__(self, **kwargs):
        """
        Author     : Martyn Colmer & thorne1
        Date       : 5 Dec 2017
        Purpose    : Sets up the IPS log handler defaults and creates a database connection     
        """

        # Setup DB connection
        self.conn = cf.get_oracle_connection()
 
        # Set starting instance params
        self.params = {}
        self.params["response_code"] = "0"
        # self.params["failover"] = False
        
        # Mapping for logging level
        self.logging_level = {40: "ERROR"
                              , 30: "WARNING"
                              , 20: "SUCCESS"}
               
        # Call to super method
        logging.Handler.__init__(self, level = kwargs.get("level", logging.NOTSET))
        
        
    def emit(self, record):
        """
        Author     : Martyn Colmer & thorne1
        Date       : 5 Dec 2017
        Purpose    : Overwritten logging handler method that carries out the writing of the data to the destination object.
                     It is called when you log a message using the logger.debug..etc method. 
        Params     : record - This is populated by the logger automatically.   
        """
        # Add date and time details to instance params dict
        py_now = datetime.datetime.now()        
        self.params["record_date"] = cx_Oracle.Timestamp(py_now.year
                                                         , py_now.month
                                                         , py_now.day
                                                         , int(py_now.hour)
                                                         , int(py_now.minute)
                                                         , int(py_now.second))
        
        # i.e logging.debug("this is your record.msg")
        self.params['warnings'] = record.msg
        
        # Add exception and traceback info to the params dict
        if record.exc_info and record.exc_info[0]:
            self.params['process_id'] = 1   # cur.fetchall PROCESS_ID +1
            self.params['error_msg'] = str(record.exc_info[1])      # error message from exception, i.e "file does not exist"
            self.params['stack_trace'] = traceback.format_exc().replace('\n','') # traceback
        else:
            self.params['process_id'] = 0
            self.params['error_msg'] = ''
            self.params['stack_trace'] = ''
        
        # Test if an error has been raised and set the response code
        # AMEND THIS
        if record.levelno == logging.ERROR:
            self.params["response_code"] = 40
        if record.levelno == logging.WARNING:
            self.params["response_code"] = 30
        
        self.commit_response(record)
   
    
    def commit_response(self, record):
        """
        Author     : Martyn Colmer & thorne1
        Date       : 5 Dec 2017
        Purpose    : Writes response code and warnings to response table  
        Params     : record - This is populated by the logger automatically
        Returns    : True/False  
        """
        
        # DB variables
        cur = self.conn.cursor()
        
        # Setup the parameters from the instance params object
        params = (self.params['process_id']
                  , self.params['response_code']
                  , self.params['error_msg']    
                  , self.params['stack_trace']
                  , self.params['warnings'])
                  
        # need to do something with self.params['record_date']
        
        # Prepare SQL statement
        table_name = "RESPONSE"
        sql = ("INSERT INTO " 
               + table_name 
               + """(PROCESS_ID
               , RESPONSE_CODE
               , ERROR_MSG
               , STACK_TRACE
               , WARNINGS) 
               VALUES(:a, :b, :c, :d, :e)""")
        
        print sql
        for x in params:
            print x
        
#        # Execute SQL
#        cur = self.conn
#        cur.execute(sql, params)
#        self.conn.commit()