'''
Created on 13 Dec 2017

@author: mahont1
'''
from IPSTransformation import CommonFunctions as cf

def unload_parameters(id = False):
    
    """
    Author     : Thomas Mahoney
    Date       : 19 Dec 2017
    Purpose    : Extracts a list of parameters from oracle to be used in the parent process.      
    Params     : id - the identifier used to extract specific parameter sets.
    Returns    : A dictionary of parameters
    """
   
    conn = cf.get_oracle_connection()
    cur = conn.cursor()
    
    if id == False:
        cur.execute("select max(PARAMETER_SET_ID) from SAS_PARAMETERS")
        id = cur.fetchone()[0]
        
    print(id)
    
    cur.execute("select PARAMETER_NAME, PARAMETER_VALUE from SAS_PARAMETERS where PARAMETER_SET_ID = " + str(id))
    results = cur.fetchall()
    tempDict = {}
    for set in results:
        tempDict[set[0].upper()] = set[1]
    
    return tempDict


parameters = unload_parameters(59)

print (parameters)
