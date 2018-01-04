import cx_Oracle
from IPSTransformation import CommonFunctions as cf

def delete_from_table(table_name, condition1 = None, operator = None, condition2 = None):
    """
    Author     : thorne1
    Date       : 7 Dec 2017
    Purpose    : Generic SQL query to delete contents of table   
    Parameters : table_name - name of table
                 condition1 - first condition
                 operator - comparison operator i.e     
                 '=' Equal
                 '!=' Not Equal
                 '>' Greater than
                 '>=' Greater than or equal, etc
                 https://www.techonthenet.com/oracle/comparison_operators.php
    Returns    : True/False (bool)   
    """
    # Confirm table exists
    if cf.check_table(table_name) == False:
        return False    
    
    # Oracle connection variables
    conn = cf.get_oracle_connection()
    cur = conn.cursor() 
    
    # Create and execute SQL query
    if condition1 == None:
        # Delete everything from table
        sql = "DELETE FROM " + table_name
        print sql
    else:
        # Delete from table where condition1 <COMPARISON OPERATOR> condition1
        sql = "DELETE FROM " + table_name + " WHERE " + condition1 + " " + operator + " '" + condition2 + "'"
        print sql
        
    try:
        cur.execute(sql)
    except cx_Oracle.DatabaseError:
        # Raise (unit testing purposes) and return False to indicate table does not exist
        raise
        return False
    else:   
        conn.commit()
        return True
        
#    sql = "SELECT * FROM " + table_name
#    result = cur.execute(sql).fetchall()
#    
#    if result != []:
#        return False
#    else:
#        return True

table_name = "TRAFFIC_DATA"
condition1 = "TRAFFICTOTAL"
operator = "!="
condition2 = "0"
#delete_from_table(table_name, condition1, operator, condition2)