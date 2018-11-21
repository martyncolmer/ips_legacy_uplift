from main.io import CommonFunctions as cf
import pandas as pd
import sys
import random
import numpy as np
import math
random.seed(123456)

def test_pv(row, fakeval, dataset):

    if(dataset == 'survey'):
        if row['SERIAL'] in (430908723026,
                             430908830004,
                             430908883009,
                             430908901030,
                             430908911021,
                             430908911022,
                             430908913020,
                             430928888035,
                             430928901029,
                             430928904037,
                             430928911028,
                             430928911034,
                             435808385014,
                             435808787013,
                             435808787016):
            print("at target")

    #PV 1

    if row['PORTROUTE'] in (111, 113, 119, 161):
        row['UNSAMP_PORT_GRP_PV'] = 'A111'
    elif row['PORTROUTE'] in (121, 123, 162, 172):
        row['UNSAMP_PORT_GRP_PV'] = 'A121'
    elif row['PORTROUTE'] in (131, 132, 133, 134, 135, 163, 173):
        row['UNSAMP_PORT_GRP_PV'] = 'A131'
    elif row['PORTROUTE'] in (141, 142, 143, 144, 145, 164):
        row['UNSAMP_PORT_GRP_PV'] = 'A141'
    elif row['PORTROUTE'] in (151, 152, 153, 165):
        row['UNSAMP_PORT_GRP_PV'] = 'A151'
    elif row['PORTROUTE'] in (181, 183, 189):
        row['UNSAMP_PORT_GRP_PV'] = 'A181'
    elif row['PORTROUTE'] in (191, 192, 193, 199):
        row['UNSAMP_PORT_GRP_PV'] = 'A191'
    elif row['PORTROUTE'] in (201, 202, 203):
        row['UNSAMP_PORT_GRP_PV'] = 'A201'
    elif row['PORTROUTE'] in (211, 213, 219):
        row['UNSAMP_PORT_GRP_PV'] = 'A211'
    elif row['PORTROUTE'] in (221, 223):
        row['UNSAMP_PORT_GRP_PV'] = 'A221'
    elif row['PORTROUTE'] in (231, 232):
        row['UNSAMP_PORT_GRP_PV'] = 'A231'
    elif row['PORTROUTE'] in (241, 243):
        row['UNSAMP_PORT_GRP_PV'] = 'A241'
    elif row['PORTROUTE'] in (381, 382, 391, 341, 331, 451):
        row['UNSAMP_PORT_GRP_PV'] = 'A991'
    elif row['PORTROUTE'] in (401, 411, 441, 471):
        row['UNSAMP_PORT_GRP_PV'] = 'A992'
    elif row['PORTROUTE'] in (311, 371, 421, 321, 319):
        row['UNSAMP_PORT_GRP_PV'] = 'A993'
    elif row['PORTROUTE'] in (461, 351, 361, 481):
        row['UNSAMP_PORT_GRP_PV'] = 'A994'
    elif row['PORTROUTE'] == 991:
        row['UNSAMP_PORT_GRP_PV'] = 'A991'
    elif row['PORTROUTE'] == 992:
        row['UNSAMP_PORT_GRP_PV'] = 'A992'
    elif row['PORTROUTE'] == 993:
        row['UNSAMP_PORT_GRP_PV'] = 'A993'
    elif row['PORTROUTE'] == 994:
        row['UNSAMP_PORT_GRP_PV'] = 'A994'
    elif row['PORTROUTE'] == 995:
        row['UNSAMP_PORT_GRP_PV'] = 'ARE'
    elif row['PORTROUTE'] in (611, 612, 613, 851, 853, 868, 852):
        row['UNSAMP_PORT_GRP_PV'] = 'DCF'
    elif row['PORTROUTE'] in (621, 631, 632, 633, 634, 854):
        row['UNSAMP_PORT_GRP_PV'] = 'SCF'
    elif row['PORTROUTE'] in (641, 865):
        row['UNSAMP_PORT_GRP_PV'] = 'LHS'
    elif row['PORTROUTE'] in (635, 636, 651, 652, 661, 662, 856):
        row['UNSAMP_PORT_GRP_PV'] = 'SLR'
    elif row['PORTROUTE'] in (671, 859, 860, 855):
        row['UNSAMP_PORT_GRP_PV'] = 'HBN'
    elif row['PORTROUTE'] in (672, 858):
        row['UNSAMP_PORT_GRP_PV'] = 'HGS'
    elif row['PORTROUTE'] in (681, 682, 691, 692, 862):
        row['UNSAMP_PORT_GRP_PV'] = 'EGS'
    elif row['PORTROUTE'] in (701, 711, 741, 864):
        row['UNSAMP_PORT_GRP_PV'] = 'SSE'
    elif row['PORTROUTE'] in (721, 722, 863):
        row['UNSAMP_PORT_GRP_PV'] = 'SNE'
    elif row['PORTROUTE'] in (731, 861):
        row['UNSAMP_PORT_GRP_PV'] = 'RSS'
    elif row['PORTROUTE'] == (811):
        row['UNSAMP_PORT_GRP_PV'] = 'T811'
    elif row['PORTROUTE'] == (812):
        row['UNSAMP_PORT_GRP_PV'] = 'T812'
    elif row['PORTROUTE'] == (911):
        row['UNSAMP_PORT_GRP_PV'] = 'E911'
    elif row['PORTROUTE'] == (921):
        row['UNSAMP_PORT_GRP_PV'] = 'E921'
    elif row['PORTROUTE'] == (951):
        row['UNSAMP_PORT_GRP_PV'] = 'E951'

    Irish = 0
    IoM = 0
    ChannelI = 0
    dvpc = 0

    if dataset == 'survey':
        if not math.isnan(row['DVPORTCODE']):
            dvpc = int(row['DVPORTCODE'] / 1000)

        if dvpc == 372:
            Irish = 1
        elif (row['DVPORTCODE'] == 999999) or math.isnan(row['DVPORTCODE']):
            if ((row['FLOW'] in (1, 3)) and (row['RESIDENCE'] == 372)):
                Irish = 1
            elif ((row['FLOW'] in (2, 4)) and (row['COUNTRYVISIT'] == 372)):
                Irish = 1

        if dvpc == 833:
            IoM = 1
        elif (row['DVPORTCODE'] == 999999) or math.isnan(row['DVPORTCODE']):
            if ((row['FLOW'] in (1, 3)) and (row['RESIDENCE'] == 833)):
                IoM = 1
            elif ((row['FLOW'] in (2, 4)) and (row['COUNTRYVISIT'] == 833)):
                IoM = 1

        if dvpc in (831, 832, 931):
            ChannelI = 1

        elif (row['DVPORTCODE'] == 999999) or math.isnan(row['DVPORTCODE']):
            if ((row['FLOW'] in (1, 3)) and (row['RESIDENCE'] in (831, 832, 931))):
                ChannelI = 1
            elif ((row['FLOW'] in (2, 4)) and (row['COUNTRYVISIT'] in (831, 832, 931))):
                ChannelI = 1

        if (Irish) and row['PORTROUTE'] in (111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152):
            row['UNSAMP_PORT_GRP_PV'] = 'AHE'
        elif (Irish) and row['PORTROUTE'] in (181, 191, 192, 189, 199):
            row['UNSAMP_PORT_GRP_PV'] = 'AGE'
        elif (Irish) and row['PORTROUTE'] in (211, 221, 231, 219, 249):
            row['UNSAMP_PORT_GRP_PV'] = 'AME'
        elif (Irish) and row['PORTROUTE'] == 241:
            row['UNSAMP_PORT_GRP_PV'] = 'ALE'
        elif (Irish) and row['PORTROUTE'] in (201, 202):
            row['UNSAMP_PORT_GRP_PV'] = 'ASE'
        elif (Irish) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
            row['UNSAMP_PORT_GRP_PV'] = 'ARE'
        elif (ChannelI) and (row['PORTROUTE'] >= 100) and (row['PORTROUTE'] < 300):
            row['UNSAMP_PORT_GRP_PV'] = 'MAC'
        elif (ChannelI) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
            row['UNSAMP_PORT_GRP_PV'] = 'RAC'
        elif (IoM) and (row['PORTROUTE'] >= 100) and (row['PORTROUTE'] < 300):
            row['UNSAMP_PORT_GRP_PV'] = 'MAM'
        elif (IoM) and (row['PORTROUTE'] >= 300) and (row['PORTROUTE'] < 600):
            row['UNSAMP_PORT_GRP_PV'] = 'RAM'

    #PV 2

    row['ARRIVEDEPART'] = int(row['ARRIVEDEPART'])
    if dataset == 'survey':
        if not math.isnan(row['DVPORTCODE']):
            dvpc = int(row['DVPORTCODE'] / 1000)
        if row['PORTROUTE'] < 300:
            if row['DVPORTCODE'] == 999999 or math.isnan(row['DVPORTCODE']):
                if row['FLOW'] in (1, 3):
                    row['REGION'] = row['RESIDENCE']
                elif row['FLOW'] in (2, 4):
                    row['REGION'] = row['COUNTRYVISIT']
                else:
                    row['REGION'] = ''
            else:
                row['REGION'] = dvpc
            if row['REGION'] in (
            8, 20, 31, 40, 51, 56, 70, 100, 112, 191, 203, 208, 233, 234, 246, 250, 268, 276, 348, 352, 380, 398, 417,
            428, 440, 442, 492, 498, 499, 528, 578, 616, 642, 643, 674, 688, 703, 705, 752, 756, 762, 795, 804, 807,
            860, 940, 942, 943, 944, 945, 946, 950, 951):
                row['UNSAMP_REGION_GRP_PV'] = '1'
            elif row['REGION'] in (124, 304, 630, 666, 840, 850):
                row['UNSAMP_REGION_GRP_PV'] = '2'
            elif row['REGION'] in (
            4, 36, 50, 64, 96, 104, 116, 126, 144, 156, 158, 242, 356, 360, 408, 410, 418, 446, 458, 462, 496, 524, 554,
            586, 608, 626, 702, 704, 764):
                row['UNSAMP_REGION_GRP_PV'] = '3'
            elif row['REGION'] in (
            12, 24, 48, 72, 108, 120, 132, 140, 148, 174, 178, 180, 204, 226, 231, 232, 262, 266, 270, 288, 324, 348,
            384, 404, 426, 430, 434, 450, 454, 466, 478, 480, 504, 508, 516, 562, 566, 624, 646, 654, 678, 686, 690,
            694, 706, 710, 716, 732, 736, 748, 768, 788, 800, 818, 834, 854, 894):
                row['UNSAMP_REGION_GRP_PV'] = '4'
            elif row['REGION'] == 392:
                row['UNSAMP_REGION_GRP_PV'] = '5'
            elif row['REGION'] == 344:
                row['UNSAMP_REGION_GRP_PV'] = '6'
            elif row['REGION'] in (
            16, 28, 32, 44, 48, 52, 60, 68, 76, 84, 90, 92, 136, 152, 166, 170, 184, 188, 192, 212, 214, 218, 222, 238,
            254, 258, 296, 308, 312, 316, 320, 328, 332, 340, 364, 368, 376, 388, 400, 414, 422, 474, 484, 500, 512,
            520, 530, 533, 540, 548, 558, 580, 581, 584, 591, 598, 604, 634, 638, 659, 660, 662, 670, 682, 690, 740,
            760, 776, 780, 784, 796, 798, 858, 862, 882, 887, 949):
                row['UNSAMP_REGION_GRP_PV'] = '7'
            elif row['REGION'] == 300:
                row['UNSAMP_REGION_GRP_PV'] = '8'
            elif row['REGION'] in (292, 620, 621, 911, 912):
                row['UNSAMP_REGION_GRP_PV'] = '9'
            elif row['REGION'] in (470, 792, 901, 902):
                row['UNSAMP_REGION_GRP_PV'] = '10'
            elif row['REGION'] == 372:
                row['UNSAMP_REGION_GRP_PV'] = '11'
            elif row['REGION'] in (831, 832, 833, 931):
                row['UNSAMP_REGION_GRP_PV'] = '12'
            elif row['REGION'] in (921, 923, 924, 926, 933):
                row['UNSAMP_REGION_GRP_PV'] = '13'
    elif dataset == 'unsampled':
        row['ARRIVEDEPART'] = int(row['ARRIVEDEPART'])
        if not math.isnan(row['REGION']):
            row['REGION'] = int(row['REGION'])
            row['UNSAMP_REGION_GRP_PV'] = str(row['REGION'])

    if row['UNSAMP_PORT_GRP_PV'] == 'A181' and row['UNSAMP_REGION_GRP_PV'] == '6':
        print("HIT")

    if row['UNSAMP_PORT_GRP_PV'] == 'A201' and row['UNSAMP_REGION_GRP_PV'] == '7' and row['ARRIVEDEPART'] == 2:
        row['UNSAMP_PORT_GRP_PV'] = 'A191'
    if row['UNSAMP_PORT_GRP_PV'] == 'HGS':
        row['UNSAMP_PORT_GRP_PV'] = 'HBN'
    if row['UNSAMP_PORT_GRP_PV'] == 'E921':
        row['UNSAMP_PORT_GRP_PV'] = 'E911'
    if row['UNSAMP_PORT_GRP_PV'] == 'E951':
        row['UNSAMP_PORT_GRP_PV'] = 'E911'

    if row['UNSAMP_PORT_GRP_PV'] == 'A181' and row['UNSAMP_REGION_GRP_PV'] == '6' and row['ARRIVEDEPART'] == 1:
        row['UNSAMP_PORT_GRP_PV'] = 'A151'
    if row['UNSAMP_PORT_GRP_PV'] == 'A211' and row['UNSAMP_REGION_GRP_PV'] == '4' and row['ARRIVEDEPART'] == 1:
        row['UNSAMP_PORT_GRP_PV'] = 'A221'
    if row['UNSAMP_PORT_GRP_PV'] == 'A241' and row['UNSAMP_REGION_GRP_PV'] == '8' and row['ARRIVEDEPART'] == 1:
        row['UNSAMP_PORT_GRP_PV'] = 'A201'

    if row['UNSAMP_PORT_GRP_PV'] == 'RSS' and row['ARRIVEDEPART'] == 1:
        row['UNSAMP_PORT_GRP_PV'] = 'HBN'
    if row['UNSAMP_PORT_GRP_PV'] == 'RSS' and row['ARRIVEDEPART'] == 2:
        row['UNSAMP_PORT_GRP_PV'] = 'HBN'
    return row


def modify_values(row, pvs, dataset):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Applies the PV rules to the specified dataframe on a row by row basis.
    Parameters   : row - the row of a dataframe passed to the function through the 'apply' statement called
                   pvs - a collection of pv names and statements to be applied to the dataframe's rows.
                   dataset -  and identifier used in the executed pv statements.
    Returns      : a modified row to be reinserted into the dataframe.
    Requirements : this function must be called through a pandas apply statement.
    Dependencies : NA
    """

    for pv in pvs:
        code = str(pv[1])
        try:
            exec(code)
        except KeyError:
            print("Key Not Found")

    if dataset in ('survey', 'shift'):
        row['SHIFT_PORT_GRP_PV'] = str(row['SHIFT_PORT_GRP_PV'])[:10]
    
    return row


def get_pvs(conn=None):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Extracts the PV data from the process_variables table.
    Parameters   : conn - a connection object linking  the database.
    Returns      : a collection of pv names and statements
    Requirements : NA
    Dependencies : NA
    """

    # Connect to the database
    if conn is None:
        conn = cf.get_sql_connection()

    # Create a cursor object from the connection
    cur = conn.cursor()

    # Specify the sql query for retrieving the process variable statements from the database
    sql = """SELECT 
                PROCVAR_NAME,PROCVAR_RULE
             FROM 
                SAS_PROCESS_VARIABLE
             ORDER BY 
                PROCVAR_ORDER"""

    # Execute the sql query
    cur.execute(sql)

    # Return the pv statements obtained from the sql query
    return cur.fetchall()

    
def process(in_table_name, out_table_name, in_id, dataset):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Runs the process variables step of the IPS calculation process.
    Parameters   : in_table_name - the table where the data is coming from.
                   out_table_name - the destination table where the modified data will be sent.
                   in_id - the column id used in the output dataset (this is used when the data is merged into the main
                           table later.
                   dataset - an identifier for the dataset currently being processed.
    Returns      : NA
    Requirements : NA
    Dependencies : NA
    """

    # Ensure the input table name is capitalised
    in_table_name = in_table_name.upper()

    # Extract the table's content into a local dataframe
    df_data = cf.get_table_values(in_table_name)

    # Fill nan values (Is this needed?)
    df_data.fillna(value=np.NaN, inplace=True)

    # Get the process variable statements
    process_variables = get_pvs()

    if dataset == 'survey':
        df_data = df_data.sort_values('SERIAL')

    # Apply process variables
    df_data = df_data.apply(modify_values, axis=1, args=(process_variables, dataset, out_table_name))

    # Create a list to hold the PV column names
    updated_columns = []

    # Loop through the pv's
    for pv in process_variables:
        updated_columns.append(pv[0].upper())

    # Generate a column list from the in_id column and the pvs for the current run
    columns = [in_id] + updated_columns
    columns = [col.upper() for col in columns]
    # Create a new dataframe from the modified data using the columns specified
    df_out = df_data[columns]

    for column in df_out:
        if df_out[column].dtype == np.int64:
            df_out[column] = df_out[column].astype(int)

    # Insert the dataframe to the output table
    cf.insert_dataframe_into_table(out_table_name, df_out)
