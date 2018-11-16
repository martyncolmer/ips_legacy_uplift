from main.io import CommonFunctions as cf
import pandas as pd
import sys
import random

pv_name = 'UNSAMP_REGION_GRP_PV'

val = """
'
dvpc = 0
row[''ARRIVEDEPART''] = int(row[''ARRIVEDEPART''])
if dataset == ''survey'':
    if not math.isnan(row[''DVPORTCODE'']):
        dvpc = int(row[''DVPORTCODE''] / 1000)
    if row[''PORTROUTE''] < 300:
        if row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if row[''FLOW''] in (1,3):
                row[''REGION''] = row[''RESIDENCE'']
            elif row[''FLOW''] in (2,4):
                row[''REGION''] = row[''COUNTRYVISIT'']
            else:
                row[''REGION''] = ''''
        else:
            row[''REGION''] = dvpc
        if row[''REGION''] in (8, 20, 31, 40, 51, 56, 70, 100, 112, 191, 203, 208, 233, 234, 246, 250, 268, 276, 348, 352, 380, 398, 417, 428, 440, 442, 492, 498, 499, 528, 578, 616, 642, 643, 674, 688, 703, 705, 752, 756, 762, 795, 804, 807, 860, 940, 942, 943, 944, 945, 946, 950, 951):
            row[''UNSAMP_REGION_GRP_PV''] = ''1''
        elif row[''REGION''] in (124, 304, 630, 666, 840, 850):
            row[''UNSAMP_REGION_GRP_PV''] = ''2''
        elif row[''REGION''] in (4, 36, 50, 64, 96, 104, 116, 126, 144, 156, 158, 242, 356, 360, 408, 410, 418, 446, 458, 462, 496, 524, 554, 586, 608, 626, 702, 704, 764):
            row[''UNSAMP_REGION_GRP_PV''] = ''3''
        elif row[''REGION''] in (12, 24, 48, 72, 108, 120, 132, 140, 148, 174, 178, 180, 204, 226, 231, 232, 262, 266, 270, 288, 324, 348, 384, 404, 426, 430, 434, 450, 454, 466, 478, 480, 504, 508, 516, 562, 566, 624, 646, 654, 678, 686, 690, 694, 706, 710, 716, 732, 736, 748, 768, 788, 800, 818, 834, 854, 894):
            row[''UNSAMP_REGION_GRP_PV''] = ''4''
        elif row[''REGION''] == 392:
            row[''UNSAMP_REGION_GRP_PV''] = ''5''
        elif row[''REGION''] == 344:
            row[''UNSAMP_REGION_GRP_PV''] = ''6''
        elif row[''REGION''] in (16, 28, 32, 44, 48, 52, 60, 68, 76, 84, 90, 92, 136, 152, 166, 170, 184, 188, 192, 212, 214, 218, 222, 238, 254, 258, 296, 308, 312, 316, 320, 328, 332, 340, 364, 368, 376, 388, 400, 414, 422, 474, 484, 500, 512, 520, 530, 533, 540, 548, 558, 580, 581, 584, 591, 598, 604, 634, 638, 659, 660, 662, 670, 682, 690, 740, 760, 776, 780, 784, 796, 798, 858, 862, 882, 887, 949):
            row[''UNSAMP_REGION_GRP_PV''] = ''7''
        elif row[''REGION''] == 300:
            row[''UNSAMP_REGION_GRP_PV''] = ''8''
        elif row[''REGION''] in (292, 620, 621, 911, 912):
            row[''UNSAMP_REGION_GRP_PV''] = ''9''
        elif row[''REGION''] in (470, 792, 901, 902):
            row[''UNSAMP_REGION_GRP_PV''] = ''10''
        elif row[''REGION''] == 372:
            row[''UNSAMP_REGION_GRP_PV''] = ''11''
        elif row[''REGION''] in (831, 832, 833, 931):
            row[''UNSAMP_REGION_GRP_PV''] = ''12''
        elif row[''REGION''] in (921, 923, 924, 926, 933):
            row[''UNSAMP_REGION_GRP_PV''] = ''13''
elif dataset == ''unsampled'':
    if not math.isnan(row[''REGION'']):
        row[''REGION''] = int(row[''REGION''])
        row[''UNSAMP_REGION_GRP_PV''] = str(row[''REGION''])
        
if row[''UNSAMP_PORT_GRP_PV''] == ''A201'' and row[''UNSAMP_REGION_GRP_PV''] == ''7'' and row[''ARRIVEDEPART''] == 2:
    row[''UNSAMP_PORT_GRP_PV''] = ''A191''
if row[''UNSAMP_PORT_GRP_PV''] == ''HGS'':
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
if row[''UNSAMP_PORT_GRP_PV''] == ''E921'':
    row[''UNSAMP_PORT_GRP_PV''] = ''E911''
if row[''UNSAMP_PORT_GRP_PV''] == ''E951'':
    row[''UNSAMP_PORT_GRP_PV''] = ''E911''

if row[''UNSAMP_PORT_GRP_PV''] == ''A181'' and row[''UNSAMP_REGION_GRP_PV''] == ''6'' and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''A151''
if row[''UNSAMP_PORT_GRP_PV''] == ''A211'' and row[''UNSAMP_REGION_GRP_PV''] == ''4'' and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''A221''
if row[''UNSAMP_PORT_GRP_PV''] == ''A241'' and row[''UNSAMP_REGION_GRP_PV''] == ''8'' and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''A201''

if row[''UNSAMP_PORT_GRP_PV''] == ''RSS'' and row[''ARRIVEDEPART''] == 1:
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
if row[''UNSAMP_PORT_GRP_PV''] == ''RSS'' and row[''ARRIVEDEPART''] == 2:
    row[''UNSAMP_PORT_GRP_PV''] = ''HBN''
'
"""


def write_pv_to_table(pv_name,value,conn = None):

    if(conn == None):
        conn = cf.get_sql_connection()

    sql = "update PROCESS_VARIABLE_PY set PV_DEF = " + val + " where (PV_NAME = '" + pv_name + "')"
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def read_pv_table(pv_name = None,conn = None):

    if(conn == None):
        conn = cf.get_sql_connection()

    if(pv_name == None):
        sql = "select PV_NAME, PROCESS_VARIABLE_ID, PV_DEF from PROCESS_VARIABLE_PY ORDER BY PROCESS_VARIABLE_ID"
        print(sql)
        cur = conn.cursor()
        cur.execute(sql)
        process_variables = cur.fetchall()
        for rec in process_variables:
            # Output process variable name and definition
            print(rec[0])
            print("")
            print(rec[2])
            print("")
            print("")

    else:
        sql = "select PV_NAME, PV_DEF from PROCESS_VARIABLE_PY where (PV_NAME = '" + pv_name + "')"
        cur = conn.cursor()
        cur.execute(sql)
        process_variables = cur.fetchall()
        # Output process variable name and definition
        print('--Process Variable--')
        print(process_variables[0][0])
        print("")
        print('--Statement--')
        print(process_variables[0][1])
        print("")

    print(len(process_variables))

write_pv_to_table(pv_name, val)
read_pv_table(pv_name)