from IPSTransformation import CommonFunctions as cf

def function1():
	"""
	Author			: Richmond Rice
	Date			: Jan 2018
	Purpose			: 
	Parameters		: NA
	Returns			: NA
	Requirements	: IPSTransformation
	Dependencies	: 
	"""

	sas_shift_data_table = 'SAS_SHIFT_DATA'
	shift_data_run_id = '1c6560b7-f1e5-435e-a85a-7efbe94fb301'

	sas_shift_data_insert_query = "INSERT INTO " + sas_shift_data_table + " \
		(REC_ID, PORTROUTE, WEEKDAY, ARRIVEDEPART, TOTAL, AM_PM_NIGHT) \
		(SELECT REC_ID_S.NEXTVAL, SD.PORTROUTE, SD.WEEKDAY, SD.ARRIVEDEPART, \
		SD.TOTAL, SD.AM_PM_NIGHT \
		FROM SHIFT_DATA SD WHERE SD.RUN_ID = '" + shift_data_run_id + "')"

	cf.delete_from_table(sas_shift_data_table)

	conn = cf.get_oracle_connection()
	cur = conn.cursor()
	cur.execute(sas_shift_data_insert_query)

def function2(): # Copy shift wt pvs for survey data
	"""
	Author			: Richmond Rice
	Date			: Jan 2018
	Purpose			: 
	Parameters		: NA
	Returns			: NA
	Requirements	: IPSTransformation
	Dependencies	: 
	"""

	sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
	sas_shift_spv_table = 'SAS_SHIFT_SPV'
	process_variable_run_id = 'IPSSeedRunFR02'

	sas_process_variable_insert_query = "INSERT INTO " + sas_process_variable_table + " \
		(PROCVAR_NAME, PROCVAR_RULE, PROCVAR_ORDER)(SELECT PV.PV_NAME, PV.PV_DEF, 0 \
		FROM PROCESS_VARIABLE PV WHERE PV.RUN_ID = '" + process_variable_run_id + "' \
		AND UPPER(PV.PV_NAME) IN ('SHIFT_PORT_GRP_PV', 'WEEKDAY_END_PV', \
		'AM_PM_NIGHT_PV', 'SHIFT_FLAG_PV', 'CROSSINGS_FLAG_PV'))"

	cf.delete_from_table(sas_process_variable_table)
	cf.delete_from_table(sas_shift_spv_table)

	conn = cf.get_oracle_connection()
	cur = conn.cursor()
	cur.execute(sas_process_variable_insert_query)

def function3(): # Update Shift Data with PVs Output 
	"""
	Author			: Richmond Rice
	Date			: Jan 2018
	Purpose			: 
	Parameters		: NA
	Returns			: NA
	Requirements	: IPSTransformation
	Dependencies	: 
	"""

	sas_shift_pv_table = 'SAS_SHIFT_PV'
	sas_shift_wt_table = 'SAS_SHIFT_WT'
	sas_process_variable_table = 'SAS_PROCESS_VARIABLE'
	sas_ps_shift_data_table = 'SAS_PS_SHIFT_DATA'

	sas_shift_data_update_query = "UPDATE SAS_SHIFT_DATA SSD SET (SSD.SHIFT_PORT_GRP_PV, \
		SSD.WEEKDAY_END_PV, SSD.AM_PM_NIGHT_PV) = (SELECT SSP.SHIFT_PORT_GRP_PV, \
		SSP.WEEKDAY_END_PV,SSP.AM_PM_NIGHT_PV FROM " + sas_shift_pv_table + "SSP \
		WHERE SSD.REC_ID = SSP.REC_ID)"

	print(sas_shift_data_update_query)

	conn = cf.get_oracle_connection()
	cur = conn.cursor()
	cur.execute(sas_shift_data_update_query)

	cf.delete_from_table(sas_shift_pv_table)
	cf.delete_from_table(sas_shift_wt_table)
	cf.delete_from_table(sas_process_variable_table)
	cf.delete_from_table(sas_ps_shift_data_table)