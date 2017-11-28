'''
Created on 11 Sep 2017

@Author: Thomas Mahoney
'''


def calculate_ips_nonresponse_weight():
    
    """ Setup Oracle Access """
    
    oralib_access(schema = scheme,dbase = dbase);                                #Connect to Oracle
    ips_error_check()                                                            #Check for Errors
    
    """ Get Parameters from table (unload_parameters2 apparently does this without compressing out spaces so guessing theres an unload_parameters1 that doesnt... merge to one function?)"""
    unload_parameters2()
    ips_error_check()
    
    

    outputData = ora_data.outputData
    summaryData = ora_data.summaryData
    nonResponseData = ora_data.nonResponseData
    
    """ Create a SAS data set from the survey data """
    data surveyData(drop = &var_NRWeight)
    set ora_data.&SurveyData;
    run;
    ips_error_check()

    """ Create a SAS data set from the survey data """
    data nonResponseData
    set &nonResponseData
    run;
    ips_error_check()   
    
    do_ips_nrweight_calculation(surveyData, nonResponseData, out, summary)
    
    """ Append data to output table """
    proc append base = &outputData data=out force;
    run;
    
    ips_error_check()
    
    """ Append summary information to table """
    proc append base=&summaryData data=summary force;
    run;
    
    ips_error_check()
    
    """ Commit the response information to the response table """
    commit_ips_sas_response()
#end


def commit_ips_sas_response():
    pass
    
    
def oralib_access(schema,dbase):
    pass


def unload_parameters2():
    pass


def ips_error_check():
    pass


def do_ips_nrweight_calculation(in, NRData, out, summary, NRStratumDef,ShiftsStratumDef, var_NRtotals, var_NonMigTotals, var_migSI, var_TandTSI, var_PSW, var_NRFlag, var_migFlag, var_respCount, var_NRWeight, var_meanSW,var_priorSum,var_meanNRW, var_grossResp, var_gnr, var_serialNum, minCountThresh):
    
    
    
    pass
