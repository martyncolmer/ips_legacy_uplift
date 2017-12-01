library(RJDBC)
library(sas7bdat)

saszipinput <- "\\\\nsdata3\\Social_Surveys_team\\CASPA\\IPS\\Testing\\testdata.zip" # path of sas zip test data
#saszipinput <- "\\\\nsdata3\\Social_Surveys_team\\CASPA\\IPS\\Testing\\quarter12017.zip" # path of sas zip real data
systime <- format(Sys.time(), "%Y%m%d%H%M%S") # timestamp for unique folder for unzip location
saszipoutput <- paste("D:\\#", systime, sep = "") # path of unzip location
csvinput <- "\\\\nsdata3\\Social_Surveys_team\\CASPA\\IPS\\Testing\\Non Response Q1 2017.csv" # path of csv

unzip(saszipinput, exdir = saszipoutput) # unzip sas data from share to local

#Sys.sleep(10) # temp pause for testing

unzippedsas <- list.files(path = saszipoutput, pattern = "*.sas", full.names = FALSE, recursive = FALSE) # list sas files unzipped

if (length(unzippedsas) == 0) # error if no sas files found
{
  print("error - no sas")
}

if (length(unzippedsas) > 1) # error if more than one sas file found
{
  print("error - too much sas")
}

if (length(unzippedsas) == 1) # import the sas file if only one file found
{
  print("importing sas")
  sasconcat <- paste(saszipoutput, "\\", unzippedsas, sep = "") # concatenate full file path of sas to be imported
  sasdata <- read.sas7bdat(sasconcat) # import the sas file via sas7bdat
  #sasdata <- read_sas(sasconcat) # import the sas file via haven
  #print(sasdatadf)
}

unlink(saszipoutput, recursive = TRUE) # delete local unzipped data

#csvimport <- read.csv(csvinput) # read csv
#print(csvimport)




# proc transpose

tran <- melt(sasdata, id = c("Serial"))

# proc contents

sasattr <- attr(sasdata, "column.info")

filtlist <- lapply(sasattr, `[`, c('name', 'length', 'type'))

sasattrdf <- data.frame(do.call(rbind, filtlist))

sasattrdf <- cbind(sasattrdf, "row" = 1:nrow(sasattrdf))

colnames(sasattrdf) <- c("variable", "length", "type", "row")

valattr <- merge(sasattrdf, tran, by = "variable")






# connect to oracle using jdbc test - roracle package is probably better but it needs to be compiled
#jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="d:/r/lib/ojdbc6.jar")
#jdbcConnection <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@//exa01-scan.ons.statistics.gov.uk:1521/DEVCON", "CASPA_1_POWELD2_DATA", "CASPA_1_POWELD2")
#testQuery <- dbGetQuery(jdbcConnection, "SELECT * from CASPA_1_POWELD2_DATA.SAS_PARAMETERS") # test query
#dbDisconnect(jdbcConnection)
#print(testQuery) # print test query

# write data to oracle test
#jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="d:/r/lib/ojdbc6.jar")
#jdbcConnection <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@//exa01-scan.ons.statistics.gov.uk:1521/DEVCON", "IPS_1_POWELD2_DATA", "IPS_1_POWELD2")
#surcoltestdf <- data.frame(VERSION_ID = 1, COLUMN_NO = c(1,2,3), COLUMN_DESC = c("Shiftno","Questno","Serial"), COLUMN_TYPE = c("number", "number", "number"), COLUMN_LENGTH = c(4,4,6))
#dbWriteTable(jdbcConnection, "SURVEY_COLUMN", surcoltestdf, append=TRUE, overwrite=FALSE)
#dbDisconnect(jdbcConnection)