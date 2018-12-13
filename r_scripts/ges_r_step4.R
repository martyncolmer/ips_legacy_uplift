# https://cran.r-project.org/web/packages/

# This is required on Nassir's laptop - START
#install.packages("D:/R/lib/ReGenesees_1.9.zip", lib = "D:/R/lib/", repos = NULL)
#install.packages("D:/R/lib/RODBC_1.3-15.zip",  lib = "D:/R/lib/", repos = NULL)
# This is required on Nassir's laptop - END

#install.packages("C:/Applications/RStudio/R-3.4.4/library/ReGenesees_1.9.zip", repos = NULL)
#install.packages("C:/Applications/RStudio/R-3.4.4/library/RODBC_1.3-15.zip", repos = NULL)

library(ReGenesees)
library(RODBC)

#library("ReGenesees", lib.loc="D:/R/lib/")

require(ReGenesees)
require(RODBC)

# Start the clock!
ptm <- proc.time()

##### ODBC connection for Microsoft SQL Server
ch <- odbcDriverConnect(
  "Driver=SQL Server; Server=CR1VWSQL14-D-01; Database=ips_test; UID=ips_dev; Pwd=ips_dev"
)

poprowvec <- sqlFetch(ch, "dbo.poprowvec_traffic")
survey_input_aux <- sqlFetch(ch, "dbo.survey_traffic_aux")

#close the ODBC connection
odbcClose(ch)

# set work directory
# setwd("R:/CASPA/IPS/Testing/Q3 2017/traffic weight")

# read in population data
# poprowvec_traffic<-read.csv("poprowvec.csv")


# delete columns not needed
poprowvec$C_group <- NULL
poprowvec$index <- NULL
#sqlSave(ch, poprowvec_traffic, tablename = NULL)

# read in survey data
# survey_traffic_aux<-read.csv("survey_input_aux.csv")
#sqlSave(ch, survey_traffic_aux, tablename = NULL)

#delete records with missing design weight
survey_input_aux$trafDesignWeight[survey_input_aux$trafDesignWeight==0] <- NA
survey_input_aux <- survey_input_aux[complete.cases(survey_input_aux$trafDesignWeight),]
str(survey_input_aux)

#declare factors
survey_input_aux[,"T_"]<-factor(survey_input_aux[,"T1"])

#run for traffic

#set up survey design
des<-e.svydesign(data=survey_input_aux,ids=~SERIAL,weights=~trafDesignWeight)

df.population<-as.data.frame(poprowvec)

pop.template(data=survey_input_aux,calmodel=~T_-1)
population.check(df.population,data=survey_input_aux,calmodel=~T_-1)

# Stop the clock
proc.time() - ptm

#call regenesees
survey_input_aux[,"tw_weight"]<-weights(e.calibrate(des,df.population,calmodel=~T_-1,calfun="linear",aggregate.stage=1))

#survey_input_aux$TRAFFIC_WEIGHT <- NULL
#survey_input_aux$test <- NULL

survey_input_aux[,"TRAFFIC_WT"]<-survey_input_aux[, "tw_weight"] / survey_input_aux[, "trafDesignWeight"]

# write.csv(survey_input_aux, "R:/CASPA/IPS/Testing/Q3 2017/traffic weight/r_traffic.csv")

r_traffic <- survey_input_aux

# open sql channel
ch <- odbcDriverConnect(
  "Driver=SQL Server; Server=CR1VWSQL14-D-01; Database=ips_test; UID=ips_dev; Pwd=ips_dev"
)

# Write data frame to sql
sqlSave(ch, r_traffic, tablename = NULL)

#close the ODBC connection
odbcClose(ch)

# Stop the clock
proc.time() - ptm
