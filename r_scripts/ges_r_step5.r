# https://cran.r-project.org/web/packages/

#install.packages("C:/Applications/RStudio/R-3.4.4/library/ReGenesees_1.9.zip", repos = NULL)
#install.packages("C:/Applications/RStudio/R-3.4.4/library/RODBC_1.3-15.zip", repos = NULL)

#library(ReGenesees)
#library(RODBC)

require(ReGenesees)
require(RODBC)

# Start the clock!
ptm <- proc.time()

##### ODBC connection for Microsoft SQL Server
ch <- odbcDriverConnect(
  "Driver=ODBC Driver 17 for SQL Server; Server=localhost; Database=ips; UID=ips; Pwd=yourStrong123Password"
)

poprowvec <- sqlFetch(ch, "dbo.poprowvec_unsamp")
survey_input_aux <- sqlFetch(ch, "dbo.survey_unsamp_aux")

#close the ODBC connection
odbcClose(ch)

#set work directory
# setwd("R:/CASPA/IPS/Testing/oct Data/unsampled weight")

#read in population data
#poprowvec<-read.csv("test_mod_totals.csv")

#delete columns not needed
poprowvec$C_group <- NULL
#sqlSave(ch, poprowvec_unsamp, tablename = NULL)

#read in survey data
# survey_input_aux<-read.csv("test_r_input.csv")
#sqlSave(ch, survey_unsamp_aux, tablename = NULL)

#delete records with missing design weight
survey_input_aux$OOHDesignWeight[survey_input_aux$OOHDesignWeight==0] <- NA
survey_input_aux <- survey_input_aux[complete.cases(survey_input_aux$OOHDesignWeight),]
str(survey_input_aux)

#declare factors
survey_input_aux[,"T_"]<-factor(survey_input_aux[,"T1"])

#run for traffic

#set up survey design
des<-e.svydesign(data=survey_input_aux,ids=~SERIAL,weights=~OOHDesignWeight)

df.population<-as.data.frame(poprowvec)

pop.template(data=survey_input_aux,calmodel=~T_-1)
population.check(df.population,data=survey_input_aux,calmodel=~T_-1)

#call regenesees
survey_input_aux[,"unsamp_traffic_weight"]<-weights(e.calibrate(des,df.population,calmodel=~T_-1,calfun="linear",aggregate.stage=1))

r_unsampled <- survey_input_aux

r_unsampled[,"UNSAMP_TRAFFIC_WT"]<-r_unsampled[, "unsamp_traffic_weight"] / r_unsampled[, "OOHDesignWeight"]

# open sql channel
ch <- odbcDriverConnect(
  "Driver=SQL Server; Server=CR1VWSQL14-D-01; Database=ips_test; UID=ips_dev; Pwd=ips_dev"
)

# Write data frame to sql
sqlSave(ch, r_unsampled, tablename = NULL)

#close the ODBC connection
odbcClose(ch)

# Stop the clock
proc.time() - ptm

#write.csv(survey_input_aux, "r:/caspa/ips/testing/q3 2017/unsampled weight/test_dec_ooh.csv")

