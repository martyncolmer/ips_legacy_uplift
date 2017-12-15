# Author: Richmond Rice
# Date: 2017-12
# Project: IPS Legacy Uplift
# Purpose: Unzip SAS file and load in SAS data, manipulate SAS data, upload the data to Oracle database.
# Purpose: Load in data from CSV.

library(RJDBC)
library(sas7bdat)
library(reshape2)

# test file - cut down data set
sas_zip_file <-
  "\\\\nsdata3\\Social_Surveys_team\\CASPA\\IPS\\Testing\\testdata.zip"

# proper file - r runs of memory when importing
sas_zip_file_quarter <-
  "\\\\nsdata3\\Social_Surveys_team\\CASPA\\IPS\\Testing\\quarter12017.zip"

# timestamp for unzipping to a unique location
sas_unzip_timestamp <-
  format(Sys.time(), "%Y%m%d%H%M%S")

# path of unzip including the timestamp
sas_unzip_path <-
  paste("D:\\#", sas_unzip_timestamp, sep = "")

# proper csv file
csv_file <-
  "\\\\nsdata3\\Social_Surveys_team\\CASPA\\IPS\\Testing\\Non Response Q1 2017.csv" # csv file

func_unzip <- function(zip_file, output_path)
  # Author: Richmond Rice
  # Date: 2017-12
  # Purpose: Unzip passed in zip file to passed in output location.
{
  tryCatch({
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "unzipping",
      zip_file
    ))
    # check zip file passed exists
    if (!file.exists(zip_file))
    {
      print("error - unzip file does not exist")
      stop("error - unzip file does not exist")
    }
    # check last 4 digits of file passed to see if its a zip file extension
    if (!substr(zip_file, (nchar(zip_file) + 1) - 4, nchar(zip_file)) == ".zip")
    {
      print("error - unzip file is not a zip")
      stop("error - unzip file is not a zip")
    }
    # unzip the zip file passed to the passed output directory
    unzip(zip_file, exdir = output_path)
  },
  warning = function(warn)
  {
    print(warn)
    # stop on warning as corrupt zip file test only raised a warning
    stop(err)
  },
  error = function(err)
  {
    print(err)
    stop(err)
  })
}

func_import_sas <- function(sas_path)
  # Author: Richmond Rice
  # Date: 2017-12
  # Purpose: Import SAS file from the passed in location.
{
  tryCatch({
    # list sas files in directory passed
    sas_files <-
      list.files(
        path = sas_path,
        pattern = "*.sas",
        full.names = FALSE,
        recursive = FALSE
      )
    # stop if no sas files found
    if (length(sas_files) == 0)
    {
      stop("error - no sas file found in directory")
    }
    # stop if more than one sas file found
    if (length(sas_files) > 1)
    {
      stop("error - too many sas files found in directory")
    }
    # attempt to import the sas file is only one found
    if (length(sas_files) == 1)
    {
      print(paste(
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        "importing",
        sas_files
      ))
      # import the sas file
      imported_sas_data <<-
        read.sas7bdat(paste(sas_path, "\\", sas_files, sep = ""))
      print(paste(
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        "imported",
        sas_files
      ))
    }
  },
  warning = function(warn)
  {
    print(warn)
  },
  error = function(err)
  {
    print(err)
    stop(err)
  })
}

func_import_csv <- function(csv_file)
  # Author: Richmond Rice
  # Date: 2017-12
  # Purpose: Import the passed in CSV file.
{
  tryCatch({
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "importing",
      csv_file
    ))
    # import the csv passed
    imported_csv_data <<- read.csv(csv_file)
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "imported",
      csv_file
    ))
  },
  warning = function(warn)
  {
    print(warn)
  },
  error = function(err)
  {
    print(err)
    stop(err)
  })
}

func_del_dir <- function(dir)
  # Author: Richmond Rice
  # Date: 2017-12
  # Purpose: Delete the passed in folder location.
{
  tryCatch({
    print(paste(format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                "deleting",
                dir))
    # delete the directory
    unlink(dir, recursive = TRUE)
    print(paste(format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                "deleted",
                dir))
  },
  warning = function(warn)
  {
    print(warn)
  },
  error = function(err)
  {
    print(err)
    stop(err)
  })
}

func_data_manipulate <- function(df)
  # Author: Richmond Rice
  # Date: 2017-12
  # Purpose: Manipulae data to the required structures.
{
  tryCatch({
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "manipulating",
      deparse(substitute(df))
    ))
    # melt (transpose) by the serial column into a data frame
    meltbyserial <-
      melt(df, id = c("Serial"))
    # also melt the serials so we can join back on them
    meltserials <-
      melt(df,
           id.vars = "Serial",
           measure.vars = "Serial")
    # combine the melted data frames - indexes stay the same
    melted <- rbind(meltbyserial, meltserials)
    # rename the columns in the melted data frame
    colnames(melted) <-
      c("serial_no", "variable", "value")
    # cast the variable column to char
    melted$variable <-
      as.character(melted$variable)
    
    sasattr <-
      attr(df, "column.info") # get column attribute data
    
    filtlist <-
      lapply(sasattr, `[`, c('name', 'length', 'type')) # filter on the attributes we want to a list
    
    sasattrdf <-
      data.frame(do.call(rbind, filtlist)) # make it into a dataframe
    
    sasattrdf <-
      cbind(sasattrdf, "row" = 1:nrow(sasattrdf)) # add row count column
    
    colnames(sasattrdf) <-
      c("variable", "length", "type", "row") # rename columns
    
    sasattrdf <-
      as.data.frame(lapply(sasattrdf, function(X)
        unname(unlist(X)))) # remove list
    
    valattr <-
      merge(sasattrdf, melted, by.x = "variable", by.y = "variable") # merge data frames
    
    surcol <-
      data.frame("1234",
                 valattr$row,
                 valattr$variable,
                 valattr$type,
                 valattr$length) # pull out data for oracle sur col table
    
    surcol <- surcol[order(surcol$valattr.row), ] # order data
    
    surcol <- unique(surcol) # distinct data
    
    surval <-
      data.frame("7777", valattr$serial, valattr$row, valattr$value) # pull out data for oracle sur val table
    
    surval <-
      surval[order(surval$valattr.serial, surval$valattr.row), ] # order data
    
    surval <- subset(surval, surval$valattr.value != "NaN")
    surval <- subset(surval, surval$valattr.value != "")
    
    surval <<- surval
    surcol <<- surcol    
    
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "manipulated",
      deparse(substitute(df))
    ))
  },
  error = function(err)
  {
    print(err)
    stop(err)
  })
}

func_oracle_insert <- function(df, table)
  # Author: Richmond Rice
  # Date: 2017-12
  # Purpose: Insert the passed in dataframe to the passed in table.
{
  tryCatch({
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "inserting",
      deparse(substitute(df)),
      "dataframe to",
      table
    ))
    # insert to oracle
    jdbcDriver <-
      JDBC(driverClass = "oracle.jdbc.OracleDriver", classPath = "d:/r/lib/ojdbc6.jar")
    jdbcConnection <-
      dbConnect(
        jdbcDriver,
        "jdbc:oracle:thin:@//exa01-scan.ons.statistics.gov.uk:1521/DEVCON",
        "IPS_1_POWELD2_DATA",
        "IPS_1_POWELD2"
      )
    dbWriteTable(jdbcConnection,
                 table,
                 df,
                 append = TRUE,
                 overwrite = FALSE)
    dbDisconnect(jdbcConnection)
    print(paste(
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      "inserted",
      deparse(substitute(df)),
      "dataframe to",
      table
    ))
  },
  warning = function(warn)
  {
    print(warn)
  },
  error = function(err)
  {
    print(err)
    stop(err)
  })
}

func_unzip(sas_zip_file, sas_unzip_path)
func_import_sas(sas_unzip_path)
func_del_dir(sas_unzip_path)
func_import_csv(csv_file)
func_data_manipulate(imported_sas_data)
func_oracle_insert(surcol, "SURVEY_COLUMN")
func_oracle_insert(surval, "SURVEY_VALUE")