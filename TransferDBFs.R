if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, foreign, RCurl, zip, here, DBI, RPostgreSQL, gtools, RPostgres)

readRenviron(".Renviron")

repair <- "-repair" %in% commandArgs()
if(repair){
   print("repair mode")
}


baseDir <- case_when(Sys.info()[['sysname']] == "Windows" ~ "c:/scripts", .default = here())
readRenviron(paste(baseDir, "/.Renviron", sep = ""))

source(paste(baseDir, "/functions.R", sep = ""))

con <- getDBConnection()

AlohaPath <- case_when(Sys.info()[['sysname']] == "Windows" ~ "c:/BootDrv/Aloha", .default = paste(here(), "/data", sep = "") ) 
#AlohaPath <- "C:/BootDrv/Aloha"
#AlohaPath <- paste(here(),"/data", sep = "")


dataSourceID <- getAlohaDataSourceId(con)

existingImports <- getExistingImportRecords(dataSourceID, con)

folders <- list.dirs(AlohaPath, recursive = FALSE)
folders <- folders[grepl("/\\d{8}$", folders)]
temp <- as.Date(sub('^\\S+([0-9]{8})', '\\1', folders), "%Y%m%d")
folders <- folders[order(temp, decreasing = TRUE)]


for (folder in folders ) {
  if(folderIsValid(folder)){
    #print(folder)
    bname <- basename(folder)
    folderDate <- as.Date(bname, format = "%Y%m%d")
    ini <- read.csv2(paste(folder, "/Aloha.ini", sep = ""), sep = "=", skip = 1, header = FALSE)
    rNum <- as.integer( ini[ini$V1 == "UNITNUMBER",]["V2"])
    entityNumber <- getEntityId(rNum, con)

    if(! importExistsInDB(folderDate, entityNumber, con)){
 
      insertGrindFiles(folderDate,  folder, entityNumber, dataSourceID, con)
    }
  }
}

dbDisconnect(con)