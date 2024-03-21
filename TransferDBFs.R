if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, foreign, RCurl, zip, here, DBI, RPostgreSQL)

source(paste(here(), "/functions.R", sep = ""))

con <- getDBConnection()

#AlohaPath <- "C:/BootDrv/Aloha"
AlohaPath <- paste(here(),"/data", sep = "")


#datesToImport <- getDatesToImport()


folders <- list.dirs(AlohaPath) # this assumes being in the main folder, otherwise specify the path
folders <- folders[grepl("/\\d{8}$", folders)]

for (folder in folders ) {
  print(folder)
  bname <- basename(folder)
  folderDate <- as.Date(bname, format = "%Y%m%d")
  ini <- read.csv2(paste(folder, "/Aloha.ini", sep = ""), sep = "=", skip = 1, header = FALSE)
  rNum <- as.integer( ini[ini$V1 == "UNITNUMBER",]["V2"])
  entityNumber <- getEntityId(rNum, con)
  if(! importExistsInDB(folderDate, entityNumber, con)){
    print("import")
    #insertAlohaDBF(folderDate,  folder, entityNumber, con)
  }
  
  #print(rName)
}

dbDisconnect(con)