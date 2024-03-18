if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, foreign, RCurl, zip, here, DBI, RPostgreSQL)

here::here()
zipFolder <- paste(here(), "/zips", sep = "")


AlohaPath <- "C:/Aloha"


if (! file.exists(zipFolder)){
  dir.create(zipFolder)
} 

folders <- list.dirs(AlohaPath) # this assumes being in the main folder, otherwise specify the path
folders <- folders[grepl("\d{8}"), folders]

for (f in folders ) {
  print(f)
  zip(zipfile = paste(zipFolder, f, sep = ""), files = list.files(f))
}
#Aloha.ini
#UNITNAME=Bones