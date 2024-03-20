getDBConnection <- function() {
  db <- "lhrc_data"
  db_host <- "blueridgegrill.cxond9zlkmhk.us-east-1.rds.amazonaws.com"
  db_port <- "5432"

  creds <- read.csv2(paste(here(), "/creds.txt", sep = ""), sep = ",", header = FALSE)
  db_user <- as.character(creds[creds$V1 == "db_user",]["V2"])

  db_pass <- as.character(creds[creds$V1 == "db_pass",]["V2"])
  print(creds)
  print(db_user)
  print(db_pass)
  
  
  # db_user <- "inventory"
  # db_pass <- "1261#Lhrc!3423.Brg"
  
  
  con <- dbConnect(RPostgres::Postgres(), dbname = db, host = db_host, port = db_port, user = db_user, password = db_pass)
  con
}

getEntityId <- function(alohaId, con){
  query <- paste("select entity_id from entities where aloha_id = ", alohaId, sep = "")
  df <- dbGetQuery(con, query)
  as.integer(df[1,1])
}

# getDatesToImport <- function(folderDates, entity, con){
#   query <- "select * from data_imports where entity_id = and import_source_id = and import date in ()"
#   
# }

importExistsInDB <- function(date, entity, con){
  query <- paste("select count(*) from data_imports where import_date = '", date, sep = "")
  query <- paste(query, "' and import_source_id = (select import_source_id from data_import_sources where lower(data_import_source_name) = 'aloha') " , sep = "")
  query <- paste(query, "and entity_id = ", entity, sep = "")
  df <- dbGetQuery(con, query)
  df[[1]] > 0
}

insertAlohaDBF <- function(entity, zipFile) {
  if (!is.null(zipFile)) {
    tempdirname <- dirname(zipFile$datapath)
    fname <- unzip(zipFile$datapath)
    dataDir <- (dirname(fname[1]))

    ####### get grind date from file name

    isError <- FALSE
    dbBegin(con)
    # dbExecute()
    dbf_files <- list.files(path = dataDir, pattern = "\\.dbf", ignore.case = TRUE, full.names = TRUE, recursive = FALSE)
    for (f in dbf_files) {
      fname <- fs::path_ext_remove(f)
      fname <- tolower(fs::path_file(fname))
      table <- paste("alohadbf_", tolower(fname), sep = "")
      dbf <- read.dbf(f, as.is = T)
      colnames(dbf) <- tolower(colnames(dbf))
      dbf <- dbf %>% mutate(grind_date = as.Date(gndDate), entity_id = entity_id)
      # print(colnames(dbf))
      print(table)
      tryCatch(
        expr = {
          dbWriteTable(
            con,
            table,
            dbf,
            row.names = FALSE,
            overwrite = FALSE,
            append = TRUE,
            field.types = NULL,
            temporary = FALSE,
            copy = TRUE
          )
        },
        error = function(w) {
          print("error")
          isError <- TRUE
          dbRollback(con)
          break
        },
        warning = function(w) {

        }
      )
    }
    if (!isError) {
      dbCommit(con)
    }
    dbDisconnect(con)
  }
}







