getDBConnection <- function() {
  db <- "lhrc_data"
  db_host <- "blueridgegrill.cxond9zlkmhk.us-east-1.rds.amazonaws.com"
  db_port <- "5432"

  db_user <- Sys.getenv('db_user')
  db_pass <- Sys.getenv('db_pass')
  con <- dbConnect(RPostgres::Postgres(), dbname = db, host = db_host, port = db_port, user = db_user, password = db_pass)
  con
}

getCurrentFileLocation <- function() {
  this_file <- commandArgs() %>%
    tibble::enframe(name = NULL) %>%
    tidyr::separate(col = value, into = c("key", "value"), sep = "=", fill = "right") %>%
    dplyr::filter(key == "--file") %>%
    dplyr::pull(value)
  if (length(this_file) == 0) {
    this_file <- rstudioapi::getSourceEditorContext()$path
  }
  return(dirname(this_file))
}

getExistingImportRecords <- function(dataSourceID, con) {
  query <- "select di.data_import_id, di.entity_id, di.import_date, count(i.*) from data_imports di "
  query <- paste(query, "inner join alohadbf_gnditem i on i.data_import_id ", sep = "")
  query <- paste(query, "= di.data_import_id where di.import_source_id = ", dataSourceID, sep = "")
  query <- paste(query, " group by di.data_import_id, di.entity_id, di.import_date")
  result <- dbGetQuery(con, query)
}

importRecordExists <- function(DF, date, entityId) {
  DF %>%
    filter(entity_id == entityId, import_date == date) %>%
    count() > 0
}

importRecordIsComplete <- function() {

}

deleteImportRecord <- function(importId, con) {
  query1 <- "select table_schema,table_name from information_schema.tables "
  query1 <- paste(query1, "where table_name like 'alohadbf%' ", sep = "")
  query1 <- paste(query1, "and table_schema not in ('information_schema', 'pg_catalog') ", sep = "")
  query1 <- paste(query1, " and table_type = 'BASE TABLE' order by table_name, table_schema;", sep = "")
  df <- dbGetQuery(con, query1)
  for (row in 1:nrow(df)) {
    tableName <- df[row, "table_name"]
    print(tableName)
  }
  # query2 <- str::glue("delete from {} where data_import_id = {}")
  # query3 <- str::glue("delete from data_imports where data_import_id = {}")
}

getEntityId <- function(alohaId, con) {
  query <- paste("select entity_id from entities where aloha_id = ", alohaId, sep = "")
  df <- dbGetQuery(con, query)
  as.integer(df[1, 1])
}

# getDatesToImport <- function(folderDates, entity, con){
#   query <- "select * from data_imports where entity_id = and import_source_id = and import date in ()"
#
# }

getAlohaDataSourceId <- function(con) {
  query <- "select data_import_source_id from data_import_sources where lower(data_import_source_name) = 'aloha'"
  df <- dbGetQuery(con, query)
  df[[1]]
}

importExistsInDB <- function(date, entity, con) {
  query <- paste("select count(*) from data_imports where import_date = '", date, sep = "")
  query <- paste(query, "' and import_source_id = ", getAlohaDataSourceId(con), sep = "")
  query <- paste(query, "and entity_id = ", entity, sep = "")
  df <- dbGetQuery(con, query)
  df[[1]] > 0
}


folderIsValid <- function(folder) {
  isValid <- FALSE
  ini <- paste(folder, "/Aloha.ini", sep = "")
  line <- paste(folder, "/GNDLINE.dbf", sep = "")

  if (file.exists(ini) & file.exists(line) & file.size(line) > 1000) {
    iniDF <- read.csv2(paste(folder, "/Aloha.ini", sep = ""), sep = "=", skip = 1, header = FALSE)
    rNum <- as.integer(iniDF[iniDF$V1 == "UNITNUMBER", ]["V2"])
    if (!is.na(rNum)) {
      isValid <- TRUE
    }
  }
  isValid
}

insertGrindFiles <- function(grindDate, folder, entityNumber, dataSourceID, con) {
  isError <- FALSE


  dbWithTransaction(
    con,
    {
      maxImportIdQuery <- paste("select max(data_import_id) from data_imports where entity_id = ", entityNumber, " and import_source_id = ", dataSourceID)
      previousImportId <- dbGetQuery(con, maxImportIdQuery)[1, 1]
      insertQuery <- paste("insert into data_imports (entity_id, import_source_id, import_date) values (", entityNumber, ", ", dataSourceID, ", '", grindDate, "')", sep = "")
      dataImportRecord <- dbExecute(con, insertQuery)
      result <- dbGetQuery(con, maxImportIdQuery)
      dataImportID <- result[1, 1]
      if (dataImportID <= previousImportId) {
        dbBreak()
        isError <- TRUE
      }

      dbf_files <- list.files(path = folder, pattern = "\\.dbf", ignore.case = TRUE, full.names = TRUE, recursive = FALSE)
      for (f in dbf_files) {
        fname <- fs::path_ext_remove(f)
        fname <- tolower(fs::path_file(fname))
        table <- paste("alohadbf_", tolower(fname), sep = "")
        dbf <- read.dbf(f, as.is = T)
        colnames(dbf) <- tolower(colnames(dbf))
        dbf <- dbf %>% select(!contains("hash", ignore.case = TRUE))
        dbf <- dbf %>% select(!contains("ssn", ignore.case = TRUE))
        dbf <- dbf %>% select(!contains("loyalty", ignore.case = TRUE))
        dbf <- dbf %>% mutate(grind_date = grindDate, entity_id = entityNumber, data_import_id = dataImportID)
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
      }
    }
  )
}

