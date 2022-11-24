library(DBI)
library(RSQLite)
library(glue)


#' Add a table to a SQLite db (or append if it exists)
#'
#' @param tbl_name
#' @param df data.frame or sf
#' @param key unique ID to use to identify duplicates if appending to existing table: defaults to ALL columns
#' @param db_name
#' @param overwrite
#'
#' @importFrom glue glue
#'
#' @return
#' @export
#'
#' @examples
#' \dontrun{
#'  write_table  (df = iris,
#' tbl_name = 'iris_test_tbl',
#' key = NULL,
#' db_name = here::here('inst','extdata', 'iris_test.db'),
#' overwrite= T)
#' }
write_table <- function (df, ...) {
  UseMethod("write_table", df)
}


write_table.sf <- function(df,
                           tbl_name,
                           db_name,
                           key = NULL,
                           overwrite = F) {


  if(is.null(key)) key <- sf::st_drop_geometry(df) %>% colnames() # don't use the geometry

  # Connect to DB
  conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)

  # Check if Db and table exists
  existing_tables <- list_tables_db(conn)


  # DB exists, table does not or we require overwriting
  if(!(tbl_name %in% existing_tables) || overwrite) {
    if(overwrite) {
      print(glue("Table {tbl_name} exists but forcing overwrite"))
      RSQLite::dbExecute(conn, glue("DROP TABLE  if exists  {tbl_name}"))
    }
    else { print(glue("Table {tbl_name} does not exist -> creating it")) }

    # Write to DB
    results <-  sf::st_write(obj = df,
                             dsn = db_name,
                             layer = tbl_name,
                             delete_dsn  = T,   # important to delete the entire thing avoid any fuck-up
                             driver = 'SQLite')

    # DB and table exist and we do not want to overwrite
  }else{

    # Append to DB
    results <- append_new_records(df,
                                  conn,
                                  db_name,
                                  tbl_name,
                                  key)
  }

  # Disconnnect
  dbDisconnect(conn)

  return(results)

}


write_table.data.frame <- function(df,
                                   tbl_name,
                                   db_name,
                                   key = NULL,
                                   overwrite = F){

  # Connect to DB
  conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)

  # Check if Db and table exists
  existing_tables <- list_tables_db(conn)


  # DB exists, table does not or we require overwriting
  if(!(tbl_name %in% existing_tables) || overwrite) {
    if(overwrite) {
      print(glue("Table {tbl_name} exists but forcing overwrite"))
      RSQLite::dbExecute(conn, glue("DROP TABLE  if exists  {tbl_name}"))
    }
    else print(glue("Table {tbl_name} does not exist -> creating it"))

    # Write to DB
    results <-  RSQLite::dbWriteTable(conn=conn,name=tbl_name, value=df)

    # DB and table exist and we do not want to overwrite
  }else{

    # Append to DB
    results <- append_new_records(df,
                                  conn,
                                  db_name,
                                  tbl_name,
                                  key)
  }

  # Disconnnect
  dbDisconnect(conn)

  return(results)
}




list_tables_db <- function(conn){

  tryCatch({
    existing_tables <- RSQLite::dbListTables(conn)
  },error=function(e){
    print(glue("Db {db_name} does not exist -> creating it along with table"))
    existing_tables <- list()
    return(existing_tables)
  })

  return(existing_tables)
}


#' Append new records to an existing table in a sql lite db
#'
#' @param conn
#' @param tbl_name
#' @param df data.frame or sf
#' @param key unique ID to use to identify duplicates if appending to existing table: defaults to ALL columns
#'
#' @return number of records added
#'
#' @examples
#' \dontrun{
#' shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))
#' conn <- dbConnect(RSQLite::SQLite(), dbname  = here::here('inst','extdata', 'nc_test.db'))
#' shp_nc_rnd <- shp_nc %>% mutate(across(where(is.numeric),  ~ rnorm(length(.),mean = 100))) # random values so these are new records
#' append_new_records(shp_nc_rnd %>% head(10),
#'                   conn,
#'                   db_name =  here::here('inst','extdata', 'nc_test.db') ,
#'                   tbl_name = 'nc_tbl',
#'                   key=NULL) # use all columns + ignore geometry
#'
#' }
#'
append_new_records <- function(x,...){
  UseMethod("append_new_records",x)
}



append_new_records.sf <- function(df,
                                  conn,
                                  db_name,
                                  tbl_name,
                                  key ){

  # Make sure table really exists
  existing_tables <- RSQLite::dbListTables(conn)
  assertthat::assert_that(length(existing_tables) > 0 ,
                          msg = 'Fatal error! Table does not exist')

  print(glue('Table {tbl_name} exists: appending '))

  # Check for existing records and try to avoid deduping

  ## Create Tmp table with new results -- use basic dataframe -- fucking sf and sqlite driver is impossibly complicated
  RSQLite::dbExecute(conn, "DROP TABLE  if exists tmp")
  RSQLite::dbWriteTable(conn, 'tmp', df %>% sf::st_drop_geometry())

  ## Create the key to use for merging and checking dups
  if(is.null(key))  key <- colnames(df) # use all columns by default if not specified
  if(any('geometry' %in% tolower(key))) {
    warning('Warning! geometry column used as a key to check for duplicates in sf object: dropping')
    key <- key[tolower(key) != 'geometry']
  }

  key_for_merge <- key
  key <- paste0( '"', key,  '"') # quote the columns
  key_str <- paste0(key, collapse = ",")
  right <- paste0(  "t_right.",  key )
  sql_is_null_clause <- paste0(
    paste0(  right , " IS NULL") ,
    collapse = " AND "
  )

  left <-  paste0(  't_left.',  key )
  right <- paste0(  't_right.',  key )
  sql_on_clause <- paste0( left, '=', right  , collapse = ' AND ')

  ## Get new records in tmp with no matching record in {tbl_name}
  sql_anti_join_clause <- glue("
      SELECT *
      FROM tmp as t_left
      LEFT JOIN {tbl_name} as t_right
      ON {sql_on_clause}
      WHERE {sql_is_null_clause};
    ")


  # This is only to get the keys of the new df
  df_to_add <- RSQLite::dbGetQuery(conn, sql_anti_join_clause)
  new_records <- nrow(df_to_add)

  shp_append <- df %>% dplyr::inner_join(df_to_add, on= key_for_merge)

  if(new_records > 0){
    print(glue('Appending {new_records} new rows to {tbl_name} -- duplicates removed based on keys: {key_str}'))
    sf::st_write(obj=shp_append %>% select(colnames(df)),
                 dsn=db_name,
                 layer=tbl_name,
                 append=T,
                 delete_dsn  = F,
                 driver = 'SQLite')
  }else{
    print(glue('No new records added -- all {nrow(df)} already exist -- duplicates identified based on keys: {key_str}'))
  }

  # Drop tmp table
  dbExecute(conn, 'DROP TABLE  if exists tmp')

  # Disconnnect
  dbDisconnect(conn)


  return(new_records)

}




append_new_records.data.frame <- function(df,
                                          conn,
                                          db_name,
                                          tbl_name,
                                          key ){

  # Make sure table really exists
  existing_tables <- RSQLite::dbListTables(conn)
  assertthat::assert_that(length(existing_tables) > 0 ,
                          msg = 'Fatal error! Table does not exist')

  print(glue('Table {tbl_name} exists: appending '))

  # Check for existing records and try to avoid deduping

  ## Create Tmp table with new results
  RSQLite::dbExecute(conn, 'DROP TABLE  if exists tmp')
  RSQLite::dbWriteTable(conn,'tmp',df)

  ## Create the key to use for merging and checking dups
  if(is.null(key))  key <- colnames(df) # use all columns by default if not specified

  key <- paste0( '"', colnames(df),  '"') # quote the columns
  key_str <- paste0(key,collapse = ',')
  right <- paste0(  't_right.',  key )
  sql_is_null_clause <- paste0(
    paste0(  right , ' IS NULL') ,
    collapse = ' AND '
  )

  left <-  paste0(  't_left.',  key )
  right <- paste0(  't_right.',  key )
  sql_on_clause <- paste0( left, '=', right  , collapse = ' AND ')

  ## Get new records in tmp with no matching record in {tbl_name}
  sql_anti_join_clause <- glue("
      SELECT *
      FROM tmp as t_left
      LEFT JOIN {tbl_name} as t_right
      ON {sql_on_clause}
      WHERE {sql_is_null_clause};
    ")


  df_to_add <- RSQLite::dbGetQuery(conn, sql_anti_join_clause)
  new_records <- nrow(df_to_add)

  if(new_records > 0){
    print(glue('Appending {new_records} new rows to {tbl_name} -- duplicates removed based on keys: {key_str}'))
    RSQLite::dbAppendTable(conn, tbl_name, df)
  }else{
    print(glue('No new records added -- all {nrow(df)} already exist -- duplicates identified based on keys: {key_str}'))
  }

  # Drop tmp table
  dbExecute(conn, 'DROP TABLE  if exists tmp')

  # Disconnnect
  dbDisconnect(conn)


  return(new_records)

}



#' Return a df from a sql lite connection that can be queried lazily with dplyr
#'
#' @param db_name or RSQLite::SQLiteConnection object
#' @param tbl_name
#'
#' @return
#' @export
#'
#' @examples
get_df_tbl <- function(x,...){
  UseMethod('get_df_tbl_dispatch',x)
}


get_df_tbl_dispatch.character <- function(db_name = here::here('inst','extdata', 'revue_technique.db'),
                                          tbl_name= 'policies_2015-2019'){

  conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)
  dplyr::tbl(conn, tbl_name)


}


get_df_tbl_dispatch.SQLiteConnection <- function(conn,
                                                 tbl_name= 'policies_2015-2019'){


  dplyr::tbl(conn, tbl_name)

}


