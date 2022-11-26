
library(RSQLite)
library(glue)
library(dplyr)
library(assertthat)
library(sf)

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


#' write_table for sf object
#'
#' @param df
#' @param tbl_name
#' @param db_name
#' @param key
#' @param overwrite
#'
#' @return
#' @export write_table.sf
#' @export
#'
#' @import RSQLite
#' @importFrom  DBI dbDisconnect
#'
#' @examples
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

    if (overwrite && (tbl_name %in% existing_tables) ) print(glue::glue("Table {tbl_name} exists but forcing overwrite"))
    else print(glue::glue("Table {tbl_name} does not exist -> creating it"))

    if (overwrite) delete_tables(conn, tbl_name)

    # Write to DB
    # Disconnnect - IMPORTANT TO DISCONNECT - OTHERWISE will get errors with st_write which opens a new connection
    DBI::dbDisconnect(conn)

    results <-  sf::st_write(obj = df,
                             dsn = db_name,
                             layer = tbl_name,
                             append = T,
                             delete_dsn  = length(existing_tables) == 0,   # T will delete the entire db ONLY if there are no tables -- can be the case since RSQLite::dbConnect creates an empty file which breaks st_write
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



  return(results)

}



#' write_table for data.frame object
#'
#' @param df
#' @param tbl_name
#' @param db_name
#' @param key
#' @param overwrite
#'
#' @return
#' @export
#' @export write_table.data.frame
#'
#' @examples
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
    if(overwrite) RSQLite::dbExecute(conn, glue::glue("DROP TABLE  if exists  {tbl_name}"))

    if (overwrite && (tbl_name %in% existing_tables) ) print(glue::glue("Table {tbl_name} exists but forcing overwrite"))
    else print(glue::glue("Table {tbl_name} does not exist -> creating it"))

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
  DBI::dbDisconnect(conn)

  return(results)
}




#' Safely list existing tables in db
#'
#' Will nt fail if DB does not exist, it will just return no tables
#'
#' @param conn
#'
#' @return
#' @export
#'
#' @examples
list_tables_db <- function(conn){

  tryCatch({
    existing_tables <- RSQLite::dbListTables(conn)
  },error=function(e){
    print(glue::glue("Db {db_name} does not exist - a fortiori there are no tables"))
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
#' @export
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



#' Append new sf records to an existing table in a sql lite db
#'
#' @param df
#' @param conn
#' @param db_name
#' @param tbl_name
#' @param key
#'
#' @return
#' @export
#' @export append_new_records.sf
#'
#' @examples
append_new_records.sf <- function(df,
                                  conn,
                                  db_name,
                                  tbl_name,
                                  key ){


  # Make sure table really exists
  existing_tables <- RSQLite::dbListTables(conn)
  assertthat::assert_that(length(existing_tables) > 0 ,
                          msg = 'Fatal error! Table does not exist')

  print(glue::glue('Table {tbl_name} exists: appending '))

  # Check for existing records and try to avoid deduping

  ## Create Tmp table with new results -- use basic dataframe -- fucking sf and sqlite driver is impossibly complicated
  RSQLite::dbExecute(conn, "DROP TABLE  if exists tmp")
  RSQLite::dbWriteTable(conn, 'tmp', df %>% sf::st_drop_geometry())

  ## Create the key to use for merging and checking dups
  if(is.null(key))  key <- colnames(df) # use all columns by default if not specified
  if(any('geometry' %in% tolower(key))) {
    warning("Warning! geometry column used as a key to check for duplicates in sf object: dropping")
    key <- key[tolower(key) != "geometry"]
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
  sql_anti_join_clause <- glue::glue("
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

  # Drop tmp table
  RSQLite::dbExecute(conn, 'DROP TABLE  if exists tmp')

  # Disconnnect - IMPORTANT TO DO THIS BEFORE st_write
  DBI::dbDisconnect(conn)

  if(new_records > 0){

    print(glue::glue('Appending {new_records} new rows to {tbl_name} -- duplicates removed based on keys: {key_str}'))
    sf::st_write(obj = shp_append %>% select(colnames(df)),
                 dsn = db_name,
                 layer = tbl_name,
                 append = T,
                 delete_dsn  = F,
                 driver = 'SQLite')
  }else{
    print(glue::glue('No new records added -- all {nrow(df)} already exist -- duplicates identified based on keys: {key_str}'))
  }




  return(new_records)

}




#' Append new df records to an existing table in a sql lite db
#'
#' @param df
#' @param conn
#' @param db_name
#' @param tbl_name
#' @param key
#'
#' @return
#' @export
#' @export append_new_records.data.frame
#'
#' @examples
append_new_records.data.frame <- function(df,
                                          conn,
                                          db_name,
                                          tbl_name,
                                          key ){

  # Make sure table really exists
  existing_tables <- RSQLite::dbListTables(conn)
  assertthat::assert_that(length(existing_tables) > 0 ,
                          msg = 'Fatal error! Table does not exist')

  print(glue::glue('Table {tbl_name} exists: appending '))

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
  sql_anti_join_clause <- glue::glue("
      SELECT *
      FROM tmp as t_left
      LEFT JOIN {tbl_name} as t_right
      ON {sql_on_clause}
      WHERE {sql_is_null_clause};
    ")


  df_to_add <- RSQLite::dbGetQuery(conn, sql_anti_join_clause)
  new_records <- nrow(df_to_add)

  if(new_records > 0){
    print(glue::glue('Appending {new_records} new rows to {tbl_name} -- duplicates removed based on keys: {key_str}'))
    RSQLite::dbAppendTable(conn, tbl_name, df)
  }else{
    print(glue::glue('No new records added -- all {nrow(df)} already exist -- duplicates identified based on keys: {key_str}'))
  }

  # Drop tmp table
  RSQLite::dbExecute(conn, 'DROP TABLE  if exists tmp')

  # Disconnnect
  DBI::dbDisconnect(conn)


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
  UseMethod('get_df_tbl',x)
}


#' Return a df using a db name
#'
#' @param db_name
#' @param tbl_name
#'
#' @return
#' @export
#' @export get_df_tbl.character
#'
#' @examples
get_df_tbl.character <- function(db_name, tbl_name){

  conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)
  return(get_df_tbl.SQLiteConnection(conn, tbl_name))

}


#' Return a df using a SQllite connection
#'
#' @param db_name
#' @param tbl_name
#'
#' @return
#' @export
#' @export get_df_tbl.SQLiteConnection
#'
#' @examples
get_df_tbl.SQLiteConnection <- function(conn, tbl_name){
  return(dplyr::tbl(conn, tbl_name))
}


#' Determine if table exists in db
#'
#' @param x
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
tbl_exists<- function(x,...){
  UseMethod('tbl_exists',x)
}

#' Determine if table exists in db (using db name)
#'
#' @param conn
#' @param tbl
#'
#' @return T if table exists
#' @export
#' @export tbl_exists.character
#'
#' @examples
tbl_exists.character <- function(db_name, tbl){

  conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)
  return(tbl_exists.SQLiteConnection(conn,tbl))

}


#' Determine if table exists in db (using SQL lite connection)
#'
#' @param conn
#' @param tbl
#'
#' @return T if table exists
#' @export
#' @export tbl_exists.SQLiteConnection
#' @examples
tbl_exists.SQLiteConnection <- function(conn, tbl){

  existing_tables <- list_tables_db(conn)
  return(any(tbl %in% existing_tables))

}



#' Delete tables in a SQLite DB (works with all types of data - regular df or spatially indexed data)
#'
#' @param conn (better to pass in a connection to an existing DB to avoid errors with unclosed connections)
#' @param list_tables
#'
#' @return list_deleted_tables
#' @export
#'
#' @examples
delete_tables <- function(conn, list_tables){

  # Get tables before
  tables_before_deletion <- RSQLite::dbListTables(conn)

  # Try deleting all tables if they exist
  lapply(list_tables,
         function(.x){
                   tryCatch({
                      # Try both - spatial object requires  updating "geometry_columns" and potentially "spatial_ref_sys"
                      RSQLite::dbExecute(conn, glue::glue("DROP TABLE  if exists {.x}"))
                      delete_spatial_obj(conn, .x)
                   },error=function(e){
                      print(glue::glue("Error deleting table {.x} - {e}"))
                  })
           }
  )

  # Get tables after
  tables_after_deletion <- RSQLite::dbListTables(conn)

  # Print message
  successful_deletions <- setdiff(tables_before_deletion, tables_after_deletion)
  list_tables_str <- paste0(successful_deletions, collapse = ", ")
  print(glue::glue("Sucessfully deleted {length(successful_deletions)} tables:\n {list_tables_str} "))

  if (length(successful_deletions) < length(list_tables) ){
    print(glue::glue("Did not manage to delete all tables: {length(list_tables) - length(successful_deletions)} failed"))
  }

  # Disconnect
  RSQLite::dbDisconnect(conn)

  return(successful_deletions)

}


#' Try to delete the auxiliary spatial data contained in a spatial lite db
#'
#' Can be important because we might get errors if we delete only
#' a table with associated geometry without updating the geometry_columns table
#'
#' @param conn
#' @param tbl_name
#'
#' @return
#' @export
#'
#' @examples
delete_spatial_obj <- function(conn, tbl_name){


  del_records <- 0

  # SQl lite sequence
  if(RSQLite::dbExistsTable(conn, "sqlite_sequence")){
    n_before <- get_df_tbl(conn, "sqlite_sequence") %>% dplyr::count() %>% pull(n)
    query_del <- glue::glue(
      "DELETE
      FROM sqlite_sequence
      WHERE name = '{tbl_name}' ;"
    )
    RSQLite::dbExecute(conn, query_del)

    n_after <- get_df_tbl(conn, "sqlite_sequence") %>% dplyr::count() %>% pull(n)

    if(n_after < n_before){
      print(glue::glue("Removed {(n_before-n_after)} records from sqlite_sequence"))
      del_records <- del_records + (n_before-n_after)
    }

  }

  # Geometry column
  if(RSQLite::dbExistsTable(conn, "geometry_columns")){
    n_before <- get_df_tbl(conn, "geometry_columns") %>% dplyr::count() %>% pull(n)
    query_del <- glue::glue(
      "DELETE
      FROM geometry_columns
      WHERE f_table_name  = '{tbl_name}' ;"
    )
    RSQLite::dbExecute(conn, query_del)
    n_after <- get_df_tbl(conn, "geometry_columns") %>% dplyr::count() %>% pull(n)

    if(n_after < n_before){
      print(glue::glue("Removed {(n_before-n_after)} records from geometry_columns"))
      del_records <- del_records + (n_before-n_after)
    }

  }


  return(del_records)
}

