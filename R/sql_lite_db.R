library(DBI)
library(RSQLite)
library(glue)


#' Add a table to a SQLite db (or append if it exists)
#'
#' @param tbl_name
#' @param df
#' @param key unique ID to use to identify duplicates if appending to existing table: defaults to ALL columns
#' @param db_name
#' @param overwrite
#'
#' @return
#' @export
#'
#' @examples
write_table <- function(tbl_name,
                        df,
                        key = NULL,
                        db_name = here::here('inst','extdata', 'revue_technique.db'),
                        overwrite= F){


  conn <- dbConnect(SQLite(), db_name)

  # Check if Db exists
  results <- 0
  existing_tables <- tryCatch({
    existing_tables <- dbListTables(conn)
  },error=function(e){
    print(glue('Db {db_name} does not exist -> creating it along with table'))
    results <- RSQLite::dbWriteTable(conn, tbl_name, df)
    existing_tables <- list()
    return(existing_tables)
  })

  if(length(existing_tables) == 0 ) return(results)

  # DB exists, either create the table or append if existing
  if(!(tbl_name %in% existing_tables) | overwrite) {
    if(overwrite) print(glue('Table {tbl_name} exists but forcing overwrite'))
    else print(glue('Table {tbl_name} does not exist -> creating it'))

    results <- RSQLite::dbWriteTable(conn, tbl_name, df)
  }else{
    results <- append_new_records(conn, tbl_name, df,  key)
  }

  return(results)
}



#' Append new records to an existing table in a sql lite db
#'
#' @param conn
#' @param tbl_name
#' @param df
#' @param key unique ID to use to identify duplicates if appending to existing table: defaults to ALL columns
#'
#' @return number of records added
#' @export
#'
#' @examples
append_new_records <- function(conn,
                               tbl_name,
                               df,
                               key){

  print(glue('Table {tbl_name} exists: appending '))

  # Check for existing records and try to avoid deduping
  if(is.null(key)){
    key <- colnames(df)
  }
  key_str <- paste0(keys,collapse = ',')
  sql_anti_join_clause <- paste0(
    paste0( glue('{tbl_name}.'), key , ' IS NULL') ,
    collapse = ' AND '
  )

  # Tmp table
  # Drop tmp table
  dbExecute(conn, 'DROP TABLE  if exists tmp')
  RSQLite::dbWriteTable(conn, 'tmp', df)

  sql_anti_join_clause <- glue("
      SELECT *
      FROM tmp
      LEFT JOIN {tbl_name}
      WHERE {sql_clause};
    ")

  df_to_add <- dbGetQuery(conn, sql_anti_join_clause)
  new_records <- nrow(df_to_add)

  if(new_records > 0){
    print(glue('Appending {new_records} new rows to {tbl_name} -- duplicates removed based on keys: {key_str}'))
    RSQLite::dbAppendTable(conn, tbl_name, df)
  }else{
    print(glue('No new records added -- all {nrow(df)} already exist -- duplicates identified based on keys: {key_str}'))
  }

  # Drop tmp table
  dbExecute(conn, 'DROP TABLE  if exists tmp')

  return(new_records)

}




#' Return a df from a sql lite connection that can be queried lazily with dplyr
#'
#' @param tbl_name
#' @param db_name
#'
#' @return
#' @export
#'
#' @examples
get_df_tbl <- function(tbl_name= 'policies_2015-2019',
                       db_name = here::here('inst','extdata', 'revue_technique.db')
){


  conn <- RSQLite::dbConnect(RSQLite::SQLite(), db_name)


  dplyr::tbl(conn, tbl_name)


}



