library(testthat)
library(magrittr)
library(RSQLite)
library(dplyr)
library(here)


# For debugging:
# dbListTables(RSQLite::dbConnect(RSQLite::SQLite(), dbname  = here::here('inst','extdata', 'nc_test.db') ))
# get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'bla')
# get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'blo')

test_that("DB creation works for df", {

  # Create the cache dir
  if(!dir.exists( here::here('inst','extdata'))) dir.create( here::here('inst','extdata'),recursive = T)

  # Remove DB
  if(file.exists(here::here('inst','extdata', 'iris_test.db'))) unlink( here::here('inst','extdata', 'iris_test.db'))

  write_table  (df = iris,
                tbl_name = 'iris_test_tbl',
                key = NULL,
                db_name = here::here('inst','extdata', 'iris_test.db'),
                overwrite= T)

  conn <- RSQLite::dbConnect(RSQLite::SQLite(),
                             dbname  = here::here('inst','extdata', 'iris_test.db')
  )

  n_before <- get_df_tbl(here::here('inst','extdata', 'iris_test.db'), 'iris_test_tbl') %>% dplyr::count()  %>% dplyr::pull(n)

  expect_equal(n_before, nrow(iris))
})




test_that("DB existence works", {

  db_name <- here::here("inst","extdata", "iris_test.db")
  tbl <- "iris_test_tbl"
  expect_true(tbl_exists(db_name, tbl))

  conn <- RSQLite::dbConnect(RSQLite::SQLite(), dbname = db_name)
  expect_true(tbl_exists(conn, tbl))

  expect_false(tbl_exists(db_name, 'smemadeuptblthatdoesnotexist'))

  RSQLite::dbDisconnect(conn)
}
)


test_that("Appending existing records works (no duplicates) with df",{

  conn <- RSQLite::dbConnect(RSQLite::SQLite(),
                             dbname  = here::here('inst','extdata', 'iris_test.db')
  )

  n_before <- get_df_tbl(here::here('inst','extdata', 'iris_test.db'), 'iris_test_tbl') %>% dplyr::count()  %>% dplyr::pull(n)

  append_new_records(iris %>% tail(2),
                     conn,
                     db_name = here::here('inst','extdata', 'iris_test.db'),
                     tbl_name = 'iris_test_tbl',
                     key=NULL)

  n_after <- get_df_tbl(here::here('inst','extdata', 'iris_test.db'), 'iris_test_tbl') %>% dplyr::count()  %>% dplyr::pull(n)

  expect_equal(n_before, n_after)

  RSQLite::dbDisconnect(conn)

})




test_that( "DB creation works for sf objects", {

  # Remove DB
  if(file.exists(here::here('inst','extdata', 'nc_test.db'))) unlink(here::here('inst','extdata', 'nc_test.db') )

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  write_table  (df = shp_nc,
                tbl_name = 'nc_tbl',
                key = 'ogc_fid',
                db_name = here::here('inst','extdata', 'nc_test.db'),
                overwrite = T)


  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>% dplyr::pull(n)

  expect_equal(as.integer(n_after), nrow(shp_nc))

}

)



test_that("Appending existing records works (no duplicates) with sf",{

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  conn <- RSQLite::dbConnect(RSQLite::SQLite(),
                             dbname  = here::here('inst','extdata', 'nc_test.db')
  )

  n_before <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>% dplyr::pull(n)

  append_new_records(shp_nc %>% tail(2),
                     conn,
                     db_name =  here::here('inst','extdata', 'nc_test.db') ,
                     tbl_name = 'nc_tbl',
                     key=NULL) # use all columns + ignore geometry

  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>% dplyr::pull(n)

  expect_equal(n_before, n_after)

  RSQLite::dbDisconnect(conn)

})





test_that("Appending new records with sf",{

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  conn <- RSQLite::dbConnect(RSQLite::SQLite(),
                             dbname  = here::here('inst','extdata', 'nc_test.db')
  )

  n_before <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>%  dplyr::pull(n)

  # Random values to numerical columns
  shp_nc_rnd <- shp_nc %>%
    dplyr::mutate(across(where(is.numeric),  ~ rnorm(length(.),mean = 100)))

  num_new <- 10

  append_new_records(shp_nc_rnd %>% head(num_new),
                     conn,
                     db_name =  here::here('inst','extdata', 'nc_test.db') ,
                     tbl_name = 'nc_tbl',
                     key=NULL) # use all columns + ignore geometry

  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count()  %>%  dplyr::pull(n)

  expect_equal(n_before+num_new, n_after)

  RSQLite::dbDisconnect(conn)

})




test_that("Adding new sf table to existing db",{

  # Remove DB
  if(file.exists(here::here('inst','extdata', 'nc_test.db'))) unlink(here::here('inst','extdata', 'nc_test.db') )

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  conn <- RSQLite::dbConnect(RSQLite::SQLite(),  dbname  = here::here('inst','extdata', 'nc_test.db') )

  # Write table to new DB
  write_table  (df = shp_nc,
                tbl_name = 'nc_tbl',
                key = 'ogc_fid', # this only works with an existing DB - ogc_fid is created automatically by sf::st_write
                db_name = here::here('inst','extdata', 'nc_test.db'),
                overwrite = T)

  n_before <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>%  dplyr::pull(n)
  conn <- RSQLite::dbConnect(RSQLite::SQLite(),  dbname  = here::here('inst','extdata', 'nc_test.db') )
  tables_before <- RSQLite::dbListTables(conn)

  # Write another table to existing DB
  write_table(shp_nc,
              tbl_name = 'nc_tbl_2',
              key = 'ogc_fid',
              db_name = here::here('inst','extdata', 'nc_test.db'),
              overwrite= T)

  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count()  %>%  dplyr::pull(n)
  conn <- RSQLite::dbConnect(RSQLite::SQLite(),  dbname  = here::here('inst','extdata', 'nc_test.db') )
  tables_after <- RSQLite::dbListTables(conn)

  # Compare tables
  expect_false(any('nc_tbl_2'  %in% tables_before))
  expect_true(any('nc_tbl_2'  %in% tables_after))

  # Make sure the existing original table stayed the same
  expect_equal(n_before, n_after)

  RSQLite::dbDisconnect(conn)

})



test_that("Add new data.frame to existing db",{


  # Remove DB
  if(file.exists(here::here('inst','extdata', 'nc_test.db'))) unlink(here::here('inst','extdata', 'nc_test.db') )

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  conn <- RSQLite::dbConnect(RSQLite::SQLite(),  dbname  = here::here('inst','extdata', 'nc_test.db') )

  # Write table to new DB
  write_table  (df = shp_nc,
                tbl_name = 'nc_tbl',
                key = 'ogc_fid',
                db_name = here::here('inst','extdata', 'nc_test.db'),
                overwrite = T)

  n_before <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>%  dplyr::pull(n)
  conn <- RSQLite::dbConnect(RSQLite::SQLite(),  dbname  = here::here('inst','extdata', 'nc_test.db') )
  tables_before <- RSQLite::dbListTables(conn)

  # Write another table (non spatial) to existing DB
  write_table(iris,
              tbl_name = 'iris',
              db_name = here::here('inst','extdata', 'nc_test.db'),
              overwrite= T)

  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count()  %>%  dplyr::pull(n)
  conn <- RSQLite::dbConnect(RSQLite::SQLite(),  dbname  = here::here('inst','extdata', 'nc_test.db') )
  tables_after <- RSQLite::dbListTables(conn)

  # Compare tables
  expect_false(any('iris'  %in% tables_before))
  expect_true(any('iris'  %in% tables_after))



  RSQLite::dbDisconnect(conn)



})



test_that("Deletion of tables",{

  # Use existing DB
  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  db_name <- here::here('inst','extdata', 'nc_test_2.db')

  # Write table to new DB
  write_table  (df = shp_nc,
                tbl_name = 'nc_tbl',
                key = 'FIPS',
                db_name = db_name,
                overwrite = T)

  # Write another table (non spatial) to existing DB
  write_table(iris,
              tbl_name = 'iris',
              db_name = db_name,
              overwrite= T)

  tbls_to_delete <- c("iris", "nc_tbl")

  conn <- RSQLite::dbConnect(RSQLite::SQLite(), dbname = db_name)
  tables_before_deletion <- RSQLite::dbListTables(conn)
  RSQLite::dbDisconnect(conn)

  expect_true(all(tbls_to_delete %in% tables_before_deletion))

  conn <- RSQLite::dbConnect(RSQLite::SQLite(), dbname = db_name)
  delete_tables(conn, tbls_to_delete)


  conn <- RSQLite::dbConnect(RSQLite::SQLite(), dbname = db_name)
  tables_after_deletion <- RSQLite::dbListTables(conn)
  expect_true(RSQLite::dbReadTable(conn, "geometry_columns") %>%
                filter(f_table_name ==  "nc_tbl") %>% nrow() == 0) # removed associated record for spatial data
  expect_true(RSQLite::dbReadTable(conn, "sqlite_sequence") %>%
                filter(name ==  "nc_tbl") %>% nrow() == 0) # removed associated record for spatial data
  RSQLite::dbDisconnect(conn)

  expect_false(any(tbls_to_delete %in% tables_after_deletion),
               info=paste0(paste0(tbls_to_delete, collapse = ','),
                           " vs ",
                           paste0(tables_after_deletion, collapse = ',')
               ))


}
)


test_that( "Spatial lite function are all available - area", {

  # Remove DB
  db_name <- here::here('inst','extdata', 'nc_test.db')

  if(file.exists(db_name)) unlink(db_name)

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  # Write table to new DB
  tbl_name <- 'nc_tbl'
  write_table  (df = shp_nc,
                tbl_name = tbl_name ,
                key = 'FIPS',
                db_name = db_name,
                overwrite = T)


  df_area <- sf::st_read(
    dsn = db_name,
    query = glue::glue("SELECT st_area(GEOMETRY) as area FROM '{tbl_name}' ")
  )


  # Computing area works
  expect_true(!any(is.na(df_area$area)))


  # Append new records
  conn <- RSQLite::dbConnect(RSQLite::SQLite(), dbname = db_name)
  append_new_records(shp_nc %>%   dplyr::mutate(across(where(is.numeric),  ~ rnorm(length(.),mean = 100))) %>%  tail(2),
                     conn,
                     db_name =  here::here('inst','extdata', 'nc_test.db') ,
                     tbl_name = tbl_name,
                     key=NULL) # use all columns + ignore geometry

  df_area_2 <- sf::st_read(
    dsn = db_name,
    query = glue::glue("SELECT st_area(GEOMETRY) as area FROM '{tbl_name}' ")
  )

  # Computing area works
  expect_true(!any(is.na(df_area_2$area)))



}


)



test_that( "Spatial lite function are all available - distance", {

  # Remove DB
  db_name <- here::here('inst','extdata', 'meuse.db')

  if(file.exists(db_name)) unlink(db_name)

  shp_meuse <- sf::st_read(system.file("sqlite/meuse.sqlite", package = "sf"))

  # Write table to new DB
  tbl_name <- 'meuse_tbl'
  write_table  (df = shp_meuse,
                tbl_name = tbl_name ,
                key = 'FIPS',
                db_name = db_name,
                overwrite = T)


  # Compute distance from point
  df_dist <- meuse_no_na <- sf::st_read(
    dsn=db_name,
    query = glue::glue("SELECT ST_DISTANCE(ST_GeomFromText('POINT (181072 333611)', 28992), GEOMETRY) as dist FROM '{tbl_name}'")
  )


  # Computing distance works
  expect_true(!any(is.na(df_dist$dist)))

}


)



