library(testthat)
library(magrittr)
library(RSQLite)
library(dplyr)
library(here)


test_that("DB creation works for df", {

  # Create the cache dir
  if(!dir.exists( here::here('inst','extdata'))) dir.create( here::here('inst','extdata'),recursive = T)

  # Remove DB
  if(file.exists( here::here('inst','extdata', 'iris_test.db'))) unlink( here::here('inst','extdata', 'iris_test.db'))

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

})




test_that( "DB creation works for sf objects", {

  # Remove DB
  if(file.exists(here::here('inst','extdata', 'nc_test.db'))) unlink(here::here('inst','extdata', 'nc_test.db') )

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  write_table  (df = shp_nc,
                tbl_name = 'nc_tbl',
                key = 'ogc_fid',
                db_name = here::here('inst','extdata', 'nc_test.db'),
                overwrite= T)


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

})

