library(testthat)
library(magrittr)

test_that("DB creation works for df", {

  # Create the cache dir
  if(!dir.exists( here::here('inst','extdata'))) dir.create( here::here('inst','extdata'),recursive = T)

  write_table  (df = iris,
                tbl_name = 'iris_test_tbl',
                key = NULL,
                db_name = here::here('inst','extdata', 'iris_test.db'),
                overwrite= T)

})


test_that("Appending existing records works (no duplicates) with df",{

  conn <- dbConnect(RSQLite::SQLite(),
                    dbname  = here::here('inst','extdata', 'iris_test.db')
                    )

  n_before <- get_df_tbl(here::here('inst','extdata', 'iris_test.db'), 'iris_test_tbl') %>% dplyr::count()  %>% dplyr::pull(n)

  append_new_records(iris %>% tail(2),
                     conn,
                     db_name = here::here('inst','extdata', 'iris_test.db'),
                     tbl_name = 'iris_test_tbl',
                     key=NULL)

  n_after <- get_df_tbl(here::here('inst','extdata', 'iris_test.db'), 'iris_test_tbl') %>% dplyr::count()  %>% dplyr::pull(n)

  assertthat::are_equal(n_before, n_after)

})




test_that( "DB creation works for sf objects", {

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  write_table  (df = shp_nc,
                tbl_name = 'nc_tbl',
                key = 'ogc_fid',
                db_name = here::here('inst','extdata', 'nc_test.db'),
                overwrite= T)


  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count() %>% dplyr::pull(n)

  assertthat::are_equal(as.integer(n_after), nrow(shp_nc))

  }

)



test_that("Appending existing records works (no duplicates) with sf",{

  shp_nc <- sf::st_read(system.file("shape/nc.shp", package="sf"))

  conn <- dbConnect(RSQLite::SQLite(),
                    dbname  = here::here('inst','extdata', 'nc_test.db')
  )

  n_before <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count()

  append_new_records(shp_nc %>% tail(2),
                     conn,
                     db_name =  here::here('inst','extdata', 'nc_test.db') ,
                     tbl_name = 'nc_tbl',
                     key=NULL)

  n_after <- get_df_tbl(here::here('inst','extdata', 'nc_test.db'), 'nc_tbl') %>% dplyr::count()

  assertthat::are_equal(n_before, n_after)

})
