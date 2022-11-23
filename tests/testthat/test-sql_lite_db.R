test_that("multiplication works", {


  tbl_name <- 'test'

  write_table  (tbl_name,
                          df,
                          key = NULL,
                          db_name = here::here('inst','extdata', 'revue_technique.db'),
                          overwrite= F)

})


test_that("",{ "Appending to enw records works"


  conn <- dbConnect(SQLite(), db_name)

  append_new_records(conn,
                     tbl_name,
                     df,
                     key)
})
