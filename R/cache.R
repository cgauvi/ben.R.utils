

#' Cache function saves temporary objects as parquet files
#'
#' Send in a unique name to save the object and a function that will create it
#' along with its parameters.
#'
#' Also possible to change the path directory otherwise will use the default one
#'
#' @param name_str
#' @param fun
#' @param path_cache_dir
#'
#' @param ...
#' @param use_parquet
#'
#' @return fun(...) - and results cached to a zip file
#' @export
#'
#' @importFrom magrittr %<>%
#' @import arrow glue purrr dplyr assertthat
#'
#' @examples
#' \dontrun{
#' #
#' }
cache_wrapper <- function(name_str,
                          fun,
                          path_cache_dir = NA,
                          use_parquet = F,
                          is_spatial = F,
                          ...) {

  # QA
  assertthat::assert_that(use_parquet + is_spatial < 2,
                          msg = 'Fatal error! cannot use both parquet and sf or sp dataset')

  assertthat::assert_that(!is.null(name_str) & !is.na(name_str) & nchar(name_str) > 0,
                          msg = "Fatal error! no file name provided for cache!"
  )

  assertthat::assert_that(dir.exists(path_cache_dir),
                          msg = "Fatal error! Cache directory does not exist")


  # Try parquet + fallback on original format
  if(use_parquet){
      df <- cache_(df,
                   name_str,
                   path_cache_dir,
                   read_fun=arrow::read_parquet,
                   write_fun=arrow::write_parquet,
                   new_extension='parquet',
                   fun=fun,
                   ...)

  }else if (is_spatial) {
    df <- cache_(df,
                 name_str=name_str,
                 path_cache_dir=path_cache_dir,
                 read_fun=sf::st_read,
                 write_fun=sf::st_write,
                 new_extension='geojson',
                 fun=fun,
                 ...)
  }else{
    # Try the default format
    df <- cache_(df,
                 name_str,
                 path_cache_dir,
                 read_fun=readr::read_csv,
                 write_fun=readr::write_csv,
                 new_extension='csv',
                 fun=fun,
                 ...)
  }


  return(df)
}




#' Title
#'
#' @param df
#' @param name_str
#' @param path_cache_dir
#' @param read_fun
#' @param write_fun
#' @param new_extension
#'
#' @return
#' @importFrom  glue glue
#' @export
#'
#' @examples
cache_ <- function(df,
                   name_str,
                   path_cache_dir,
                   read_fun,
                   write_fun,
                   new_extension,
                   fun,
                   ...){


  # Remove extension if present
  name_str_no_ext <- tools::file_path_sans_ext(name_str)
  if(!is.null(new_extension)) name_str_new <- glue('{name_str_no_ext}.{new_extension}')
  else  name_str_new <- name_str

  # Create the path
  path <- file.path(path_cache_dir,name_str_new)

  # Get the data if not already cached
  if (!file.exists(path)) {
    print(glue("{path} does not exist yet - running {as.character(substitute(fun))} ..."))

    df <- fun(...)

    write_fun(df, path)
    print(glue("Saving results: \n{path}..."))


  } else {
    print(glue("Reading existing {path} ..."))

    # read
    df <- read_fun(path)
  }

  return(df)
}

