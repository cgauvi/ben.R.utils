test_that("Caching sf object works", {

  read_geojson <- function(){
    sf::st_read("https://data.montreal.ca/dataset/9f3911af-3a5f-4c4b-89c7-239ba487b1f1/resource/19ce5356-2ba6-4702-ad6c-c84e13698693/download/secteurs_deneigement_saison_2021-2022.geojson")

  }

  # Create the cache dir
  if(!dir.exists( here::here('cache'))) dir.create( here::here('cache'))

  # Read in once
  shp_mtl_geojson <- cache_wrapper (name_str = 'mtl_business.geojson',
                                    fun = read_geojson,
                                    path_cache_dir = here::here('cache'),
                                    is_spatial = T
  )

  # Cached
  assertthat::assert_that(file.exists(here::here('cache','mtl_business.geojson')))

  # Same
  shp_cached <- sf::st_read(here::here('cache','mtl_business.geojson'))

  assertthat::assert_that(all(dim(shp_mtl_geojson) == dim(shp_cached)))
  assertthat::assert_that(all(colnames(shp_mtl_geojson) == colnames(shp_cached)))

})


test_that("Caching csv object works", {

  # Create the cache dir
  if(!dir.exists( here::here('cache'))) dir.create( here::here('cache'))

  read_csv <- function(){
    readr::read_csv("https://data.montreal.ca/dataset/fa01965a-6db5-42f9-b889-d39769b046eb/resource/0e09829f-3e24-4fb3-94cf-0e8b727de22b/download/inspection-aliments-bilan.csv")
  }

  # Read in once
  df_aliments <- cache_wrapper (name_str = 'aliments.csv',
                                    fun = read_csv,
                                    path_cache_dir = here::here('cache'),
                                    is_spatial = F
  )

  # Cached
  assertthat::assert_that(file.exists(here::here('cache','aliments.csv')))

  # Same
  df_aliments_cached <- readr::read_csv(here::here('cache','aliments.csv'))

  assertthat::assert_that(all(dim(df_aliments) == dim(df_aliments_cached)))
  assertthat::assert_that(all(colnames(df_aliments) == colnames(df_aliments_cached)))

})




test_that("Caching parquet object works", {

  # Create the cache dir
  if(!dir.exists( here::here('cache'))) dir.create( here::here('cache'))

  read_csv <- function(){
    readr::read_csv("https://data.montreal.ca/dataset/fa01965a-6db5-42f9-b889-d39769b046eb/resource/0e09829f-3e24-4fb3-94cf-0e8b727de22b/download/inspection-aliments-bilan.csv")
  }

  # Read in once
  df_aliments <- cache_wrapper (name_str = 'aliments.parquet',
                                fun = read_csv,
                                path_cache_dir = here::here('cache'),
                                use_parquet = T
  )

  # Cached
  assertthat::assert_that(file.exists(here::here('cache','aliments.parquet')))

  # Same
  df_aliments_cached <- arrow::read_parquet(here::here('cache','aliments.parquet'))

  assertthat::assert_that(all(dim(df_aliments) == dim(df_aliments_cached)))
  assertthat::assert_that(all(colnames(df_aliments) == colnames(df_aliments_cached)))

})

