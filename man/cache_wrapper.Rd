% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cache.R
\name{cache_wrapper}
\alias{cache_wrapper}
\title{Cache function saves temporary objects as parquet files}
\usage{
cache_wrapper(
  name_str,
  fun,
  path_cache_dir = NA,
  use_parquet = F,
  is_spatial = F,
  ...
)
}
\arguments{
\item{path_cache_dir}{}

\item{use_parquet}{}
}
\value{
fun(...) - and results cached to a zip file
}
\description{
Send in a unique name to save the object and a function that will create it
along with its parameters.
}
\details{
Also possible to change the path directory otherwise will use the default one
}
\examples{
\dontrun{
#
}
}
