# R/parsers/parser_scrape.R
suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tibble)
})

parse_scrape_generic <- function(indicator_code, url){
  df <- try(suppressWarnings(readr::read_csv(url, show_col_types = FALSE)), silent = TRUE)
  if (inherits(df, "try-error") || is.null(df) || !nrow(df)) return(tibble())

  ref_area <- dplyr::coalesce(df$Code, df$code, df$ISO3, df$iso3, df$Entity, df$entity)
  period   <- dplyr::coalesce(df$Year, df$year, df$date)
  value    <- dplyr::coalesce(df$Value, df$value)

  tibble(
    indicator_code = indicator_code,
    ref_area = as.character(ref_area),
    period   = as.character(period),
    value    = suppressWarnings(as.numeric(value)),
    obs_status = NA_character_
  ) |>
    dplyr::filter(!is.na(value), !is.na(period), nzchar(period))

ensure_schema(out, indicator_code, "scrape") 
}
