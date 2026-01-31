library(rvest); library(xml2); library(dplyr); library(purrr); library(readr); library(stringr)
source("R/parsers/parser_scrape_generic.R")

route_scrape <- function(indicator_code, url){
  parse_scrape_generic(indicator_code, url)
}

ingest_scrape_all <- function(cat) {
  if (!"scrape_url" %in% names(cat)) return(NULL)
  safe_route <- purrr::safely(function(code, u) route_scrape(code, u), otherwise = tibble(), quiet = TRUE)
  cat %>%
    filter(!is.na(scrape_url) & nzchar(scrape_url)) %>%
    mutate(res = map2(indicator_code, scrape_url, ~ safe_route(.x, .y)),
           raw = map(res, "result"),
           err = map(res, "error")) %>%
    select(indicator_code, indicator_name, scrape_url, raw, err)
}
