
# parser_ilostat.R — ILOSTAT SDMX-like parser
# Example endpoint: https://www.ilo.org/sdmx/rest/data/ILO,DF_SDGP_10.4.1/.COD....?startPeriod=2000&endPeriod=2025
library(readr); library(dplyr); library(jsonlite); library(httr2); library(stringr); library(tidyr)

parse_ilostat <- function(indicator_code, endpoint){
  req <- httr2::request(endpoint) %>% req_perform()
  stopifnot(resp_status(req) < 300)
  ct <- resp_header(req, "content-type")
  if (grepl("json", ct)) {
    js <- jsonlite::fromJSON(resp_body_string(req), flatten = TRUE)
    if (!"dataSets" %in% names(js)) return(tibble())
    # Placeholder: depends on SDMX structure; user-specific mapping will be added later
    return(tibble(indicator_code=indicator_code)[0,])
  } else {
    # Try CSV fallback
    txt <- resp_body_string(req)
    df <- tryCatch(readr::read_csv(I(txt), show_col_types = FALSE), error = function(e) tibble())
    df
  }
}
