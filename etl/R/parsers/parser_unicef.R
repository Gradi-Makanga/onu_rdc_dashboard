
# parser_unicef.R — UNICEF Data API
library(jsonlite); library(dplyr); library(tidyr)

parse_unicef <- function(indicator_code, endpoint){
  js <- jsonlite::fromJSON(endpoint, flatten = TRUE)
  d <- as_tibble(js$data)
  if (!nrow(d)) return(tibble())
  tibble(
    indicator_code = indicator_code,
    ref_area = d$refArea,
    period = as.character(d$timePeriod),
    value = as.numeric(d$value),
    obs_status = d$upperBound
  ) %>% filter(!is.na(value))
}
