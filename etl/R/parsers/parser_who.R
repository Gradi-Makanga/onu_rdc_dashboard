
# parser_who.R — WHO GHO API
# Example endpoint: https://ghoapi.azureedge.net/api/Indicator?$filter=IndicatorCode eq 'MORT_100K'
library(jsonlite); library(dplyr); library(httr2); library(tidyr)

parse_who <- function(indicator_code, endpoint){
  js <- jsonlite::fromJSON(endpoint, flatten = TRUE)
  d <- as_tibble(js$value)
  if (!nrow(d)) return(tibble())
  # Harmonize to (ref_area, period, value)
  guess_val <- dplyr::coalesce(d$NumericValue, d$ValueNumeric, d$FactValueNumeric, d$Value)
  tibble(
    indicator_code = indicator_code,
    ref_area = d$SpatialDimValueCode %||% d$SpatialDim,
    period = as.character(d$TimeDim %||% d$TimeDimensionValue),
    value = suppressWarnings(as.numeric(guess_val)),
    obs_status = d$Dim1Type
  ) %>% filter(!is.na(value))
}
