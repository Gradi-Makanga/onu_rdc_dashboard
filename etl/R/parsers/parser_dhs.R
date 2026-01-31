# --- DHS minimal parser ------------------------------------------------------
# Exemple:
# https://api.dhsprogram.com/rest/dhs/v8/data?countryIds=CD&indicatorIds=CO_MOBB_M_MOB,CO_MOBB_W_MOB&f=json
suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(tibble)
  library(purrr)
})

coalesce_chr <- function(...) {
  x <- list(...)
  for (v in x) {
    if (!is.null(v) && length(v) > 0) return(as.character(v))
  }
  return(NA_character_)
}

parse_dhs <- function(endpoint, indicator_code) {
  if (is.null(endpoint) || is.na(endpoint) || endpoint == "") {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  resp <- try(request(endpoint) |> req_timeout(60) |> req_perform(), silent = TRUE)
  if (inherits(resp, "try-error")) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  js <- try(fromJSON(resp_body_string(resp), flatten = TRUE), silent = TRUE)
  if (inherits(js, "try-error")) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  # DHS structure: soit js$Data (v8), soit js$data (selon versions)
  d <- js$Data %||% js$data
  if (is.null(d) || NROW(d) == 0) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }
  d <- as_tibble(d)

  # colonnes plausibles
  ref_area <- d$CountryISO3 %||% d$ISO3 %||% d$CountryCode %||% d$DHS_CountryCode %||% NA_character_
  period   <- d$SurveyYear %||% d$Year %||% d$Time %||% NA
  period   <- suppressWarnings(as.integer(period))
  value    <- suppressWarnings(as.numeric(d$Value %||% d$value))

  out <- tibble(
    indicator_code = if (!is.null(indicator_code) && !is.na(indicator_code) && indicator_code != "")
      indicator_code else coalesce_chr(d$IndicatorId, d$Indicator, NA_character_),
    ref_area  = ref_area,
    period    = period,
    value     = value,
    obs_status = NA_character_,
    source    = "dhs"
  ) |>
    filter(!is.na(ref_area), !is.na(period)) |>
    distinct()

  if (nrow(out) == 0) out <- out[0,]
  out
}
`%||%` <- function(a,b) if (is.null(a)) b else a
 