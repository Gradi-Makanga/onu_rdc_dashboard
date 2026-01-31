# --- UNSDG minimal parser ----------------------------------------------------
# Exemple:
# https://unstats.un.org/SDGAPI/v1/sdg/Indicator/Data?indicator=16.1.3&areaCode=180
suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(tibble)
  library(purrr)
})

parse_unsdg <- function(endpoint, indicator_code) {
  if (is.null(endpoint) || is.na(endpoint) || endpoint == "") {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  # pagesize pour éviter la pagination
  url <- endpoint
  if (!grepl("pageSize=", url, ignore.case = TRUE)) {
    sep <- if (grepl("\\?", url)) "&" else "?"
    url <- paste0(url, sep, "pageSize=10000")
  }

  resp <- try(request(url) |> req_timeout(60) |> req_perform(), silent = TRUE)
  if (inherits(resp, "try-error")) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  js <- try(fromJSON(resp_body_string(resp), flatten = TRUE), silent = TRUE)
  # structure attendue: js$data (data.frame)
  if (inherits(js, "try-error") || is.null(js$data) || NROW(js$data) == 0) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  d <- as_tibble(js$data)

  # Champs utiles: timePeriodStart, value, geoAreaCode
  # Certains jeux ont value en caractère
  period <- suppressWarnings(as.integer(d$timePeriodStart %||% d$timePeriod %||% NA_integer_))
  value  <- suppressWarnings(as.numeric(d$value %||% d$Value %||% NA_real_))

  # obs_status: Nature (C/E/M/…)
  obs <- d$attributes.Nature %||% d$attributes$Nature %||% NA_character_

  out <- tibble(
    indicator_code = if (!is.null(indicator_code) && !is.na(indicator_code) && indicator_code != "")
      indicator_code else (d$indicator %||% d$indicatorId %||% NA_character_),
    ref_area  = d$geoAreaCode %||% d$GeoAreaCode %||% d$geoArea %||% NA_character_,
    period    = period,
    value     = value,
    obs_status = obs,
    source    = "unsdg"
  ) |>
    filter(!is.na(ref_area), !is.na(period)) |>
    distinct()

  if (nrow(out) == 0) out <- out[0,]
  out
}
`%||%` <- function(a,b) if (is.null(a)) b else a
 