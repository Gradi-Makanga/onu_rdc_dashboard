# --- World Bank minimal parser ----------------------------------------------
# Attend un endpoint WB v2 du type:
# https://api.worldbank.org/v2/country/COD/indicator/SP.POP.TOTL?format=json
# Avec pagination large pour récupérer tout
suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(tibble)
  library(purrr)
})

safe_num <- function(x) suppressWarnings(as.numeric(x))

parse_worldbank <- function(endpoint, indicator_code) {
  if (is.null(endpoint) || is.na(endpoint) || endpoint == "") {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  # Assure format & pagesize
  url <- endpoint
  if (!grepl("format=json", url, fixed = TRUE)) {
    sep <- if (grepl("\\?", url)) "&" else "?"
    url <- paste0(url, sep, "format=json")
  }
  if (!grepl("per_page=", url)) {
    sep <- if (grepl("\\?", url)) "&" else "?"
    url <- paste0(url, sep, "per_page=20000")
  }

  resp <- try(request(url) |> req_perform(), silent = TRUE)
  if (inherits(resp, "try-error")) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  js <- try(fromJSON(resp_body_string(resp), simplifyDataFrame = TRUE), silent = TRUE)
  # WB json = list(meta, data) ; data peut être data.frame ou NULL
  if (inherits(js, "try-error") || length(js) < 2 || is.null(js[[2]]) || NROW(js[[2]]) == 0) {
    return(tibble(indicator_code=character(), ref_area=character(),
                  period=integer(), value=double(), obs_status=character(),
                  source=character()))
  }

  df <- as_tibble(js[[2]])

  # Champs fréquents: value, date, countryiso3code, indicator$id
  out <- tibble(
    indicator_code = if (!is.null(indicator_code) && !is.na(indicator_code) && indicator_code != "")
      indicator_code else
      coalesce(df$indicator$id %||% df$indicator.value %||% NA_character_),
    ref_area = df$countryiso3code %||% df$country$id %||% df$countryiso2code %||% NA_character_,
    period = suppressWarnings(as.integer(df$date)),
    value  = safe_num(df$value),
    obs_status = NA_character_,
    source = "worldbank"
  ) |>
    filter(!is.na(ref_area), !is.na(period)) |>
    distinct()

  if (nrow(out) == 0) out <- out[0,]
  out
}
`%||%` <- function(a,b) if (is.null(a)) b else a
