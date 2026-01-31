# helpers.R — utilitaires API/transformations (ASCII/UTF-8)

suppressPackageStartupMessages({
  library(httr2)
  library(readr)
  library(dplyr)
  library(jsonlite)
})

api_get_text <- function(api_base, api_key = "", path, timeout = 60) {
  u <- paste0(api_base, path)
  req <- httr2::request(u)
  if (nzchar(api_key)) req <- httr2::req_headers(req, "X-API-Key" = api_key)
  req <- httr2::req_timeout(req, timeout)
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) >= 400) stop("HTTP ", httr2::resp_status(resp), " on ", path)
  rawToChar(httr2::resp_body_raw(resp))
}

qs_export <- function(ind_code, ref_area = NULL, source = NULL, obs = NULL, start = NULL, end = NULL) {
  kv <- list(
    indicator_code = ind_code,
    ref_area = ref_area, source = source, obs_status = obs,
    start = start, end = end
  )
  kv <- kv[!vapply(kv, function(x) is.null(x) || (is.character(x) && !nzchar(x)), FALSE)]
  if (length(kv) == 0) return("")
  keys <- names(kv)
  vals <- vapply(kv, function(x) utils::URLencode(as.character(x), reserved = TRUE), character(1))
  paste0("?", paste(paste0(keys, "=", vals), collapse = "&"))
}

load_indicators <- function(api_base, api_key = "", query = "") {
  path <- "/indicators"
  if (nzchar(query)) path <- paste0(path, "?q=", utils::URLencode(query, reserved = TRUE))
  txt <- api_get_text(api_base, api_key, path, timeout = 60)
  out <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (!is.data.frame(out)) {
    tibble(indicator_code = character(0), indicator_name = character(0))
  } else {
    out %>%
      mutate(
        indicator_code = as.character(indicator_code),
        indicator_name = as.character(indicator_name)
      ) %>%
      distinct(indicator_code, indicator_name)
  }
}

fetch_values <- function(api_base, api_key = "",
                         ind_code, ref_area = NULL, source = NULL, obs = NULL,
                         start = NULL, end = NULL) {
  qs  <- qs_export(ind_code, ref_area, source, obs, start, end)
  txt <- api_get_text(api_base, api_key, paste0("/export/csv", qs), timeout = 90)
  suppressMessages(
    readr::read_csv(I(txt), show_col_types = FALSE)
  )
}

# --- Geo: fond monde simple (ISO_A3) ---
get_world_sf <- function() {
  suppressPackageStartupMessages({
    library(sf)
    library(rnaturalearth)
  })
  rnaturalearth::ne_countries(scale = "medium", returnclass = "sf") %>%
    dplyr::mutate(iso3 = .data$iso_a3) %>%
    dplyr::select(iso3, name, geometry)
}

summarise_latest <- function(df) {
  df %>%
    mutate(period = suppressWarnings(as.integer(period)),
           value  = suppressWarnings(as.numeric(value))) %>%
    filter(!is.na(ref_area), !is.na(period), !is.na(value)) %>%
    arrange(ref_area, period) %>%
    group_by(ref_area) %>%
    slice_tail(n = 1) %>%
    ungroup()
}
