# R/ingest_api.R — lecture catalogue + routeur + parseurs + export
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(readr)
  library(httr2)
  library(jsonlite)
  library(tibble)
})

# ----------------------- utilitaires communs ----------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x
safe_num <- function(x) suppressWarnings(as.numeric(x))
safe_int <- function(x) suppressWarnings(as.integer(x))

empty_df <- function() {
  tibble(
    indicator_code = character(),
    ref_area       = character(),
    period         = integer(),
    value          = double(),
    obs_status     = character(),
    source         = character()
  )
}

# Garantie les colonnes et types (ne filtre pas les NA)
ensure_schema <- function(df) {
  if (is.null(df) || !is.data.frame(df)) return(empty_df())
  need <- c("indicator_code","ref_area","period","value","obs_status","source")
  miss <- setdiff(need, names(df))
  if (length(miss)) df[miss] <- NA
  df |>
    transmute(
      indicator_code = as.character(indicator_code),
      ref_area       = as.character(ref_area),
      period         = safe_int(period),
      value          = safe_num(value),
      obs_status     = as.character(obs_status),
      source         = as.character(source)
    )
}

safe_fetch_json <- function(url, timeout = 60) {
  req <- request(url) |>
    req_user_agent("ONU-RDC-Dashboard/ingest") |>
    req_timeout(timeout)
  resp <- req_perform(req)
  jsonlite::fromJSON(resp_body_string(resp), flatten = TRUE)
}

# ----------------------- lecture du catalogue --------------------------------
load_catalogue <- function(path = "catalogue/catalogue_enriched.csv") {
  stopifnot(file.exists(path))

  first_line <- readLines(path, n = 1, warn = FALSE)
  delim <- if (str_count(first_line, ";") > str_count(first_line, ",")) ";" else ","

  cat_df <- readr::read_delim(
    file = path,
    delim = delim,
    col_types = readr::cols(.default = readr::col_character()),
    trim_ws  = TRUE,
    locale   = readr::locale(decimal_mark = if (delim == ";") "," else "."),
    show_col_types = FALSE
  )

  names(cat_df) <- names(cat_df) |>
    str_trim() |>
    str_to_lower() |>
    str_replace_all("\\s+", "_")

  pick_col <- function(df, candidates) {
    x <- intersect(candidates, names(df))
    if (length(x)) x[[1]] else NA_character_
  }

  old_indicator <- pick_col(cat_df, c("indicator_code","code","indicator"))
  old_endpoint  <- pick_col(cat_df, c("endpoint","api_endpoint","url"))
  old_scrape    <- pick_col(cat_df, c("scrape_url","scraping","scrape","csv_url","download_url"))
  old_source    <- pick_col(cat_df, c("source","src"))

  if (!is.na(old_indicator) && old_indicator != "indicator_code")
    cat_df <- rename(cat_df, indicator_code = all_of(old_indicator))
  if (!is.na(old_endpoint) && old_endpoint != "endpoint")
    cat_df <- rename(cat_df, endpoint = all_of(old_endpoint))
  if (!is.na(old_scrape) && old_scrape != "scrape_url")
    cat_df <- rename(cat_df, scrape_url = all_of(old_scrape))
  if (!is.na(old_source) && old_source != "source")
    cat_df <- rename(cat_df, source = all_of(old_source))

  need <- c("indicator_code","endpoint","scrape_url","source")
  miss <- setdiff(need, names(cat_df))
  if (length(miss)) stop("Colonnes manquantes dans le catalogue: ", paste(miss, collapse = ", "))

  # normalisation des alias et complétion de source par motif d’URL
  cat_df <- cat_df |>
    mutate(
      indicator_code = str_trim(indicator_code),
      endpoint       = str_trim(endpoint),
      scrape_url     = str_trim(scrape_url),
      source         = tolower(str_trim(source))
    ) |>
    filter(!(is.na(endpoint)   | endpoint   == "") |
           !(is.na(scrape_url) | scrape_url == "")) |>
    mutate(
      source = case_when(
        source %in% c("wdi","world bank","world_bank","worldbank api") ~ "worldbank",
        source %in% c("ourworlddata","ourworldindata","owid","our world in data") ~ "scrape",
        source %in% c("dhs api","dhsapi","dhs_api") ~ "dhs",
        TRUE ~ source
      ),
      source = case_when(
        source == "" & str_detect(endpoint, "worldbank\\.org")            ~ "worldbank",
        source == "" & str_detect(endpoint, "unstats\\.un\\.org/SDGAPI")  ~ "unsdg",
        source == "" & str_detect(endpoint, "api\\.dhsprogram\\.com")     ~ "dhs",
        source == "" & scrape_url != ""                                   ~ "scrape",
        TRUE ~ source
      )
    )

  cat_df
}

# ----------------------- routeur & ingestion ---------------------------------
route_api <- function(indicator_code, endpoint = NA, source = NA, scrape_url = NA) {
  scalar <- function(x) {
    if (length(x) == 0) return(NA_character_)
    if (is.na(x)) return(NA_character_)
    as.character(x[[1]])
  }
  indicator_code <- scalar(indicator_code)
  endpoint       <- scalar(endpoint)
  source         <- tolower(scalar(source))
  scrape_url     <- scalar(scrape_url)

  if (is.na(source) || source == "") {
    if (!is.na(endpoint) && str_detect(endpoint, "worldbank\\.org"))         source <- "worldbank"
    else if (!is.na(endpoint) && str_detect(endpoint, "unstats\\.un\\.org")) source <- "unsdg"
    else if (!is.na(endpoint) && str_detect(endpoint, "api\\.dhsprogram"))   source <- "dhs"
    else if (!is.na(scrape_url) && scrape_url != "")                         source <- "scrape"
    else source <- "unknown"
  }

  out <- switch(
    source,
    "worldbank" = tryCatch(parse_worldbank_all(indicator_code, endpoint), error = function(e) empty_df()),
    "unsdg"     = tryCatch(parse_unsdg_all(indicator_code, endpoint),     error = function(e) empty_df()),
    "dhs"       = tryCatch(parse_dhs_all(indicator_code, endpoint),       error = function(e) empty_df()),
    "scrape"    = tryCatch(parse_scrape_generic(indicator_code, scrape_url), error = function(e) empty_df()),
    empty_df()
  )

  # si aucune donnée, retourner une ligne témoin "no_data"
  if (nrow(out) == 0) {
    out <- tibble(
      indicator_code = indicator_code,
      ref_area       = NA_character_,
      period         = NA_integer_,
      value          = NA_real_,
      obs_status     = "no_data",
      source         = source
    )
  }
  out
}

ingest_api_all <- function(cat_df) {
  if (is.null(cat_df) || nrow(cat_df) == 0) return(empty_df())
  purrr::pmap_dfr(
    cat_df[, c("indicator_code","endpoint","source","scrape_url")],
    function(indicator_code, endpoint, source, scrape_url) {
      route_api(indicator_code, endpoint, source, scrape_url)
    }
  )
}

# ----------------------- WDI (pagination complète) ---------------------------
# World Bank: liste [meta, data]; on boucle sur pages si besoin.
parse_worldbank_all <- function(indicator_code, endpoint) {
  if (is.na(endpoint) || endpoint == "") return(empty_df())
  base <- endpoint
  if (!str_detect(base, "format=json"))
    base <- paste0(base, ifelse(str_detect(base, "\\?"), "&", "?"), "format=json")
  if (!str_detect(base, "per_page="))
    base <- paste0(base, "&per_page=20000")

  # 1er appel pour connaître nb de pages
  js1 <- safe_fetch_json(base)
  if (length(js1) < 2) return(empty_df())
  meta <- js1[[1]]
  pages <- meta$pages %||% meta$Pages %||% 1

  all <- list(as_tibble(js1[[2]]))
  if (is.na(pages)) pages <- 1
  if (pages > 1) {
    for (p in 2:pages) {
      urlp <- paste0(base, "&page=", p)
      jsp <- safe_fetch_json(urlp)
      if (length(jsp) >= 2) all[[length(all)+1]] <- as_tibble(jsp[[2]])
    }
  }
  dat <- bind_rows(all)
  if (nrow(dat) == 0) return(empty_df())

  tibble(
    indicator_code = indicator_code,
    ref_area       = coalesce(dat$countryiso3code, dat$country$id, dat$country.value, NA_character_),
    period         = safe_int(dat$date),
    value          = safe_num(dat$value),
    obs_status     = NA_character_,
    source         = "worldbank"
  )
}

# ----------------------- UNSDG (pagination complète) -------------------------
# Ajoute pagesize=10000 et itère 'page' si 'totalPages' > 1.
parse_unsdg_all <- function(indicator_code, endpoint) {
  if (is.na(endpoint) || endpoint == "") return(empty_df())
  base <- if (str_detect(endpoint, "pagesize=")) endpoint else
    paste0(endpoint, ifelse(str_detect(endpoint, "\\?"), "&", "?"), "pagesize=10000")

  # 1er appel
  js1 <- safe_fetch_json(base)
  if (is.null(js1)) return(empty_df())
  total_pages <- js1$totalPages %||% js1$TotalPages %||% 1

  get_data <- function(js) {
    tryCatch(as_tibble(js$data), error = function(e) tibble())
  }

  out <- list(get_data(js1))
  if (!is.na(total_pages) && total_pages > 1) {
    for (p in 2:as.integer(total_pages)) {
      urlp <- paste0(base, "&page=", p)
      jsp <- safe_fetch_json(urlp)
      if (!is.null(jsp)) out[[length(out)+1]] <- get_data(jsp)
    }
  }
  dat <- bind_rows(out)
  if (nrow(dat) == 0) return(empty_df())

  tibble(
    indicator_code = indicator_code,
    ref_area       = coalesce(dat$geoAreaCode, dat$geoArea, NA_character_),
    period         = safe_int(dat$timePeriodStart),
    value          = safe_num(dat$value),
    obs_status     = coalesce(dat$attributes.Nature., dat$valueType, NA_character_),
    source         = "unsdg"
  )
}

# ----------------------- DHS (pagination complète v8 & legacy) ---------------
dhs_fetch_all <- function(endpoint) {
  base <- if (str_detect(endpoint, "f=json")) endpoint else
    paste0(endpoint, ifelse(str_detect(endpoint, "\\?"), "&", "?"), "f=json")

  add_params <- function(url, ...) {
    qs <- list(...)
    for (nm in names(qs)) {
      sep <- ifelse(str_detect(url, "\\?"), "&", "?")
      url <- paste0(url, sep, nm, "=", qs[[nm]])
    }
    url
  }

  # v8
  page_size <- 5000
  page      <- 1
  out <- list()
  repeat {
    u <- add_params(base, pageSize = page_size, pageNumber = page)
    js <- try(safe_fetch_json(u), silent = TRUE)
    if (inherits(js, "try-error") || is.null(js)) break
    dat <- try(as_tibble(js$Data), silent = TRUE)
    if (inherits(dat, "try-error") || is.null(dat)) dat <- tibble()
    out[[length(out)+1]] <- dat
    total_pages <- js$TotalPages %||% js$totalPages %||% NA
    if (is.na(total_pages) || page >= as.integer(total_pages) || nrow(dat) == 0) break
    page <- page + 1
  }
  res <- bind_rows(out)
  if (nrow(res) > 0) return(res)

  # legacy
  page_size <- 5000
  page      <- 1
  out <- list()
  repeat {
    u <- add_params(base, perpage = page_size, page = page)
    js <- try(safe_fetch_json(u), silent = TRUE)
    if (inherits(js, "try-error") || is.null(js)) break
    dat <- try(as_tibble(js$Data), silent = TRUE)
    if (inherits(dat, "try-error") || is.null(dat)) dat <- tibble()
    out[[length(out)+1]] <- dat
    total_pages <- js$TotalPages %||% js$totalPages %||% NA
    if (is.na(total_pages) || page >= as.integer(total_pages) || nrow(dat) == 0) break
    page <- page + 1
  }
  bind_rows(out)
}

parse_dhs_all <- function(indicator_code, endpoint) {
  dat <- dhs_fetch_all(endpoint)
  if (is.null(dat) || nrow(dat) == 0) return(empty_df())

  period <- dat$SurveyYear %||% dat$Year %||%
    suppressWarnings(as.integer(stringr::str_extract(dat$SurveyYearLabel %||% "", "\\d{4}")))
  ref <- coalesce(dat$CountryISO3, dat$ISO3, dat$CountryCode, NA_character_)

  tibble(
    indicator_code = indicator_code,
    ref_area       = as.character(ref),
    period         = safe_int(period),
    value          = safe_num(coalesce(dat$Value, dat$value)),
    obs_status     = coalesce(dat$Indicator, dat$CharacteristicLabel, dat$SurveyType, NA_character_),
    source         = "dhs"
  )
}

# ----------------------- Scraping CSV (ex. OWID) -----------------------------
parse_scrape_generic <- function(indicator_code, url) {
  if (is.na(url) || url == "") return(empty_df())
  df <- suppressMessages(readr::read_csv(url, show_col_types = FALSE))
  if (nrow(df) == 0) return(empty_df())

  # colonnes candidates
  year_col <- intersect(tolower(names(df)), c("year","date","period"))
  year_col <- if (length(year_col)) names(df)[match(year_col[1], tolower(names(df)))] else NA_character_
  if (is.na(year_col)) return(empty_df())

  ref_col <- NA_character_
  for (c in c("Code","ISO3","Entity","country","country_code","iso3")) {
    if (c %in% names(df)) { ref_col <- c; break }
  }

  num_cols <- names(df)[vapply(df, is.numeric, logical(1))]
  num_cols <- setdiff(num_cols, year_col)
  if (length(num_cols) == 0) return(empty_df())
  val_col <- num_cols[1]

  out <- tibble(
    indicator_code = indicator_code,
    ref_area       = if (!is.na(ref_col)) as.character(df[[ref_col]]) else NA_character_,
    period         = safe_int(df[[year_col]]),
    value          = safe_num(df[[val_col]]),
    obs_status     = NA_character_,
    source         = "scrape"
  )
  out
}

# ----------------------- export ----------------------------------------------
write_exports <- function(df, out_dir = "data_final") {
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  out_path <- file.path(out_dir, "export_latest.csv")
  df |>
    arrange(indicator_code, source, ref_area, dplyr::desc(period)) |>
    readr::write_csv(out_path, na = "")
  out_path
}
 