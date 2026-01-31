# _targets.R — cat -> api_raw -> data_final -> data_named -> export_latest -> db_upsert
suppressPackageStartupMessages({
  library(targets); library(dplyr); library(readr); library(stringr); library(tidyr)
})

# Charger .env
if (requireNamespace("dotenv", quietly = TRUE) && file.exists(".env")) {
  dotenv::load_dot_env(".env")
} else if (file.exists(".env")) {
  readRenviron(".env")
}

tar_option_set(
  packages = c("dplyr","purrr","stringr","readr","httr2","jsonlite","tibble","tidyr",
               "DBI","RPostgres","arrow","rvest","xml2")
)

# Source toutes tes fonctions (WD = etl/)
targets::tar_source("R")

# -------- Helpers catalogue: extraction code WB depuis api_endpoint/scrape_url
extract_wb_code <- function(x) {
  x <- tolower(trimws(as.character(x)))
  if (length(x) == 0) return(character(0))
  code1 <- stringr::str_match(x, "/indicator/([a-z0-9\\._]+)")[,2]
  code2 <- stringr::str_match(x, "indicator=([a-z0-9\\._]+)")[,2]
  dplyr::coalesce(code1, code2)
}
col_or_na <- function(df, name) {
  if (name %in% names(df)) df[[name]] else rep(NA_character_, nrow(df))
}
build_indicator_lookup <- function(cat_df) {
  api_col    <- col_or_na(cat_df, "api_endpoint")
  scrape_col <- col_or_na(cat_df, "scrape_url")
  wb_any <- dplyr::coalesce(extract_wb_code(api_col), extract_wb_code(scrape_col))
  cat_df %>%
    dplyr::mutate(
      indicator_code_std = tolower(trimws(as.character(.data$indicator_code))),
      indicator_name_std = dplyr::coalesce(as.character(.data$indicator_name), NA_character_),
      source_std         = tolower(trimws(as.character(.data$source))),
      wb_code            = dplyr::if_else(source_std == "worldbank", wb_any, NA_character_)
    ) %>%
    dplyr::select(indicator_code_std, indicator_name_std, source_std, wb_code) %>%
    dplyr::distinct()
}
add_indicator_names2 <- function(df, cat_df) {
  lk <- build_indicator_lookup(cat_df)
  ind_vec <- dplyr::coalesce(
    tolower(trimws(as.character(col_or_na(df, "indicator")))),
    tolower(trimws(as.character(col_or_na(df, "indicator_id")))),
    tolower(trimws(as.character(col_or_na(df, "series_code")))),
    tolower(trimws(as.character(col_or_na(df, "series_id")))),
    tolower(trimws(as.character(col_or_na(df, "wb_indicator")))),
    tolower(trimws(as.character(col_or_na(df, "wb_code"))))
  )
  df0 <- df %>%
    dplyr::mutate(
      indicator_code = tolower(trimws(as.character(dplyr::coalesce(.data$indicator_code, ind_vec)))),
      source         = tolower(trimws(as.character(.data$source))),
      wb_indicator   = ind_vec
    )
  out <- df0 %>%
    dplyr::left_join(lk %>% dplyr::select(indicator_code_std, indicator_name_std),
                     by = c("indicator_code" = "indicator_code_std")) %>%
    dplyr::left_join(
      lk %>% dplyr::filter(!is.na(.data$wb_code), .data$wb_code != "") %>% dplyr::select(wb_code, indicator_name_std),
      by = dplyr::join_by(wb_indicator == wb_code)
    ) %>%
    dplyr::mutate(
      indicator_name = dplyr::coalesce(.data$indicator_name_std.x, .data$indicator_name_std.y),
      indicator_code = toupper(.data$indicator_code)
    ) %>%
    dplyr::select(-dplyr::ends_with(".x"), -dplyr::ends_with(".y")) %>%
    dplyr::select(dplyr::everything(), .data$indicator_name)
  out
}

list(
  tar_target(
    cat,
    {
      path_cat <- if (file.exists("../catalogue/catalogue_enriched.csv")) "../catalogue/catalogue_enriched.csv" else "catalogue/catalogue_enriched.csv"
      load_catalogue(path_cat)
    }
  ),
  tar_target(
    api_raw,
    ingest_api_all(cat)
  ),
  tar_target(
    data_final,
    ensure_schema(api_raw)
  ),
  tar_target(
    data_named,
    add_indicator_names2(data_final, cat)
  ),
  tar_target(
    export_latest,
    {
      out <- tryCatch(write_exports(data_named, out_dir = "data_final"), error = function(e) e)
      path <- if (is.character(out) && length(out) == 1) out else "data_final/export_latest.csv"
      if (!file.exists(path)) {
        dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
        readr::write_csv(data_named, path)
      }
      path
    },
    format = "file"
  ),
  tar_target(
    db_upsert,
    {
      to_db(export_latest)
      "ok"
    }
  )
)
