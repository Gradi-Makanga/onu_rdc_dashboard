
# to_postgres.R — write cleaned data to PostgreSQL and export files
library(DBI); library(RPostgres); library(dplyr); library(arrow); library(readr)

pg_con <- function() {
  dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("PGHOST"),
    port = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname = Sys.getenv("PGDATABASE"),
    user = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode = Sys.getenv("PGSSLMODE","prefer")
  )
}

# to_postgres.R — patch bind_and_clean (éviter doublons & normaliser)
library(dplyr); library(tidyr); library(tidyselect)

# ---- helpers coalesce ----
coalesce_chr <- function(...) {
  x <- list(...)
  x <- lapply(x, function(v) if (is.null(v)) NA_character_ else as.character(v))
  dplyr::coalesce(!!!x)
}
coalesce_num <- function(...) {
  x <- list(...)
  x <- lapply(x, function(v) if (is.null(v)) NA_real_ else suppressWarnings(as.numeric(v)))
  dplyr::coalesce(!!!x)
}

# ---- normaliseurs sans { } ----
normalize_api_df <- function(df){
  if (is.null(df) || !nrow(df)) return(NULL)
  df <- tidyr::unnest(df, cols = c(raw), names_sep = "_")
  df$indicator_code <- coalesce_chr(df$raw_indicator_code, df$indicator_code)
  df$ref_area       <- coalesce_chr(df$raw_ref_area, df$ref_area, df$raw_countryiso3code, df$raw_iso3c, df$raw_iso3)
  df$period         <- coalesce_chr(df$raw_period, df$period, df$raw_date, df$raw_year, df$raw_time)
  df$value          <- coalesce_num(df$raw_value, df$value, df$raw_val, df$raw_obs_value)
  df$obs_status     <- coalesce_chr(df$raw_obs_status, df$obs_status)
  df <- dplyr::select(df, indicator_code, ref_area, period, value, obs_status)
  df <- dplyr::filter(df, !is.na(value), !is.na(period), !is.na(ref_area), nzchar(ref_area), nzchar(period))
  df
}

normalize_scrape_df <- function(df){
  if (is.null(df) || !nrow(df)) return(NULL)
  df <- tidyr::unnest(df, cols = c(raw), names_sep = "_")
  df$indicator_code <- coalesce_chr(df$raw_indicator_code, df$indicator_code)
  df$ref_area       <- coalesce_chr(df$raw_ref_area, df$ref_area, df$raw_iso3, df$raw_pays, df$raw_country)
  df$period         <- coalesce_chr(df$raw_period, df$period, df$raw_year, df$raw_date, df$raw_time)
  df$value          <- coalesce_num(df$raw_value, df$value, df$raw_val, df$raw_obs_value)
  df$obs_status     <- coalesce_chr(df$raw_obs_status, df$obs_status)
  df <- dplyr::select(df, indicator_code, ref_area, period, value, obs_status)
  df <- dplyr::filter(df, !is.na(value), !is.na(period), !is.na(ref_area), nzchar(ref_area), nzchar(period))
  df
}

# ---- fonction principale ----
bind_and_clean <- function(api_raw, scrape_raw) {
  d_api <- if (!is.null(api_raw)   && nrow(api_raw))   normalize_api_df(api_raw)   else NULL
  d_scr <- if (!is.null(scrape_raw) && nrow(scrape_raw)) normalize_scrape_df(scrape_raw) else NULL
  dplyr::bind_rows(d_api, d_scr)
}



write_to_postgres <- function(df) {
  if (is.null(df) || !nrow(df)) return(invisible(TRUE))
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbWriteTable(con, "staging_raw", df, append = TRUE, row.names = FALSE)
  invisible(TRUE)
}

write_exports <- function(df, out_dir) {
  if (is.null(df) || !nrow(df)) {
    message("⚠️ Aucun enregistrement à exporter")
    return(invisible(FALSE))
  }
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(df, file.path(out_dir, "export_latest.csv"))
  arrow::write_parquet(df, file.path(out_dir, "export_latest.parquet"))
  message("✅ Export écrit dans ", out_dir)
  invisible(TRUE)
} 

