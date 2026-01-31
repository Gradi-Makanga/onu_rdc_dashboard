# to_db.R — Ingestion permissive + indicator_name + encodage UTF-8 garanti
# - Sanitize UTF-8: iconv() sur toutes les colonnes caractère (sub="byte")
# - SET client_encoding = 'UTF8' côté Postgres
# - Logs précis de transaction, rollback propre
# ------------------------------------------------------------------------------
suppressPackageStartupMessages({
  library(DBI); library(RPostgres); library(readr); library(dplyr); library(stringr)
})

# Charger .env si présent
load_dotenv_if_any <- function() {
  if (requireNamespace("dotenv", quietly = TRUE)) {
    for (p in c(".env","../.env")) {
      if (file.exists(p)) { try(dotenv::load_dot_env(file = p), silent = TRUE); break }
    }
  } else {
    if (file.exists(".env")) readRenviron(".env") else if (file.exists("../.env")) readRenviron("../.env")
  }
}

pg_connect <- function() {
  load_dotenv_if_any()
  need <- c("PGHOST","PGDATABASE","PGUSER","PGPASSWORD")
  miss <- need[!nzchar(Sys.getenv(need))]
  if (length(miss)) stop("Missing required env vars: ", paste(miss, collapse=", "))
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT","5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode  = Sys.getenv("PGSSLMODE","prefer")
  )
  # Forcer l'encodage côté session
  try(DBI::dbExecute(con, "SET client_encoding TO 'UTF8';"), silent = TRUE)
  con
}

# ---- helpers encodage --------------------------------------------------------
utf8_sanitize_df <- function(df) {
  # Convertit toutes les colonnes caractère en UTF-8, remplace octets invalides par \\xNN
  char_cols <- names(df)[vapply(df, is.character, logical(1))]
  for (c in char_cols) {
    df[[c]] <- iconv(df[[c]], from = "", to = "UTF-8", sub = "byte")
  }
  df
}

ensure_tables_and_view <- function(con, schema="public",
                                   table_main="indicator_values",
                                   table_stg ="indicator_values_staging",
                                   view_all  ="indicator_values_all") {

  # Table principale (clés NOT NULL + UNIQUE)
  DBI::dbExecute(con, sprintf('CREATE SCHEMA IF NOT EXISTS "%s";', schema))
  DBI::dbExecute(con, sprintf(
    'CREATE TABLE IF NOT EXISTS "%1$s"."%2$s" (
       indicator_code TEXT NOT NULL,
       ref_area       TEXT NOT NULL,
       period         INTEGER NOT NULL,
       value          DOUBLE PRECISION,
       obs_status     TEXT,
       source         TEXT NOT NULL,
       indicator_name TEXT,
       inserted_at    TIMESTAMPTZ DEFAULT now(),
       updated_at     TIMESTAMPTZ DEFAULT now()
     );', schema, table_main))
  DBI::dbExecute(con, sprintf('ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS indicator_name TEXT;', schema, table_main))

  # Contrainte UNIQUE si absente
  rel_regclass <- sprintf('"%s"."%s"', schema, table_main)
  conname <- sprintf('%s_unique_keys', table_main)
  sql_check_add <- sprintf(
    "DO $do$ BEGIN
       IF NOT EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid = '%s'::regclass AND conname = '%s'
       ) THEN
         ALTER TABLE %s
         ADD CONSTRAINT \"%s\" UNIQUE (indicator_code, ref_area, period, source);
       END IF;
     END $do$;",
    rel_regclass, conname, rel_regclass, conname
  )
  DBI::dbExecute(con, sql_check_add)

  # Trigger updated_at (si absent)
  DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE FUNCTION %1$s_touch_updated_at() RETURNS trigger AS $$
     BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;", table_main))
  sql_trg <- sprintf(
    "DO $do$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '%2$s_set_updated_at') THEN
         CREATE TRIGGER \"%2$s_set_updated_at\"
         BEFORE UPDATE ON \"%1$s\".\"%2$s\"
         FOR EACH ROW EXECUTE PROCEDURE %2$s_touch_updated_at();
       END IF;
     END $do$;",
    schema, table_main
  )
  DBI::dbExecute(con, sql_trg)

  # Table staging (clés NULL autorisées + reason + indicator_name)
  DBI::dbExecute(con, sprintf(
    'CREATE TABLE IF NOT EXISTS "%1$s"."%2$s" (
       indicator_code TEXT,
       ref_area       TEXT,
       period         INTEGER,
       value          DOUBLE PRECISION,
       obs_status     TEXT,
       source         TEXT,
       indicator_name TEXT,
       load_ts        TIMESTAMPTZ DEFAULT now(),
       reason         TEXT
     );', schema, table_stg))
  DBI::dbExecute(con, sprintf('ALTER TABLE "%s"."%s" ADD COLUMN IF NOT EXISTS indicator_name TEXT;', schema, table_stg))

  # Vue union (drop/recreate propre)
  DBI::dbExecute(con, sprintf('DROP VIEW IF EXISTS "%s"."%s";', schema, view_all))
  sql_view <- sprintf(
    "CREATE VIEW \"%s\".\"%s\" AS
       SELECT 'main'::text AS record_origin,
              indicator_code, ref_area, period, value, obs_status, source, indicator_name, inserted_at, updated_at
       FROM \"%s\".\"%s\"
     UNION ALL
       SELECT 'staging'::text AS record_origin,
              indicator_code, ref_area, period, value, obs_status,
              COALESCE(source, 'unknown') AS source,
              indicator_name,
              load_ts AS inserted_at, load_ts AS updated_at
       FROM \"%s\".\"%s\";",
    schema, view_all, schema, table_main, schema, table_stg
  )
  DBI::dbExecute(con, sql_view)
}

read_any <- function(x) {
  if (is.data.frame(x)) return(x)
  stopifnot(is.character(x), length(x)==1L)
  if (!file.exists(x)) stop('Fichier introuvable: ', x)
  readr::read_csv(x, show_col_types = FALSE)
}

normalize_df <- function(df) {
  # standardiser les noms
  names(df) <- tolower(gsub("[^a-z0-9]+","_", names(df)))

  # alias possibles -> copie si dispo
  alias <- list(
    indicator_code = c("indicator_code","indicator","code","ind_code","indicateur"),
    ref_area       = c("ref_area","iso3","countryiso3","country_code","pays","country","geo","adm0_iso3"),
    period         = c("period","year","time","annee","date"),
    value          = c("value","val","valeur","obs_value"),
    obs_status     = c("obs_status","status","statut","obsstat"),
    source         = c("source","provider","origine","dataset","api"),
    indicator_name = c("indicator_name","name","indicator_label","indicateur_nom","label","title")
  )
  for (k in names(alias)) if (!k %in% names(df)) {
    cand <- intersect(alias[[k]], names(df))
    if (length(cand)) df[[k]] <- df[[cand[1]]]
  }

  # s'assurer que TOUTES les colonnes attendues existent (sinon NA)
  expected <- c("indicator_code","ref_area","period","value","obs_status","source","indicator_name")
  for (k in expected) if (!k %in% names(df)) df[[k]] <- NA

  # normalisation / typage + UTF-8 sanitize
  df |>
    dplyr::mutate(
      indicator_code = ifelse(is.na(indicator_code), NA_character_, stringr::str_squish(as.character(indicator_code))),
      ref_area       = ifelse(is.na(ref_area), NA_character_, toupper(stringr::str_squish(as.character(ref_area)))),
      period         = suppressWarnings(as.integer(period)),
      value          = suppressWarnings(as.numeric(value)),
      obs_status     = ifelse(is.na(obs_status), NA_character_, as.character(obs_status)),
      source         = ifelse(is.na(source), NA_character_, stringr::str_squish(as.character(source))),
      indicator_name = ifelse(is.na(indicator_name), NA_character_, as.character(indicator_name)),
      value          = ifelse(is.finite(value), value, NA_real_)
    ) |>
    utf8_sanitize_df() |>
    dplyr::select(dplyr::any_of(expected))
}

to_db <- function(x = "data_final/export_latest.csv",
                  con = NULL,
                  schema = Sys.getenv("PGSCHEMA","public"),
                  table_main = Sys.getenv("PGTABLE","indicator_values"),
                  table_stg  = paste0(Sys.getenv("PGTABLE","indicator_values"), "_staging")) {

  own <- is.null(con)
  if (own) con <- pg_connect()
  on.exit({ if (own && DBI::dbIsValid(con)) try(DBI::dbDisconnect(con), silent=TRUE) }, add=TRUE)

  ensure_tables_and_view(con, schema, table_main, table_stg, view_all = "indicator_values_all")

  df <- read_any(x)
  df <- normalize_df(df)

  # Séparer clés complètes vs manquantes
  key_ok <- !is.na(df$indicator_code) & nzchar(df$indicator_code) &
            !is.na(df$ref_area)       & nzchar(df$ref_area) &
            !is.na(df$period) &
            !is.na(df$source)         & nzchar(df$source)

  df_ok  <- df[key_ok, , drop = FALSE]
  df_bad <- df[!key_ok, , drop = FALSE]

  # Dédoublonner côté OK (évite "ON CONFLICT ... affect row a second time")
  if (nrow(df_ok)) {
    df_ok$.row_id <- seq_len(nrow(df_ok))
    df_ok <- df_ok |>
      dplyr::group_by(indicator_code, ref_area, period, source) |>
      dplyr::arrange(is.na(value), .row_id, .by_group = TRUE) |>
      dplyr::slice_tail(n = 1) |>
      dplyr::ungroup() |>
      dplyr::select(-.row_id)
  }

  message(sprintf("[to_db] lignes OK: %d | lignes staging: %d", nrow(df_ok), nrow(df_bad)))

  # Transaction avec logs précis
  DBI::dbBegin(con)
  step <- "init"
  tryCatch({
    # 1) UPSERT des lignes OK vers la table principale
    if (nrow(df_ok)) {
      # Sécurité UTF-8 au cas où
      df_ok <- utf8_sanitize_df(df_ok)

      step <- "create temp table OK"
      tmp1 <- paste0("tmp_ok_", as.integer(Sys.time()))
      DBI::dbExecute(con, sprintf(
        'CREATE TEMP TABLE "%s" (
           indicator_code TEXT, ref_area TEXT, period INTEGER, value DOUBLE PRECISION,
           obs_status TEXT, source TEXT, indicator_name TEXT
         ) ON COMMIT DROP;', tmp1))

      step <- "copy OK -> temp"
      DBI::dbWriteTable(con, tmp1, df_ok, append = TRUE, row.names = FALSE)

      step <- "upsert OK -> main"
      DBI::dbExecute(con, sprintf(
        'INSERT INTO "%1$s"."%2$s"(indicator_code, ref_area, period, value, obs_status, source, indicator_name)
         SELECT indicator_code, ref_area, period, value, obs_status, source, indicator_name FROM "%3$s"
         ON CONFLICT (indicator_code, ref_area, period, source)
         DO UPDATE SET
           value          = EXCLUDED.value,
           obs_status     = EXCLUDED.obs_status,
           indicator_name = COALESCE(EXCLUDED.indicator_name, "%2$s".indicator_name),
           updated_at     = now();',
        schema, table_main, tmp1))
    }

    # 2) INSERT des lignes BAD vers la table staging avec raison
    if (nrow(df_bad)) {
      df_bad <- utf8_sanitize_df(df_bad)

      step <- "prepare staging data"
      reasons <- ifelse(is.na(df_bad$indicator_code) | !nzchar(df_bad$indicator_code), "missing indicator_code",
                 ifelse(is.na(df_bad$ref_area) | !nzchar(df_bad$ref_area), "missing ref_area",
                 ifelse(is.na(df_bad$period), "missing period",
                 ifelse(is.na(df_bad$source) | !nzchar(df_bad$source), "missing source", "unknown"))))
      df_bad$reason <- reasons

      step <- "create temp table BAD"
      tmp2 <- paste0("tmp_bad_", as.integer(Sys.time()))
      DBI::dbExecute(con, sprintf(
        'CREATE TEMP TABLE "%s" (
           indicator_code TEXT, ref_area TEXT, period INTEGER, value DOUBLE PRECISION,
           obs_status TEXT, source TEXT, indicator_name TEXT, reason TEXT
         ) ON COMMIT DROP;', tmp2))

      step <- "copy BAD -> temp"
      DBI::dbWriteTable(con, tmp2, df_bad, append = TRUE, row.names = FALSE)

      step <- "insert BAD -> staging"
      DBI::dbExecute(con, sprintf(
        'INSERT INTO "%1$s"."%2$s"(indicator_code, ref_area, period, value, obs_status, source, indicator_name, reason)
         SELECT indicator_code, ref_area, period, value, obs_status, source, indicator_name, reason FROM "%3$s";',
        schema, table_stg, tmp2))
    }

    step <- "commit"
    DBI::dbCommit(con)
    message(sprintf("[to_db] Upsert(main)=%d  |  Insert(staging)=%d  -> OK",
                    nrow(df_ok), nrow(df_bad)))
    invisible(TRUE)
  }, error = function(e) {
    try(DBI::dbRollback(con), silent = TRUE)
    stop(paste0("[to_db] Transaction failed at step: ", step, "\nError: ", conditionMessage(e)), call. = FALSE)
  })
}
