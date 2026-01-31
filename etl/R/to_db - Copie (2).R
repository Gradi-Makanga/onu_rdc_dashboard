# to_db.R — Ingestion TOTALE (100%) depuis le fichier *transformé* export_latest.csv
#
# ✅ Ne lit que data_final/export_latest.csv (déjà transformé).
# ✅ Pas de mélange avec d’anciens dumps : TRUNCATE des tables avant import (par défaut).
# ✅ Contrainte UNIQUE = (indicator_code, ref_area, period, source, value, obs_status).
# ✅ Dédoublonnage sur ces 6 colonnes avant insertion dans la table principale.
# ✅ Toutes les lignes sont aussi conservées en STAGING (y compris clés incomplètes).
#
# Usage direct (hors targets) :
#   source("R/to_db.R"); to_db("data_final", replace_mode = TRUE)
# Dans targets, garde la cible `db_upsert` existante qui appelle to_db("data_final").
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI); library(RPostgres); library(readr); library(dplyr); library(stringr)
})

# ------------------------- .env / Connexion PG -------------------------------
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
  try(DBI::dbExecute(con, "SET client_encoding TO 'UTF8';"), silent = TRUE)
  con
}

# ------------------------------ Utilitaires ---------------------------------
utf8_sanitize_df <- function(df) {
  char_cols <- names(df)[vapply(df, is.character, logical(1))]
  for (c in char_cols) df[[c]] <- iconv(df[[c]], from = "", to = "UTF-8", sub = "byte")
  df
}

ensure_tables_and_view <- function(con, schema="public",
                                   table_main="indicator_values",
                                   table_stg ="indicator_values_staging",
                                   view_all  ="indicator_values_all") {
  DBI::dbExecute(con, sprintf("CREATE SCHEMA IF NOT EXISTS \"%s\";", schema))
  DBI::dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS \"%1$s\".\"%2$s\" (
       indicator_code TEXT NOT NULL,
       ref_area       TEXT NOT NULL,
       period         INTEGER NOT NULL,
       value          DOUBLE PRECISION,
       obs_status     TEXT,
       source         TEXT NOT NULL,
       indicator_name TEXT,
       inserted_at    TIMESTAMPTZ DEFAULT now(),
       updated_at     TIMESTAMPTZ DEFAULT now()
     );", schema, table_main))
  DBI::dbExecute(con, sprintf("ALTER TABLE \"%s\".\"%s\" ADD COLUMN IF NOT EXISTS indicator_name TEXT;", schema, table_main))

  # --- Contrainte UNIQUE = (indicator_code, ref_area, period, source, value, obs_status)
  rel_regclass <- sprintf("\"%s\".\"%s\"", schema, table_main)
  old_default  <- sprintf("%s_indicator_code_ref_area_period_source_key", table_main)
  old_custom   <- sprintf("%s_unique_keys", table_main)
  old_unique5  <- sprintf("%s_unique5", table_main)
  new_name     <- sprintf("%s_unique6", table_main)

  sql_drop_legacy <- sprintf(
    "DO $$ BEGIN
       IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = '%s') THEN
         ALTER TABLE %s DROP CONSTRAINT \"%s\"; END IF;
       IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = '%s') THEN
         ALTER TABLE %s DROP CONSTRAINT \"%s\"; END IF;
       IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = '%s'::regclass AND conname = '%s') THEN
         ALTER TABLE %s DROP CONSTRAINT \"%s\"; END IF;
     END $$;",
    rel_regclass, old_default, rel_regclass, old_default,
    rel_regclass, old_custom,  rel_regclass, old_custom,
    rel_regclass, old_unique5, rel_regclass, old_unique5
  )
  DBI::dbExecute(con, sql_drop_legacy)

  sql_add_new <- sprintf(
    "DO $$ BEGIN
       IF NOT EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid = '%s'::regclass AND conname = '%s'
       ) THEN
         ALTER TABLE %s
         ADD CONSTRAINT \"%s\" UNIQUE (indicator_code, ref_area, period, source, value, obs_status);
       END IF;
     END $$;",
    rel_regclass, new_name, rel_regclass, new_name
  )
  DBI::dbExecute(con, sql_add_new)

  # --- Trigger updated_at
  fn_name <- sprintf("\"%s\".\"%s_touch_updated_at\"", schema, table_main)
  sql_fn <- sprintf(
    "CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$
       BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;",
    fn_name
  )
  DBI::dbExecute(con, sql_fn)

  sql_trg <- sprintf(
    "DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '%2$s_set_updated_at') THEN
         CREATE TRIGGER \"%2$s_set_updated_at\"
         BEFORE UPDATE ON \"%1$s\".\"%2$s\"
         FOR EACH ROW EXECUTE PROCEDURE %3$s();
       END IF;
     END $$;",
    schema, table_main, fn_name
  )
  DBI::dbExecute(con, sql_trg)

  # --- Table STAGING
  DBI::dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS \"%1$s\".\"%2$s\" (
       indicator_code TEXT,
       ref_area       TEXT,
       period         INTEGER,
       value          DOUBLE PRECISION,
       obs_status     TEXT,
       source         TEXT,
       indicator_name TEXT,
       load_ts        TIMESTAMPTZ DEFAULT now(),
       reason         TEXT
     );", schema, table_stg))
  DBI::dbExecute(con, sprintf("ALTER TABLE \"%s\".\"%s\" ADD COLUMN IF NOT EXISTS indicator_name TEXT;", schema, table_stg))

  # --- Vue UNION (utile si ton app lit la vue au lieu de la table main)
  DBI::dbExecute(con, sprintf("DROP VIEW IF EXISTS \"%s\".\"%s\";", schema, view_all))
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

# ------------------------- Lecture du CSV transformé ------------------------
read_any <- function(x) {
  # x: data.frame | chemin fichier | dossier
  if (is.data.frame(x)) return(x)
  stopifnot(is.character(x), length(x) == 1L)

  # Si dossier, on exige *export_latest.csv* à l'intérieur
  if (dir.exists(x)) {
    cand <- file.path(x, "export_latest.csv")
    if (!file.exists(cand)) stop("Fichier introuvable: ", cand,
                                 ". Assure-toi que le transformé a bien ce nom.")
    x <- cand
  }

  if (basename(x) != "export_latest.csv") {
    stop("Ce script n'ingère que le fichier transformé nommé *export_latest.csv*. Reçu: ", basename(x))
  }

  if (!file.exists(x)) stop('Fichier introuvable: ', x)
  message(sprintf('[to_db] lecture du fichier TRANSFORMÉ: %s', x))
  readr::read_csv(x, show_col_types = FALSE)
}

normalize_df <- function(df) {
  names(df) <- tolower(gsub("[^a-z0-9]+","_", names(df)))
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
  expected <- c("indicator_code","ref_area","period","value","obs_status","source","indicator_name")
  for (k in expected) if (!k %in% names(df)) df[[k]] <- NA

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

# --------------------------- Ingestion principale ---------------------------
# - replace_mode = TRUE  => TRUNCATE main + staging avant INSERT (pas de mélange)
# - replace_mode = FALSE => UPSERT incrémental (conflit sur 6 colonnes)
# ---------------------------------------------------------------------------

to_db <- function(x = "data_final",
                  con = NULL,
                  schema = Sys.getenv("PGSCHEMA","public"),
                  table_main = Sys.getenv("PGTABLE","indicator_values"),
                  table_stg  = paste0(Sys.getenv("PGTABLE","indicator_values"), "_staging"),
                  replace_mode = TRUE) {

  own <- is.null(con)
  if (own) con <- pg_connect()
  on.exit({ if (own && DBI::dbIsValid(con)) try(DBI::dbDisconnect(con), silent=TRUE) }, add=TRUE)

  ensure_tables_and_view(con, schema, table_main, table_stg, view_all = "indicator_values_all")

  df <- read_any(x) |> normalize_df()

  # Complétude de la clé (avec value + obs_status)
  key_ok <- !is.na(df$indicator_code) & nzchar(df$indicator_code) &
            !is.na(df$ref_area)       & nzchar(df$ref_area) &
            !is.na(df$period) &
            !is.na(df$source)         & nzchar(df$source) &
            !is.na(df$value) &
            !is.na(df$obs_status) & nzchar(df$obs_status)

  reason <- rep(NA_character_, nrow(df))
  reason[!key_ok] <- ifelse(is.na(df$indicator_code) | !nzchar(df$indicator_code), "missing indicator_code",
                       ifelse(is.na(df$ref_area)     | !nzchar(df$ref_area),     "missing ref_area",
                       ifelse(is.na(df$period),                              "missing period",
                       ifelse(is.na(df$source)     | !nzchar(df$source),       "missing source",
                       ifelse(is.na(df$value),                                "missing value",
                       ifelse(is.na(df$obs_status) | !nzchar(df$obs_status),  "missing obs_status", "unknown"))))))

  df_all <- df; df_all$reason <- reason

  DBI::dbBegin(con)
  step <- "init"
  tryCatch({
    if (replace_mode) {
      step <- "truncate tables"
      DBI::dbExecute(con, sprintf("TRUNCATE TABLE \"%s\".\"%s\";", schema, table_stg))
      DBI::dbExecute(con, sprintf("TRUNCATE TABLE \"%s\".\"%s\";", schema, table_main))
    }

    # Temp ALL
    step <- "create temp ALL"
    tmp_all <- paste0("tmp_all_", as.integer(as.numeric(Sys.time())))
    DBI::dbExecute(con, sprintf(
      "CREATE TEMP TABLE \"%s\" (
         indicator_code TEXT, ref_area TEXT, period INTEGER, value DOUBLE PRECISION,
         obs_status TEXT, source TEXT, indicator_name TEXT, reason TEXT
       ) ON COMMIT DROP;", tmp_all))

    step <- "copy ALL -> temp"
    DBI::dbWriteTable(con, tmp_all, utf8_sanitize_df(df_all), append = TRUE, row.names = FALSE)

    # Insert ALL -> STAGING
    step <- "insert ALL -> staging"
    DBI::dbExecute(con, sprintf(
      "INSERT INTO \"%1$s\".\"%2$s\"(indicator_code, ref_area, period, value, obs_status, source, indicator_name, reason)
       SELECT indicator_code, ref_area, period, value, obs_status, source, indicator_name, reason FROM \"%3$s\";",
      schema, table_stg, tmp_all))

    # Dédupe OK (6 colonnes)
    step <- "dedupe OK in R"
    df_ok <- df_all[is.na(df_all$reason), , drop = FALSE]
    if (nrow(df_ok)) {
      df_ok$.row_id <- seq_len(nrow(df_ok))
      df_ok <- df_ok |>
        dplyr::group_by(indicator_code, ref_area, period, source, value, obs_status) |>
        dplyr::arrange(.row_id, .by_group = TRUE) |>
        dplyr::slice_tail(n = 1) |>
        dplyr::ungroup() |>
        dplyr::select(-.row_id, -reason)

      # Temp OK
      step <- "create temp OK"
      tmp_ok <- paste0("tmp_ok_", as.integer(as.numeric(Sys.time())))
      DBI::dbExecute(con, sprintf(
        "CREATE TEMP TABLE \"%s\" (
           indicator_code TEXT, ref_area TEXT, period INTEGER, value DOUBLE PRECISION,
           obs_status TEXT, source TEXT, indicator_name TEXT
         ) ON COMMIT DROP;", tmp_ok))

      step <- "copy OK -> temp"
      DBI::dbWriteTable(con, tmp_ok, utf8_sanitize_df(df_ok), append = TRUE, row.names = FALSE)

      # INSERT / UPSERT
      step <- if (replace_mode) "insert main (replace mode)" else "upsert main"
      if (replace_mode) {
        DBI::dbExecute(con, sprintf(
          "INSERT INTO \"%1$s\".\"%2$s\"(indicator_code, ref_area, period, value, obs_status, source, indicator_name)
           SELECT indicator_code, ref_area, period, value, obs_status, source, indicator_name FROM \"%3$s\";",
          schema, table_main, tmp_ok))
      } else {
        DBI::dbExecute(con, sprintf(
          "INSERT INTO \"%1$s\".\"%2$s\"(indicator_code, ref_area, period, value, obs_status, source, indicator_name)
           SELECT indicator_code, ref_area, period, value, obs_status, source, indicator_name FROM \"%3$s\"
           ON CONFLICT (indicator_code, ref_area, period, source, value, obs_status)
           DO UPDATE SET
             indicator_name = COALESCE(EXCLUDED.indicator_name, \"%2$s\".indicator_name),
             updated_at     = now();",
          schema, table_main, tmp_ok))
      }
    }

    DBI::dbCommit(con)
    message(sprintf('[to_db] OK | total=%d | key_ok=%d | key_bad=%d | mode=%s',
                    nrow(df_all), sum(key_ok), sum(!key_ok), ifelse(replace_mode, "REPLACE", "UPSERT")))
    invisible(TRUE)
  }, error = function(e) {
    try(DBI::dbRollback(con), silent = TRUE)
    stop(paste0("[to_db] Transaction failed at step: ", step, "\nError: ", conditionMessage(e)), call. = FALSE)
  })
}
