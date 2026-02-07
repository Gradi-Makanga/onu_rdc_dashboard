# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(readr)
  library(dplyr)
  library(glue)
  library(dotenv)
})

# -----------------------------
# Helpers
# -----------------------------
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x)) y else x

# Trouver .env : priorité racine projet (/app/.env), puis répertoire courant
find_env <- function() {
  candidates <- c(
    normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE),  # ex: /app/.env si getwd=/app/api
    normalizePath(file.path(getwd(), ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(dirname(getwd())), ".env"), mustWork = FALSE)
  )
  for (p in candidates) if (file.exists(p)) return(p)
  NA_character_
}

.env.path <- find_env()
if (is.na(.env.path)) stop("❌ Fichier .env introuvable (place-le à la racine du projet).")
dotenv::load_dot_env(.env.path)  # (pas d'argument override: compat vieux dotenv)
message("[plumber] .env chargé depuis: ", .env.path)

pg_con <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
    sslmode  = Sys.getenv("PGSSLMODE", "prefer")
  )
}

# Convertir colonnes "exotiques" (inet, etc.) en types JSON-friendly
sanitize_df <- function(x) {
  if (!is.data.frame(x)) return(x)
  for (nm in names(x)) {
    col <- x[[nm]]
    # pq_inet / autres classes non sérialisables -> character
    if (!is.atomic(col) || inherits(col, c("pq_inet", "pq_bytea", "POSIXlt"))) {
      x[[nm]] <- as.character(col)
    }
  }
  x
}

schema_name <- function() Sys.getenv("PGSCHEMA", "public")
table_name  <- function() Sys.getenv("PGTABLE", "indicator_values")  # <- modifiable côté Railway

qualified_table_sql <- function(con) {
  sch <- schema_name()
  tab <- table_name()
  # Noms SQL sécurisés
  paste0(DBI::dbQuoteIdentifier(con, sch), ".", DBI::dbQuoteIdentifier(con, tab))
}

table_exists <- function(con) {
  sch <- schema_name()
  tab <- table_name()
  n <- DBI::dbGetQuery(
    con,
    "SELECT COUNT(*)::int AS n
     FROM information_schema.tables
     WHERE table_schema = $1 AND table_name = $2;",
    params = list(sch, tab)
  )$n[1]
  isTRUE(n > 0)
}

suggest_tables <- function(con) {
  DBI::dbGetQuery(
    con,
    "SELECT table_schema, table_name
     FROM information_schema.tables
     WHERE table_type='BASE TABLE'
       AND (table_name ILIKE '%indicator%' OR table_name ILIKE '%value%')
     ORDER BY table_schema, table_name
     LIMIT 50;"
  ) |>
    sanitize_df()
}

stop_table_missing <- function(con) {
  msg <- glue(
    "La table/vue {schema_name()}.{table_name()} n'existe pas. ",
    "Vérifie PGSCHEMA / PGTABLE (variables Railway)."
  )
  list(
    ok = FALSE,
    error = TRUE,
    message = msg,
    current = list(PGSCHEMA = schema_name(), PGTABLE = table_name()),
    suggestions = suggest_tables(con)
  )
}

# -----------------------------
# Global plumber config (CORS + error handler)
# -----------------------------
#* @plumber
function(pr) {
  # CORS
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]] |> trimws()
  pr$registerHooks(list(
    preroute = function(req, res) {
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    }
  ))

  # Error handler -> JSON propre (évite "An exception occurred." sans détails)
  pr$setErrorHandler(function(req, res, err) {
    res$setHeader("Content-Type", "application/json")
    res$status <- 500
    list(error = TRUE, message = conditionMessage(err))
  })

  pr
}

# -----------------------------
# API Key filter (désactivée si API_KEY vide)
# -----------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""
  if (identical(allowed, "") || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = TRUE, message = "Unauthorized: invalid API key")
  }
}

# -----------------------------
# Routes
# -----------------------------

#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

#* @get /debug/env
function() {
  list(
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGREADUSER = Sys.getenv("PGREADUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGREADPASS_set = nzchar(Sys.getenv("PGREADPASS")),
    PGSSLMODE = Sys.getenv("PGSSLMODE"),
    PGSCHEMA = Sys.getenv("PGSCHEMA", "public"),
    PGTABLE  = Sys.getenv("PGTABLE", "indicator_values"),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN")
  )
}

#* @get /debug/pingdb
function(res) {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Ping + info serveur (server_addr peut être inet -> converti en char)
  out <- tryCatch({
    q <- "
      SELECT
        current_database() AS db,
        current_user AS usr,
        current_schema() AS schema,
        inet_server_addr() AS server_addr,
        inet_server_port() AS server_port,
        version() AS version;
    "
    df <- DBI::dbGetQuery(con, q) |> sanitize_df()
    list(
      ok = TRUE,
      config = list(
        host = Sys.getenv('PGHOST'),
        port = as.integer(Sys.getenv('PGPORT', '5432')),
        dbname = Sys.getenv('PGDATABASE'),
        user = Sys.getenv('PGUSER', Sys.getenv('PGREADUSER',''))
      ),
      result = df
    )
  }, error = function(e) {
    res$status <- 500
    list(ok = FALSE, error = TRUE, message = conditionMessage(e))
  })

  out
}

# ---- Export CSV ----
#* @serializer csv
#* @get /export/csv
function(res, indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (!table_exists(con)) {
    res$status <- 500
    return(stop_table_missing(con))
  }

  tbl <- qualified_table_sql(con)

  qry <- glue("
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM {tbl}
    WHERE 1=1
  ")

  if (nzchar(indicator_code)) qry <- paste0(qry, glue(" AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}"))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(" AND ref_area = {DBI::dbQuoteString(con, ref_area)}"))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(" AND period >= {as.numeric(start)}"))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(" AND period <= {as.numeric(end)}"))

  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  DBI::dbGetQuery(con, qry) |> sanitize_df()
}

# ---- Liste indicateurs ----
#* @get /indicators
function(res, q = "") {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (!table_exists(con)) {
    res$status <- 500
    return(stop_table_missing(con))
  }

  tbl <- qualified_table_sql(con)

  qry <- glue("
    SELECT DISTINCT indicator_code, indicator_name
    FROM {tbl}
    WHERE indicator_code IS NOT NULL
  ")

  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    qry <- paste0(qry, glue("
      AND (
        indicator_code ILIKE {DBI::dbQuoteString(con, like)}
        OR indicator_name ILIKE {DBI::dbQuoteString(con, like)}
      )
    "))
  }

  qry <- paste0(qry, " ORDER BY indicator_code;")
  DBI::dbGetQuery(con, qry) |> sanitize_df()
}

# ---- JSON paginé ----
#* @serializer unboxedJSON
#* @get /values
function(res, indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (!table_exists(con)) {
    res$status <- 500
    return(stop_table_missing(con))
  }

  tbl <- qualified_table_sql(con)

  limit  <- max(1, min(as.integer(limit), 10000))
  offset <- max(0, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(" AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}"))
  if (nzchar(ref_area))       where <- paste0(where, glue(" AND ref_area = {DBI::dbQuoteString(con, ref_area)}"))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(" AND period >= {as.numeric(start)}"))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(" AND period <= {as.numeric(end)}"))

  total <- DBI::dbGetQuery(con, glue("SELECT COUNT(*)::int AS n FROM {tbl} {where};"))$n[1]

  rows <- DBI::dbGetQuery(con, glue("
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM {tbl}
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  ")) |> sanitize_df()

  list(total = total, limit = limit, offset = offset, rows = rows)
}

# ---- Export XLSX ----
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(res, indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (!table_exists(con)) {
    res$status <- 500
    return(stop_table_missing(con))
  }

  tbl <- qualified_table_sql(con)

  qry <- glue("
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM {tbl}
    WHERE 1=1
  ")

  if (nzchar(indicator_code)) qry <- paste0(qry, glue(" AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}"))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(" AND ref_area = {DBI::dbQuoteString(con, ref_area)}"))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(" AND period >= {as.numeric(start)}"))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(" AND period <= {as.numeric(end)}"))

  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  dat <- DBI::dbGetQuery(con, qry) |> sanitize_df()

  tf <- tempfile(fileext = ".xlsx")
  if (!requireNamespace("writexl", quietly = TRUE)) install.packages("writexl")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ---- Metrics (texte) ----
#* @serializer html
#* @get /metrics
function(res) {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (!table_exists(con)) {
    res$status <- 500
    m <- stop_table_missing(con)
    return(paste0("onu_api_error 1\nonu_api_error_message \"", gsub("\"", "'", m$message), "\"\n"))
  }

  tbl <- qualified_table_sql(con)

  n <- DBI::dbGetQuery(con, glue("SELECT COUNT(*)::bigint AS n FROM {tbl};"))$n[1]
  latest <- DBI::dbGetQuery(con, glue("SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM {tbl};"))

  max_ins <- suppressWarnings(as.numeric(as.POSIXct(latest$max_ins[[1]])))
  max_upd <- suppressWarnings(as.numeric(as.POSIXct(latest$max_upd[[1]])))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", ifelse(is.finite(max_ins), max_ins, "NaN"), "\n",
    "onu_api_last_updated_at ",  ifelse(is.finite(max_upd), max_upd, "NaN"), "\n"
  )
}