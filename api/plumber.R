# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(x)) y else x

# ----------------------------
# .env loading (LOCAL only)
# ----------------------------
find_env <- function() {
  p1 <- normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE)
  p2 <- normalizePath(file.path(getwd(), ".env"), mustWork = FALSE)
  p3 <- normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  for (p in c(p1, p2, p3)) if (file.exists(p)) return(p)
  NA_character_
}

DATABASE_URL <- Sys.getenv("DATABASE_URL", unset = "")
if (!nzchar(DATABASE_URL)) {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    dotenv::load_dot_env(.env.path)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] DATABASE_URL absent et .env introuvable (OK si variables Railway sont utilisées).")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

# ----------------------------
# Parse DATABASE_URL (no httr2)
# Supports: postgres://user:pass@host:port/db?sslmode=require
# ----------------------------
parse_db_url <- function(u) {
  u <- trimws(u)
  u <- sub("^postgresql://", "postgres://", u, ignore.case = TRUE)

  m <- regexec("^postgres://([^:/?#]+)(?::([^@/?#]*))?@([^:/?#]+)(?::([0-9]+))?/([^?#]+)(?:\\?(.*))?$", u, perl = TRUE)
  r <- regmatches(u, m)[[1]]
  if (length(r) == 0) stop("DATABASE_URL invalide (format attendu: postgres://user:pass@host:port/db?sslmode=...).")

  user <- utils::URLdecode(r[2])
  pass <- utils::URLdecode(r[3] %||% "")
  host <- r[4]
  port <- as.integer(r[5] %||% "5432")
  db   <- r[6]

  qs <- r[7] %||% ""
  sslmode <- ""
  if (nzchar(qs)) {
    parts <- strsplit(qs, "&", fixed = TRUE)[[1]]
    kv <- strsplit(parts, "=", fixed = TRUE)
    for (pair in kv) {
      if (length(pair) == 2 && tolower(pair[1]) == "sslmode") {
        sslmode <- utils::URLdecode(pair[2])
        break
      }
    }
  }

  list(
    host = host,
    port = port,
    dbname = db,
    user = user,
    password = pass,
    sslmode = sslmode %||% ""
  )
}

# ----------------------------
# Effective DB config
# Priority: DATABASE_URL -> PG* vars
# ----------------------------
effective_db_cfg <- function() {
  dbu <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(dbu)) {
    cfg <- parse_db_url(dbu)
    # Railway often needs sslmode=require even if missing in url
    if (!nzchar(cfg$sslmode)) cfg$sslmode <- Sys.getenv("PGSSLMODE", unset = "require")
    return(cfg)
  }

  list(
    host = Sys.getenv("PGHOST", unset = ""),
    port = as.integer(Sys.getenv("PGPORT", unset = "5432")),
    dbname = Sys.getenv("PGDATABASE", unset = ""),
    user = Sys.getenv("PGUSER", unset = Sys.getenv("PGREADUSER", unset = "")),
    password = Sys.getenv("PGPASSWORD", unset = Sys.getenv("PGREADPASS", unset = "")),
    sslmode = Sys.getenv("PGSSLMODE", unset = "prefer")
  )
}

pg_schema <- function() Sys.getenv("PGSCHEMA", unset = "public")
pg_table  <- function() Sys.getenv("PGTABLE",  unset = "")  # optional

pg_con <- function() {
  cfg <- effective_db_cfg()
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = cfg$host,
    port     = cfg$port,
    dbname   = cfg$dbname,
    user     = cfg$user,
    password = cfg$password,
    sslmode  = cfg$sslmode %||% "require"
  )
}

# ----------------------------
# Helpers: JSON-safe + table detection
# ----------------------------
json_safe_df <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0) return(df)
  for (nm in names(df)) {
    x <- df[[nm]]
    # pq_inet / inet / unknown classes -> character
    if (!is.atomic(x) || inherits(x, c("pq_inet", "inet"))) df[[nm]] <- as.character(x)
    if (inherits(x, c("POSIXct", "POSIXlt", "Date"))) df[[nm]] <- as.character(x)
    if (inherits(x, "difftime")) df[[nm]] <- as.numeric(x)
  }
  df
}

table_exists <- function(con, schema, table) {
  if (!nzchar(schema) || !nzchar(table)) return(FALSE)
  q <- "
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1;
  "
  nrow(DBI::dbGetQuery(con, q, params = list(schema, table))) > 0
}

detect_table <- function(con, schema) {
  # 1) explicit PGTABLE
  t0 <- pg_table()
  if (nzchar(t0) && table_exists(con, schema, t0)) return(t0)

  # 2) auto-detect indicator_values* in schema (prefer exact)
  q <- "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_type IN ('BASE TABLE','VIEW')
      AND table_name ILIKE 'indicator_values%'
    ORDER BY CASE WHEN table_name = 'indicator_values' THEN 0 ELSE 1 END, table_name
    LIMIT 1;
  "
  r <- DBI::dbGetQuery(con, q, params = list(schema))
  if (nrow(r) == 0) return("")
  r$table_name[[1]] %||% ""
}

list_tables_like <- function(con, schema) {
  q <- "
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_type IN ('BASE TABLE','VIEW')
    ORDER BY table_name;
  "
  DBI::dbGetQuery(con, q, params = list(schema))
}

# ----------------------------
# API Key (disabled if API_KEY empty)
# ----------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got <- req$HTTP_X_API_KEY %||% ""
  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = TRUE, message = "Unauthorized: invalid API key")
  }
}

# ----------------------------
# CORS (+ handle OPTIONS)
# ----------------------------
#* @plumber
function(pr) {
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", unset = "*"), ",")[[1]]
  origins <- trimws(origins)

  pr$registerHooks(list(
    preroute = function(req, res) {
      origin <- req$HTTP_ORIGIN %||% "*"
      allow <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
      if (identical(req$REQUEST_METHOD, "OPTIONS")) {
        res$status <- 204
        return(list())
      }
      NULL
    }
  ))
  pr
}

# ----------------------------
# Basic routes
# ----------------------------
#* @get /
function() {
  list(ok = TRUE, service = "onu_rdc_dashboard_api", time = as.character(Sys.time()))
}

#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

# ----------------------------
# Debug
# ----------------------------
#* @serializer unboxedJSON
#* @get /debug/env
function() {
  cfg <- effective_db_cfg()
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", unset = "")),
    PGHOST = Sys.getenv("PGHOST", unset = ""),
    PGPORT = Sys.getenv("PGPORT", unset = ""),
    PGDATABASE = Sys.getenv("PGDATABASE", unset = ""),
    PGUSER = Sys.getenv("PGUSER", unset = ""),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD", unset = "")),
    PGSSLMODE = Sys.getenv("PGSSLMODE", unset = ""),
    PGSCHEMA = pg_schema(),
    PGTABLE  = pg_table(),
    EFFECTIVE = list(
      host = cfg$host,
      port = cfg$port,
      dbname = cfg$dbname,
      user = cfg$user,
      sslmode = cfg$sslmode
    ),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", unset = "")
  )
}

#* @serializer unboxedJSON
#* @get /debug/tables
function(res) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  tabs <- list_tables_like(con, schema)
  list(schema = schema, tables = json_safe_df(tabs))
}

#* @serializer unboxedJSON
#* @get /debug/pingdb
function(res) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  tbl <- detect_table(con, schema)

  info <- DBI::dbGetQuery(con, "
    SELECT
      current_database() AS db,
      current_user AS usr,
      current_schema() AS schema,
      inet_server_addr() AS server_addr,
      inet_server_port() AS server_port,
      version() AS version;
  ")
  info <- json_safe_df(info)

  if (!nzchar(tbl)) {
    res$status <- 200
    return(list(
      ok = TRUE,
      detected = list(schema = schema, table = NULL),
      result = info,
      error = TRUE,
      message = "Aucune table trouvée dans public (attendu: indicator_values%). Charge d'abord les données dans Postgres Railway."
    ))
  }

  list(
    ok = TRUE,
    detected = list(schema = schema, table = tbl),
    result = info
  )
}

# ----------------------------
# Data endpoints (use detected table)
# ----------------------------

#* @get /indicators
#* @serializer unboxedJSON
function(q = "", res) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- detect_table(con, schema)
  if (!nzchar(table)) {
    res$status <- 503
    return(list(error = TRUE, message = "Table introuvable (indicator_values%). Voir /debug/tables et charger les données dans Postgres Railway."))
  }

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."{table}"
    WHERE indicator_code IS NOT NULL
  ')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                             OR indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, " ORDER BY indicator_code;")
  json_safe_df(DBI::dbGetQuery(con, qry))
}

#* @get /values
#* @serializer unboxedJSON
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0, res) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- detect_table(con, schema)
  if (!nzchar(table)) {
    res$status <- 503
    return(list(error = TRUE, message = "Table introuvable (indicator_values%). Voir /debug/tables et charger les données dans Postgres Railway."))
  }

  limit  <- max(1L, min(as.integer(limit), 10000L))
  offset <- max(0L, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::int AS n FROM "{schema}"."{table}" {where};'))$n[1]

  rows <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."{table}"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))
  rows <- json_safe_df(rows)

  list(total = total, limit = limit, offset = offset, rows = rows)
}

#* @get /export/csv
#* @serializer csv
function(indicator_code = "", ref_area = "", start = NA, end = NA, res) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- detect_table(con, schema)
  if (!nzchar(table)) {
    res$status <- 503
    return(data.frame(error = "Table introuvable (indicator_values%). Charge les données dans Postgres Railway."))
  }

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."{table}"
    WHERE 1=1
  ')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")

  DBI::dbGetQuery(con, qry)
}

#* @get /export/xlsx
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
function(indicator_code = "", ref_area = "", start = NA, end = NA, res) {
  if (!requireNamespace("writexl", quietly = TRUE)) {
    res$status <- 500
    return(plumber::as_attachment(charToRaw("writexl manquant. Ajoute-le dans tes dépendances Docker/build."), "error.txt"))
  }

  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- detect_table(con, schema)
  if (!nzchar(table)) {
    res$status <- 503
    return(plumber::as_attachment(charToRaw("Table introuvable (indicator_values%). Charge les données dans Postgres Railway."), "error.txt"))
  }

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM "{schema}"."{table}"
    WHERE 1=1
  ')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")

  dat <- DBI::dbGetQuery(con, qry)
  tf <- tempfile(fileext = ".xlsx")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

#* @get /metrics
#* @serializer html
function(res) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- detect_table(con, schema)
  if (!nzchar(table)) {
    res$status <- 503
    return("onu_api_rows_total 0\nonu_api_error 1\n")
  }

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))
  max_ins <- latest$max_ins[[1]]
  max_upd <- latest$max_upd[[1]]

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (!is.null(max_ins) && !is.na(max_ins)) as.numeric(as.POSIXct(max_ins)) else "NaN", "\n",
    "onu_api_last_updated_at ",  if (!is.null(max_upd) && !is.na(max_upd)) as.numeric(as.POSIXct(max_upd)) else "NaN", "\n"
  )
}