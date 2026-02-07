# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x)) y else x

# ---------------------------
# 1) Charger .env UNIQUEMENT en local (et sans écraser les variables existantes)
# ---------------------------
find_env <- function() {
  cand <- c(
    normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE),
    normalizePath(file.path(getwd(), ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  )
  for (p in cand) if (file.exists(p)) return(p)
  NA_character_
}

load_env_safely <- function(path) {
  # Charge le .env mais NE remplace PAS les variables déjà définies
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  lines <- trimws(lines)
  lines <- lines[lines != "" & !startsWith(lines, "#")]

  parse_line <- function(x) {
    x <- sub("^export\\s+", "", x)
    if (!grepl("=", x, fixed = TRUE)) return(NULL)
    key <- trimws(sub("=.*$", "", x))
    val <- sub("^[^=]*=", "", x)
    val <- trimws(val)
    # enlever quotes simples/doubles si présentes
    if (nchar(val) >= 2 && ((startsWith(val, "\"") && endsWith(val, "\"")) ||
                           (startsWith(val, "'")  && endsWith(val, "'")))) {
      val <- substr(val, 2, nchar(val) - 1)
    }
    list(key = key, val = val)
  }

  kvs <- lapply(lines, parse_line)
  kvs <- kvs[!vapply(kvs, is.null, logical(1))]

  for (kv in kvs) {
    if (!nzchar(Sys.getenv(kv$key, unset = ""))) {
      Sys.setenv(kv$key = kv$val)
    }
  }
  invisible(TRUE)
}

DB_SOURCE <- "env"
if (nzchar(Sys.getenv("DATABASE_URL", unset = ""))) {
  DB_SOURCE <- "DATABASE_URL"
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
} else {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    load_env_safely(.env.path)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] Aucun .env trouvé. (OK si Railway variables).")
  }
}

# ---------------------------
# 2) Parser DATABASE_URL (sans httr2) + connexion PG
# ---------------------------
parse_database_url <- function(url) {
  # Ex: postgres://user:pass@host:5432/dbname?sslmode=require
  url0 <- url
  url0 <- sub("^postgresql://", "postgres://", url0)

  # query
  q <- ""
  if (grepl("\\?", url0)) {
    q <- sub("^[^?]*\\?", "", url0)
    url0 <- sub("\\?.*$", "", url0)
  }

  # remove scheme
  rest <- sub("^postgres://", "", url0)

  # split creds / host
  parts <- strsplit(rest, "@", fixed = TRUE)[[1]]
  if (length(parts) != 2) stop("DATABASE_URL invalide (format user:pass@host:port/db attendu).")

  cred <- parts[1]
  hostdb <- parts[2]

  # user / pass
  user <- sub(":.*$", "", cred)
  pass <- sub("^[^:]*:", "", cred)

  user <- utils::URLdecode(user)
  pass <- utils::URLdecode(pass)

  # host:port / db
  hp_db <- strsplit(hostdb, "/", fixed = TRUE)[[1]]
  if (length(hp_db) < 2) stop("DATABASE_URL invalide (dbname manquant).")

  hostport <- hp_db[1]
  dbname <- paste(hp_db[-1], collapse = "/")
  dbname <- utils::URLdecode(dbname)

  hp <- strsplit(hostport, ":", fixed = TRUE)[[1]]
  host <- hp[1]
  port <- if (length(hp) >= 2) as.integer(hp[2]) else 5432L

  # sslmode depuis query si présent
  sslmode <- Sys.getenv("PGSSLMODE", unset = "prefer")
  if (nzchar(q) && grepl("sslmode=", q, fixed = TRUE)) {
    kv <- strsplit(q, "&", fixed = TRUE)[[1]]
    kv <- kv[grepl("^sslmode=", kv)]
    if (length(kv)) sslmode <- sub("^sslmode=", "", kv[1])
  }

  list(host = host, port = port, dbname = dbname, user = user, password = pass, sslmode = sslmode)
}

pg_schema <- function() Sys.getenv("PGSCHEMA", unset = "public")
pg_table  <- function() Sys.getenv("PGTABLE",  unset = "indicator_values")

pg_con <- function() {
  if (DB_SOURCE == "DATABASE_URL") {
    cfg <- parse_database_url(Sys.getenv("DATABASE_URL"))
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = cfg$host,
      port     = cfg$port,
      dbname   = cfg$dbname,
      user     = cfg$user,
      password = cfg$password,
      sslmode  = cfg$sslmode
    )
  } else {
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = Sys.getenv("PGHOST", unset = "localhost"),
      port     = as.integer(Sys.getenv("PGPORT", unset = "5432")),
      dbname   = Sys.getenv("PGDATABASE", unset = ""),
      user     = Sys.getenv("PGUSER", unset = Sys.getenv("PGREADUSER", unset = "")),
      password = Sys.getenv("PGPASSWORD", unset = Sys.getenv("PGREADPASS", unset = "")),
      sslmode  = Sys.getenv("PGSSLMODE", unset = "prefer")
    )
  }
}

table_exists <- function(con, schema, table) {
  q <- "
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1;
  "
  res <- DBI::dbGetQuery(con, q, params = list(schema, table))
  nrow(res) > 0
}

# Convertir les colonnes non-JSON-friendly (ex pq_inet) en character
json_safe_df <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0) return(df)
  for (nm in names(df)) {
    cls <- class(df[[nm]])
    if (any(cls %in% c("pq_inet", "POSIXlt"))) df[[nm]] <- as.character(df[[nm]])
    if (inherits(df[[nm]], "POSIXct")) df[[nm]] <- format(df[[nm]], tz = "UTC", usetz = TRUE)
  }
  df
}

# ---------------------------
# 3) API Key (désactivée si API_KEY vide)
# ---------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got <- req$HTTP_X_API_KEY %||% ""

  if (identical(allowed, "") || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401L
    list(error = TRUE, message = "Unauthorized: invalid API key")
  }
}

# ---------------------------
# 4) CORS
# ---------------------------
#* @plumber
function(pr) {
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", unset = "*"), ",")[[1]]
  origins <- trimws(origins)

  pr$registerHooks(list(
    preroute = function(req, res) {
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    }
  ))
  pr
}

# ---------------------------
# 5) Endpoints
# ---------------------------

#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()), db_source = DB_SOURCE)
}

#* @get /debug/env
function() {
  list(
    db_source = DB_SOURCE,
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", unset = "")),
    PGHOST = Sys.getenv("PGHOST", unset = ""),
    PGPORT = Sys.getenv("PGPORT", unset = ""),
    PGDATABASE = Sys.getenv("PGDATABASE", unset = ""),
    PGUSER = Sys.getenv("PGUSER", unset = ""),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD", unset = "")),
    PGSSLMODE = Sys.getenv("PGSSLMODE", unset = ""),
    PGSCHEMA = pg_schema(),
    PGTABLE  = pg_table(),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", unset = "")
  )
}

#* @get /debug/pingdb
#* @serializer unboxedJSON
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  # ping + table check
  info <- DBI::dbGetQuery(con, "SELECT current_database() AS db, current_user AS usr, current_schema() AS schema, inet_server_addr() AS server_addr, inet_server_port() AS server_port, version();")
  info <- json_safe_df(info)

  exists <- table_exists(con, schema, table)

  list(
    ok = TRUE,
    config = list(
      schema = schema,
      table  = table,
      db_source = DB_SOURCE
    ),
    table_exists = exists,
    result = info
  )
}

#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Vérifie PGTABLE/PGSCHEMA ou charge les données dans Railway Postgres."))
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
  qry <- paste0(qry, ' ORDER BY indicator_code, ref_area, period;')

  DBI::dbGetQuery(con, qry)
}

#* @get /indicators
#* @serializer unboxedJSON
function(q = "") {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}."))
  }

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."{table}"
    WHERE indicator_code IS NOT NULL
  ')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                         OR  indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, " ORDER BY indicator_code;")
  json_safe_df(DBI::dbGetQuery(con, qry))
}

#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}."))
  }

  limit  <- max(1L, min(as.integer(limit), 10000L))
  offset <- max(0L, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::int AS n FROM "{schema}"."{table}" {where};'))$n[1]
  rows  <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."{table}"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))

  list(total = total, limit = limit, offset = offset, rows = json_safe_df(rows))
}

#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}."))
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

  if (!requireNamespace("writexl", quietly = TRUE)) {
    stop("Package 'writexl' manquant dans l'image Docker. Ajoute-le dans l'installation des packages.")
  }
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

#* @serializer html
#* @get /metrics
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    return(paste0("onu_api_table_exists 0\n"))
  }

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  max_ins <- suppressWarnings(as.numeric(as.POSIXct(latest$max_ins[[1]])))
  max_upd <- suppressWarnings(as.numeric(as.POSIXct(latest$max_upd[[1]])))

  paste0(
    "onu_api_table_exists 1\n",
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", (max_ins %||% NA), "\n",
    "onu_api_last_updated_at ",  (max_upd %||% NA), "\n"
  )
} 