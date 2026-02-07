# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dplyr)
  library(readr)
  library(dotenv)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(as.character(x))) y else x

# --- Charger .env si présent (mais NE PAS bloquer si absent, ex: Railway) ---
find_env <- function() {
  c(
    normalizePath(file.path(getwd(), ".env"), mustWork = FALSE),
    normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  ) |>
    (\(p) p[file.exists(p)][1])() |>
    (\(p) if (length(p) == 0) NA_character_ else p)()
}

.env.path <- find_env()
if (!is.na(.env.path)) {
  # NOTE: certaines versions de dotenv n'ont pas l'argument override
  dotenv::load_dot_env(.env.path)
  message("[plumber] .env chargé depuis: ", .env.path)
} else {
  message("[plumber] Aucun .env trouvé (OK si les variables sont définies dans Railway).")
}

# --- Parse DATABASE_URL (Railway) : postgres://user:pass@host:port/db?sslmode=require ---
parse_database_url <- function(u) {
  u <- trimws(u)
  if (!nzchar(u)) return(NULL)

  u2 <- sub("^postgresql://", "postgres://", u, ignore.case = TRUE)

  # scheme://userinfo@host:port/path?query
  m <- regexec("^(\\w+)://([^@/]+)@([^:/\\?]+)(?::(\\d+))?/([^\\?]+)(?:\\?(.*))?$", u2, perl = TRUE)
  r <- regmatches(u2, m)[[1]]
  if (length(r) == 0) return(NULL)

  scheme <- r[2]
  userinfo <- r[3]
  host <- r[4]
  port <- r[5] %||% ""
  dbname <- r[6]
  query <- r[7] %||% ""

  # user:pass (password peut contenir :)
  up <- strsplit(userinfo, ":", fixed = TRUE)[[1]]
  user <- utils::URLdecode(up[1])
  pass <- if (length(up) >= 2) utils::URLdecode(paste(up[-1], collapse = ":")) else ""

  qs <- list()
  if (nzchar(query)) {
    parts <- strsplit(query, "&", fixed = TRUE)[[1]]
    for (p in parts) {
      kv <- strsplit(p, "=", fixed = TRUE)[[1]]
      k <- utils::URLdecode(kv[1] %||% "")
      v <- utils::URLdecode(kv[2] %||% "")
      if (nzchar(k)) qs[[k]] <- v
    }
  }

  list(
    scheme = scheme,
    host = host,
    port = if (nzchar(port)) as.integer(port) else 5432L,
    dbname = dbname,
    user = user,
    password = pass,
    sslmode = qs$sslmode %||% qs$`sslmode` %||% ""
  )
}

# --- Déterminer la config DB (priorité à DATABASE_URL) ---
get_db_cfg <- function() {
  # 1) Railway / standard
  db_url <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(db_url)) {
    cfg <- parse_database_url(db_url)
    if (!is.null(cfg)) {
      return(list(
        host = cfg$host,
        port = cfg$port,
        dbname = cfg$dbname,
        user = cfg$user,
        password = cfg$password,
        sslmode = cfg$sslmode %||% Sys.getenv("PGSSLMODE", "require")
      ))
    }
  }

  # 2) Variables PG*
  list(
    host     = Sys.getenv("PGHOST", unset = ""),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE", unset = ""),
    user     = Sys.getenv("PGUSER", unset = Sys.getenv("PGREADUSER", "")),
    password = Sys.getenv("PGPASSWORD", unset = Sys.getenv("PGREADPASS", "")),
    sslmode  = Sys.getenv("PGSSLMODE", "require")
  )
}

pg_con <- function() {
  cfg <- get_db_cfg()

  # garde-fou pour éviter "localhost" en prod
  if (!nzchar(cfg$host)) stop("PGHOST/DATABASE_URL manquant (host vide).")
  if (tolower(cfg$host) %in% c("localhost", "127.0.0.1", "::1")) {
    stop("PGHOST pointe vers localhost. Sur Railway, utilise DATABASE_URL ou les variables PGHOST/PGPORT fournies par Postgres.")
  }

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

# ========= API KEY FILTER (désactivé si API_KEY vide) =========
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got <- req$HTTP_X_API_KEY %||% req$HTTP_X_APIKEY %||% req$HTTP_X_API_KEY %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    return(forward())
  }

  res$status <- 401L
  return(list(error = TRUE, message = "Unauthorized: invalid API key"))
}

# ========= CORS =========
#* @plumber
function(pr) {
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]] |> trimws()

  pr$registerHooks(list(
    preroute = function(req, res) {
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
      if (!is.null(req$REQUEST_METHOD) && req$REQUEST_METHOD == "OPTIONS") {
        res$status <- 204L
        return(list())
      }
      forward()
    }
  ))

  pr
}

# ========= HEALTH =========
#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

# ========= DEBUG ENV (sans mot de passe) =========
#* @get /debug/env
function() {
  cfg <- get_db_cfg()
  list(
    DATABASE_URL_set   = nzchar(Sys.getenv("DATABASE_URL", "")),
    PGHOST             = Sys.getenv("PGHOST"),
    PGPORT             = Sys.getenv("PGPORT"),
    PGDATABASE         = Sys.getenv("PGDATABASE"),
    PGUSER             = Sys.getenv("PGUSER"),
    PGREADUSER         = Sys.getenv("PGREADUSER"),
    PGPASSWORD_set     = nzchar(Sys.getenv("PGPASSWORD", "")),
    PGREADPASS_set     = nzchar(Sys.getenv("PGREADPASS", "")),
    PGSSLMODE          = Sys.getenv("PGSSLMODE"),
    CORS_ALLOW_ORIGIN  = Sys.getenv("CORS_ALLOW_ORIGIN"),
    effective_host     = cfg$host,
    effective_port     = cfg$port,
    effective_dbname   = cfg$dbname,
    effective_user     = cfg$user,
    effective_sslmode  = cfg$sslmode
  )
}

# ========= DEBUG PING DB (DOIT TOURNER) =========
#* @serializer unboxedJSON
#* @get /debug/pingdb
function() {
  cfg <- get_db_cfg()

  out <- tryCatch({
    con <- pg_con()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    x <- DBI::dbGetQuery(con, "
      SELECT
        current_database() AS db,
        current_user AS usr,
        current_schema() AS schema,
        inet_server_addr() AS server_addr,
        inet_server_port() AS server_port,
        version() AS version;
    ")

    # Evite le bug <simpleError: No method asJSON S3 class: pq_inet>
    x$server_addr <- as.character(x$server_addr)
    x$server_port <- as.integer(x$server_port)

    list(ok = TRUE, config = list(host = cfg$host, port = cfg$port, dbname = cfg$dbname, user = cfg$user), result = x)
  }, error = function(e) {
    list(
      ok = FALSE,
      config = list(host = cfg$host, port = cfg$port, dbname = cfg$dbname, user = cfg$user),
      error = conditionMessage(e)
    )
  })

  out
}

# ========= EXPORT CSV =========
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
               FROM "{schema}"."indicator_values"
               WHERE 1=1')

  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")

  DBI::dbGetQuery(con, qry)
}

# ========= LISTE INDICATEURS =========
#* @get /indicators
function(q = "") {
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('SELECT DISTINCT indicator_code, indicator_name
               FROM "{schema}"."indicator_values"
               WHERE indicator_code IS NOT NULL')

  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                             OR indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }

  qry <- paste0(qry, " ORDER BY indicator_code;")
  DBI::dbGetQuery(con, qry)
}

# ========= VALUES PAGINÉ =========
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  limit  <- max(1L, min(as.integer(limit), 10000L))
  offset <- max(0L, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::int AS n FROM "{schema}"."indicator_values" {where};'))$n[1]

  rows <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."indicator_values"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))

  list(total = total, limit = limit, offset = offset, rows = rows)
}

# ========= EXPORT XLSX =========
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
               FROM "{schema}"."indicator_values" WHERE 1=1')

  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")

  dat <- DBI::dbGetQuery(con, qry)
  tf <- tempfile(fileext = ".xlsx")

  if (!requireNamespace("writexl", quietly = TRUE)) install.packages("writexl", repos = "https://cloud.r-project.org")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ========= METRICS =========
#* @serializer html
#* @get /metrics
function() {
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."indicator_values";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."indicator_values";'))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", as.numeric(as.POSIXct(latest$max_ins[[1]])), "\n",
    "onu_api_last_updated_at ",  as.numeric(as.POSIXct(latest$max_upd[[1]])),  "\n"
  )
}