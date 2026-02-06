# api/plumber.R  ---- FINAL (robust JSON errors + robust DATABASE_URL parsing)

suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(glue)
  library(httr2)   # url_parse()
  library(dotenv)  # optional local .env
})

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || identical(x, "")) y else x

# ------------------------------------------------------------------------------
# 1) Load .env locally if present (safe in production if absent)
# ------------------------------------------------------------------------------
find_env <- function(){
  candidates <- c(
    file.path(getwd(), ".env"),
    file.path(getwd(), "..", ".env"),
    file.path(dirname(getwd()), ".env")
  )
  candidates <- vapply(candidates, normalizePath, character(1), mustWork = FALSE)
  for (p in candidates) if (file.exists(p)) return(p)
  NA_character_
}

.env.path <- find_env()
if (!is.na(.env.path)) {
  dotenv::load_dot_env(.env.path)
  message("[plumber] .env loaded from: ", .env.path)
} else {
  message("[plumber] No .env found (OK in production).")
}

# ------------------------------------------------------------------------------
# 2) Robust DATABASE_URL parsing (handles special chars in password)
# ------------------------------------------------------------------------------
parse_database_url <- function(url){
  if (!nzchar(url)) stop("DATABASE_URL is empty")

  u <- httr2::url_parse(url)

  host <- u$hostname
  port <- u$port
  if (is.na(port) || port == "") port <- 5432L else port <- as.integer(port)

  user <- u$username
  pass <- u$password

  # Decode URL-encoded user/pass safely (does NOT depend on httr2::url_decode)
  if (!is.na(user) && nzchar(user)) user <- utils::URLdecode(user)
  if (!is.na(pass) && nzchar(pass)) pass <- utils::URLdecode(pass)

  dbname <- u$path
  dbname <- sub("^/", "", dbname)

  list(host = host, port = port, user = user, password = pass, dbname = dbname)
}

get_db_con <- function(){
  db_url <- Sys.getenv("DATABASE_URL", "")

  if (nzchar(db_url)) {
    cfg <- parse_database_url(db_url)
    return(DBI::dbConnect(
      RPostgres::Postgres(),
      host     = cfg$host,
      port     = cfg$port,
      dbname   = cfg$dbname,
      user     = cfg$user,
      password = cfg$password,
      sslmode  = Sys.getenv("PGSSLMODE", "require")
    ))
  }

  # fallback (local)
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
    sslmode  = Sys.getenv("PGSSLMODE", "require")
  )
}

pg_schema <- function() Sys.getenv("PGSCHEMA", "public")

# ------------------------------------------------------------------------------
# 3) Global plumber config: CORS + JSON error handler (prevents "An exception occurred.")
# ------------------------------------------------------------------------------
#* @plumber
function(pr){

  # ---- Force JSON errors everywhere (Swagger will show the real message)
  pr$setErrorHandler(function(req, res, err){
    res$status <- 500
    res$setHeader("Content-Type", "application/json; charset=utf-8")
    list(ok = FALSE, error = as.character(err))
  })

  # ---- CORS (optional). Set CORS_ALLOW_ORIGIN="*" or "https://xxx,https://yyy"
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]] |> trimws()

  pr$registerHooks(list(
    preroute = function(req, res){
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    }
  ))

  pr
}

# ------------------------------------------------------------------------------
# 4) Optional API key filter (if API_KEY empty => no protection)
# ------------------------------------------------------------------------------
#* @filter apikey
function(req, res){
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""
  if (identical(allowed, "") || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    return(list(ok = FALSE, error = "Unauthorized: invalid API key"))
  }
}

# ------------------------------------------------------------------------------
# 5) Routes
# ------------------------------------------------------------------------------
#* @get /health
#* @serializer unboxedJSON
function(){
  list(ok = TRUE, status = "ok", time = as.character(Sys.time()))
}

#* @get /debug/env
#* @serializer unboxedJSON
function(){
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL")),
    PGHOST           = Sys.getenv("PGHOST"),
    PGPORT           = Sys.getenv("PGPORT"),
    PGDATABASE       = Sys.getenv("PGDATABASE"),
    PGUSER           = Sys.getenv("PGUSER"),
    PGPASSWORD_set   = nzchar(Sys.getenv("PGPASSWORD")),
    PGREADUSER       = Sys.getenv("PGREADUSER"),
    PGREADPASS_set   = nzchar(Sys.getenv("PGREADPASS")),
    PGSSLMODE        = Sys.getenv("PGSSLMODE", "require"),
    PGSCHEMA         = Sys.getenv("PGSCHEMA", "public"),
    env_file_loaded  = ifelse(is.na(.env.path), NA_character_, .env.path)
  )
}

#* @get /debug/pingdb
#* @serializer unboxedJSON
function(res){
  tryCatch({
    con <- get_db_con()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    info <- DBI::dbGetQuery(con, "
      SELECT current_database() AS db,
             current_user AS user,
             current_schema() AS schema,
             inet_server_addr() AS server_addr,
             inet_server_port() AS server_port,
             version();
    ")

    list(ok = TRUE, info = info)
  }, error = function(e){
    res$status <- 500
    list(ok = FALSE, error = conditionMessage(e))
  })
}

# ------------------------------------------------------------------------------
# 6) Data endpoints (adapt if your table name differs)
# Assumes table: indicator_values in schema PGSCHEMA (default public)
# Columns used: indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
# ------------------------------------------------------------------------------

#* @get /indicators
#* @serializer unboxedJSON
function(q = ""){
  con <- get_db_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- pg_schema()

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."indicator_values"
    WHERE indicator_code IS NOT NULL
  ')

  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    qry <- paste0(qry, glue('
      AND (
        indicator_code ILIKE {DBI::dbQuoteString(con, like)}
        OR indicator_name ILIKE {DBI::dbQuoteString(con, like)}
      )
    '))
  }

  qry <- paste0(qry, " ORDER BY indicator_code;")
  DBI::dbGetQuery(con, qry)
}

#* @get /values
#* @serializer unboxedJSON
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0){
  con <- get_db_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- pg_schema()

  limit  <- max(1L, min(as.integer(limit), 10000L))
  offset <- max(0L, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code))
    where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))
    where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))

  st <- suppressWarnings(as.numeric(start))
  en <- suppressWarnings(as.numeric(end))
  if (!is.na(st)) where <- paste0(where, glue(" AND period >= {st}"))
  if (!is.na(en)) where <- paste0(where, glue(" AND period <= {en}"))

  total <- DBI::dbGetQuery(con, glue('
    SELECT COUNT(*)::int AS n
    FROM "{schema}"."indicator_values"
    {where};
  '))$n[1]

  rows <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."indicator_values"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))

  list(ok = TRUE, total = total, limit = limit, offset = offset, rows = rows)
}

#* @get /export/csv
#* @serializer csv
function(indicator_code = "", ref_area = "", start = NA, end = NA){
  con <- get_db_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- pg_schema()

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."indicator_values"
    WHERE 1=1
  ')

  if (nzchar(indicator_code))
    qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))
    qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))

  st <- suppressWarnings(as.numeric(start))
  en <- suppressWarnings(as.numeric(end))
  if (!is.na(st)) qry <- paste0(qry, glue(" AND period >= {st}"))
  if (!is.na(en)) qry <- paste0(qry, glue(" AND period <= {en}"))

  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  DBI::dbGetQuery(con, qry)
}

#* @get /metrics
#* @serializer html
function(){
  con <- get_db_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- pg_schema()

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."indicator_values";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('
    SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd
    FROM "{schema}"."indicator_values";
  '))

  max_ins <- latest$max_ins[[1]]
  max_upd <- latest$max_upd[[1]]

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (!is.null(max_ins) && !is.na(max_ins)) as.numeric(as.POSIXct(max_ins)) else "NaN", "\n",
    "onu_api_last_updated_at ",  if (!is.null(max_upd) && !is.na(max_upd)) as.numeric(as.POSIXct(max_upd)) else "NaN", "\n"
  )
} 