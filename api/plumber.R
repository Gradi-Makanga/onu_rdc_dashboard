# api/plumber.R
library(plumber)
library(DBI); library(RPostgres)
library(readr); library(dplyr); library(glue); library(dotenv)
library(httr2)

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || identical(x, "")) y else x

# --- Localisation dynamique du .env (LOCAL uniquement) ---
find_env <- function(){
  p1 <- normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE)
  p2 <- normalizePath(file.path(getwd(), ".env"), mustWork = FALSE)
  p3 <- normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  for (p in c(p1,p2,p3)) if (file.exists(p)) return(p)
  NA_character_
}

.env.path <- find_env()
if (!is.na(.env.path)) {
  dotenv::load_dot_env(.env.path)
  message("[plumber] .env chargé depuis: ", .env.path)
} else {
  message("[plumber] Aucun .env trouvé (OK en production).")
}

# --- Parse robuste de DATABASE_URL (Railway) ---
parse_database_url <- function(url){
  if (!nzchar(url)) stop("DATABASE_URL is empty")

  u <- httr2::url_parse(url)

  host <- u$hostname
  port <- u$port
  if (is.na(port) || port == "") port <- 5432L else port <- as.integer(port)

  user <- u$username
  pass <- u$password
  if (!is.na(user) && nzchar(user)) user <- utils::URLdecode(user)
  if (!is.na(pass) && nzchar(pass)) pass <- utils::URLdecode(pass)

  dbname <- u$path
  dbname <- sub("^/", "", dbname)

  list(host=host, port=port, user=user, password=pass, dbname=dbname)
}

# --- Connexion DB : Railway-first (DATABASE_URL), sinon variables PG* ---
pg_con <- function() {
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

  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT","5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER","")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS","")),
    sslmode  = Sys.getenv("PGSSLMODE","require")
  )
}

# ---- API Key (désactivée si API_KEY vide) ----
#* @filter apikey
function(req, res){
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""
  if (identical(allowed, "") || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    return(list(ok=FALSE, error="Unauthorized: invalid API key"))
  }
}

# ---- Plumber global: CORS + JSON error handler ----
#* @plumber
function(pr){

  # Force JSON errors (plus jamais "An exception occurred.")
  pr$setErrorHandler(function(req, res, err){
    res$status <- 500
    res$setHeader("Content-Type", "application/json; charset=utf-8")
    list(ok = FALSE, error = as.character(err))
  })

  # CORS
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

# ---- Health ----
#* @get /health
#* @serializer unboxedJSON
function(){ list(ok=TRUE, status="ok", time=as.character(Sys.time())) }

# ---- Debug env ----
#* @get /debug/env
#* @serializer unboxedJSON
function(){
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL")),
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGREADUSER = Sys.getenv("PGREADUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGREADPASS_set = nzchar(Sys.getenv("PGREADPASS")),
    PGSSLMODE = Sys.getenv("PGSSLMODE"),
    PGSCHEMA = Sys.getenv("PGSCHEMA","public"),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN"),
    env_file_loaded = ifelse(is.na(.env.path), NA_character_, .env.path)
  )
}

# ---- Debug ping DB (incassable + message clair) ----
#* @get /debug/pingdb
#* @serializer unboxedJSON
function(res){
  tryCatch({
    con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
    info <- DBI::dbGetQuery(con, '
      SELECT current_database() AS db,
             current_user AS user,
             current_schema() AS schema,
             inet_server_addr() AS server_addr,
             inet_server_port() AS server_port,
             version();
    ')
    list(ok=TRUE, info=info)
  }, error = function(e){
    res$status <- 500
    list(ok=FALSE, error=conditionMessage(e))
  })
}

# ---- Export CSV ----
#* @serializer csv
#* @get /export/csv
function(indicator_code="", ref_area="", start=NA, end=NA){
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA","public")
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

# ---- Liste indicateurs ----
#* @get /indicators
function(q = ""){
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA","public")
  qry <- glue('SELECT DISTINCT indicator_code, indicator_name
               FROM "{schema}"."indicator_values"
               WHERE indicator_code IS NOT NULL')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%","",q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                        OR    indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, " ORDER BY indicator_code;")
  DBI::dbGetQuery(con, qry)
}

# ---- JSON paginé ----
#* @serializer unboxedJSON
#* @get /values
function(indicator_code="", ref_area="", start=NA, end=NA, limit=1000, offset=0){
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA","public")
  limit  <- max(1, min(as.integer(limit), 10000))
  offset <- max(0, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::int AS n FROM "{schema}"."indicator_values" {where};'))$n[1]
  rows  <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."indicator_values"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))
  list(total = total, limit = limit, offset = offset, rows = rows)
}

# ---- Export XLSX ----
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code="", ref_area="", start=NA, end=NA){
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA","public")
  qry <- glue('SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
               FROM "{schema}"."indicator_values" WHERE 1=1')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  dat <- DBI::dbGetQuery(con, qry)
  tf <- tempfile(fileext = ".xlsx")
  if (!requireNamespace("writexl", quietly = TRUE)) install.packages("writexl")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ---- Metrics (texte) ----
#* @serializer html
#* @get /metrics
function(){
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA","public")
  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."indicator_values";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."indicator_values";'))
  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", as.numeric(as.POSIXct(latest$max_ins[[1]])), "\n",
    "onu_api_last_updated_at ",  as.numeric(as.POSIXct(latest$max_upd[[1]])),  "\n"
  )
}