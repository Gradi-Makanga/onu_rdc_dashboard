# plumber.R  --- ONU RDC Dashboard API (Railway-ready)
# Objectif: /debug/pingdb doit tourner + endpoints /indicators /values /export/*
# - En prod Railway: DATABASE_URL (Railway) est prioritaire
# - En local: on charge .env si DATABASE_URL absent

suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
})

# ---------------------------
# Utils
# ---------------------------
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(x)) y else x

trim1 <- function(x) trimws(as.character(x %||% ""))
nz1   <- function(x) nzchar(trim1(x))

# Parse postgres DATABASE_URL without extra packages
# Supports: postgres://user:pass@host:port/dbname?sslmode=require
parse_database_url <- function(url){
  url <- trim1(url)
  if (!nzchar(url)) return(NULL)

  # remove scheme
  url2 <- sub("^postgres(ql)?://", "", url, ignore.case = TRUE)

  # split query
  parts <- strsplit(url2, "\\?", fixed = FALSE)[[1]]
  main  <- parts[1]
  query <- if (length(parts) >= 2) parts[2] else ""

  # user:pass@host:port/db
  # userinfo (optional)
  userinfo <- ""
  hostpart <- main

  if (grepl("@", main, fixed = TRUE)) {
    tmp <- strsplit(main, "@", fixed = TRUE)[[1]]
    userinfo <- tmp[1]
    hostpart <- tmp[2]
  }

  user <- ""
  pass <- ""
  if (nzchar(userinfo)) {
    up <- strsplit(userinfo, ":", fixed = TRUE)[[1]]
    user <- utils::URLdecode(up[1])
    pass <- if (length(up) >= 2) utils::URLdecode(paste(up[-1], collapse=":")) else ""
  }

  # host:port/dbname
  hp_db <- strsplit(hostpart, "/", fixed = TRUE)[[1]]
  hp    <- hp_db[1]
  db    <- if (length(hp_db) >= 2) hp_db[2] else ""

  host <- hp
  port <- NA_integer_
  if (grepl(":", hp, fixed = TRUE)) {
    hp2  <- strsplit(hp, ":", fixed = TRUE)[[1]]
    host <- hp2[1]
    port <- suppressWarnings(as.integer(hp2[2]))
  }

  # query params (only sslmode needed)
  sslmode <- ""
  if (nzchar(query)) {
    kvs <- strsplit(query, "&", fixed = TRUE)[[1]]
    for (kv in kvs) {
      kv2 <- strsplit(kv, "=", fixed = TRUE)[[1]]
      k <- trim1(kv2[1])
      v <- if (length(kv2) >= 2) utils::URLdecode(kv2[2]) else ""
      if (tolower(k) == "sslmode") sslmode <- v
    }
  }

  list(
    host = host,
    port = if (!is.na(port)) port else 5432L,
    dbname = utils::URLdecode(db),
    user = user,
    password = pass,
    sslmode = sslmode
  )
}

# Find .env (local dev)
find_env <- function(){
  candidates <- c(
    file.path(getwd(), ".env"),
    file.path(getwd(), "..", ".env"),
    file.path(dirname(getwd()), ".env"),
    "/app/.env",
    "/app/api/.env"
  )
  for (p in candidates) {
    p2 <- normalizePath(p, mustWork = FALSE)
    if (file.exists(p2)) return(p2)
  }
  NA_character_
}

# Decide config source
use_railway <- nz1(Sys.getenv("DATABASE_URL", ""))

if (!use_railway) {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    dotenv::load_dot_env(.env.path)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] Pas de DATABASE_URL et .env introuvable => variables système uniquement.")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

pg_schema <- function() trim1(Sys.getenv("PGSCHEMA", "public"))
pg_table  <- function() trim1(Sys.getenv("PGTABLE", ""))  # peut être vide en prod

# Build connection params (DATABASE_URL > PG* vars)
effective_db_params <- function(){
  dburl <- Sys.getenv("DATABASE_URL", "")
  if (nz1(dburl)) {
    p <- parse_database_url(dburl)
    if (!is.null(p)) {
      if (!nzchar(p$sslmode)) p$sslmode <- Sys.getenv("PGSSLMODE", "require")
      return(p)
    }
  }
  list(
    host = Sys.getenv("PGHOST", "localhost"),
    port = as.integer(Sys.getenv("PGPORT","5432")),
    dbname = Sys.getenv("PGDATABASE", ""),
    user = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER","")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS","")),
    sslmode = Sys.getenv("PGSSLMODE","prefer")
  )
}

pg_con <- function(){
  p <- effective_db_params()
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = p$host,
    port     = as.integer(p$port),
    dbname   = p$dbname,
    user     = p$user,
    password = p$password,
    sslmode  = p$sslmode
  )
}

table_exists <- function(con, schema, table){
  if (!nzchar(table)) return(FALSE)
  q <- "
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1;
  "
  res <- DBI::dbGetQuery(con, q, params = list(schema, table))
  nrow(res) > 0
}

detect_table <- function(con, schema){
  # If PGTABLE provided and exists -> return it
  t0 <- pg_table()
  if (nzchar(t0) && table_exists(con, schema, t0)) return(t0)

  # Otherwise, search for indicator_values* in schema
  q <- "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_type = 'BASE TABLE'
      AND table_name ILIKE 'indicator_values%'
    ORDER BY
      CASE WHEN table_name = 'indicator_values' THEN 0 ELSE 1 END,
      table_name
    LIMIT 1;
  "
  r <- DBI::dbGetQuery(con, q, params = list(schema))
  if (nrow(r) == 0) return(NA_character_)
  r$table_name[[1]]
}

json_safe_df <- function(df){
  # Convert problematic types (inet, etc.) to character safely
  for (nm in names(df)) {
    x <- df[[nm]]
    cls <- class(x)
    if (inherits(x, "pq_inet") || inherits(x, "pq_regproc") || inherits(x, "pq_name")) {
      df[[nm]] <- as.character(x)
    }
    # POSIXct ok; leave as is (plumber/jsonlite handles)
  }
  df
}

# ---------------------------
# API Key filter (disabled if API_KEY empty)
# ---------------------------
#* @filter apikey
function(req, res){
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% req$HTTP_X_APIKEY %||% ""
  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = TRUE, message = "Unauthorized: invalid API key")
  }
}

# ---------------------------
# CORS + OPTIONS
# ---------------------------
#* @plumber
function(pr){
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]]
  origins <- trimws(origins)

  pr$registerHooks(list(
    preroute = function(req, res){
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "*"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
      res$setHeader("Access-Control-Allow-Credentials", "true")
      NULL
    }
  ))
  pr
}

#* @options /<path:.*>
function(res){
  res$status <- 204
  ""
}

# ---------------------------
# Endpoints
# ---------------------------

#* @get /health
function(){
  list(status="ok", time=as.character(Sys.time()))
}

#* @get /debug/env
function(){
  eff <- effective_db_params()
  list(
    DATABASE_URL_set  = nz1(Sys.getenv("DATABASE_URL","")),
    PGHOST            = Sys.getenv("PGHOST",""),
    PGPORT            = Sys.getenv("PGPORT",""),
    PGDATABASE        = Sys.getenv("PGDATABASE",""),
    PGUSER            = Sys.getenv("PGUSER",""),
    PGPASSWORD_set    = nzchar(Sys.getenv("PGPASSWORD","")),
    PGSSLMODE         = Sys.getenv("PGSSLMODE",""),
    PGSCHEMA          = pg_schema(),
    PGTABLE           = pg_table(),
    EFFECTIVE = list(
      host    = eff$host,
      port    = eff$port,
      dbname  = eff$dbname,
      user    = eff$user,
      sslmode = eff$sslmode
    ),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN","")
  )
}

#* @get /debug/pingdb
function(){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  # Ping query (cast inet to text to avoid pq_inet json error)
  q <- "
    SELECT
      current_database() AS db,
      current_user      AS usr,
      current_schema()  AS schema,
      inet_server_addr()::text AS server_addr,
      inet_server_port() AS server_port,
      version()         AS version
  "
  out <- DBI::dbGetQuery(con, q)
  out <- json_safe_df(out)

  if (is.na(table)) {
    return(list(
      ok = TRUE,
      detected = list(schema = schema, table = NULL),
      result = out,
      error = TRUE,
      message = glue("Aucune table trouvée dans {schema} (attendu: indicator_values%). Charge d'abord les données dans Postgres Railway.")
    ))
  }

  list(
    ok = TRUE,
    detected = list(schema = schema, table = table),
    result = out
  )
}

#* @serializer csv
#* @get /export/csv
function(indicator_code="", ref_area="", start=NA, end=NA){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}: indicator_values%"))

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
function(q = ""){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}: indicator_values%"))

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."{table}"
    WHERE indicator_code IS NOT NULL
  ')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%","",q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                         OR  indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, ' ORDER BY indicator_code;')

  DBI::dbGetQuery(con, qry)
}

#* @serializer unboxedJSON
#* @get /values
function(indicator_code="", ref_area="", start=NA, end=NA, limit=1000, offset=0){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}: indicator_values%"))

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

#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code="", ref_area="", start=NA, end=NA){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}: indicator_values%"))

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM "{schema}"."{table}"
    WHERE 1=1
  ')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))

  qry <- paste0(qry, ' ORDER BY indicator_code, ref_area, period;')
  dat <- DBI::dbGetQuery(con, qry)

  tf <- tempfile(fileext = ".xlsx")
  if (!requireNamespace("writexl", quietly = TRUE)) {
    install.packages("writexl", repos = "https://cloud.r-project.org")
  }
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

#* @serializer html
#* @get /metrics
function(){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (is.na(table)) return("onu_api_rows_total 0\n")

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  max_ins <- latest$max_ins[[1]]
  max_upd <- latest$max_upd[[1]]

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (is.na(max_ins)) "NaN" else as.numeric(as.POSIXct(max_ins)), "\n",
    "onu_api_last_updated_at ",  if (is.na(max_upd)) "NaN" else as.numeric(as.POSIXct(max_upd)), "\n"
  )
}