# api/plumber.R  (Railway-ready)
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(readr)
  library(dplyr)
  library(glue)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(as.character(x))) y else x

# ----------------------------
# 1) Charger .env uniquement en local (si DATABASE_URL absent)
# ----------------------------
find_env <- function(){
  cands <- c(
    normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE),
    normalizePath(file.path(getwd(), ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE),
    normalizePath("/app/.env", mustWork = FALSE),
    normalizePath("/app/api/.env", mustWork = FALSE)
  )
  for (p in cands) if (file.exists(p)) return(p)
  NA_character_
}

if (!nzchar(Sys.getenv("DATABASE_URL", ""))) {
  if (requireNamespace("dotenv", quietly = TRUE)) {
    .env.path <- find_env()
    if (!is.na(.env.path)) {
      dotenv::load_dot_env(.env.path)   # pas de override= (compatibilité)
      message("[plumber] .env chargé depuis: ", .env.path)
    } else {
      message("[plumber] Pas de DATABASE_URL et .env introuvable (OK si vars déjà définies).")
    }
  } else {
    message("[plumber] dotenv non installé (OK si vars déjà définies).")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

# ----------------------------
# 2) Parse DATABASE_URL (postgres://user:pass@host:port/db?sslmode=require)
# ----------------------------
parse_database_url <- function(url){
  url <- trimws(url)
  if (!nzchar(url)) return(NULL)

  url <- sub("^jdbc:", "", url)
  url <- sub("^postgresql://", "postgres://", url, ignore.case = TRUE)
  url <- sub("^postgres://", "", url, ignore.case = TRUE)

  # querystring
  qs <- ""
  if (grepl("\\?", url)) {
    parts <- strsplit(url, "\\?", perl = TRUE)[[1]]
    url <- parts[1]
    qs  <- parts[2] %||% ""
  }

  # userinfo@host/db
  userinfo <- ""
  hostpart <- url
  if (grepl("@", url, fixed = TRUE)) {
    tmp <- strsplit(url, "@", fixed = TRUE)[[1]]
    userinfo <- tmp[1]
    hostpart <- tmp[2]
  }

  user <- ""
  pass <- ""
  if (nzchar(userinfo)) {
    up <- strsplit(userinfo, ":", fixed = TRUE)[[1]]
    user <- utils::URLdecode(up[1] %||% "")
    pass <- utils::URLdecode(up[2] %||% "")
  }

  hp_db <- strsplit(hostpart, "/", fixed = TRUE)[[1]]
  hp    <- hp_db[1] %||% ""
  db    <- utils::URLdecode(hp_db[2] %||% "")

  host <- hp
  port <- 5432L
  if (grepl(":", hp, fixed = TRUE)) {
    hp2  <- strsplit(hp, ":", fixed = TRUE)[[1]]
    host <- hp2[1] %||% ""
    port <- suppressWarnings(as.integer(hp2[2] %||% "5432"))
    if (is.na(port)) port <- 5432L
  }

  sslmode <- ""
  if (nzchar(qs)) {
    kvs <- strsplit(qs, "&", fixed = TRUE)[[1]]
    for (kv in kvs) {
      p <- strsplit(kv, "=", fixed = TRUE)[[1]]
      k <- tolower(p[1] %||% "")
      v <- utils::URLdecode(p[2] %||% "")
      if (k == "sslmode") sslmode <- v
    }
  }

  list(host=host, port=port, dbname=db, user=user, password=pass, sslmode=sslmode)
}

# ----------------------------
# 3) Connexion PG : Railway (DATABASE_URL) > variables PG*
# ----------------------------
pg_schema <- function() Sys.getenv("PGSCHEMA", "public")
pg_table  <- function() Sys.getenv("PGTABLE", "")  # peut être vide en prod

effective_db_cfg <- function(){
  dburl <- Sys.getenv("DATABASE_URL", "")
  if (nzchar(dburl)) {
    cfg <- parse_database_url(dburl)
    return(list(
      host     = cfg$host,
      port     = cfg$port,
      dbname   = cfg$dbname,
      user     = cfg$user,
      password = cfg$password,
      sslmode  = (cfg$sslmode %||% Sys.getenv("PGSSLMODE", "require")) %||% "require"
    ))
  }

  list(
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT","5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER","")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS","")),
    sslmode  = Sys.getenv("PGSSLMODE","prefer")
  )
}

pg_con <- function() {
  cfg <- effective_db_cfg()
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = cfg$host,
    port     = as.integer(cfg$port),
    dbname   = cfg$dbname,
    user     = cfg$user,
    password = cfg$password,
    sslmode  = cfg$sslmode
  )
}

# ----------------------------
# 4) Détection table (si indicator_values n'existe pas en prod Railway)
# ----------------------------
table_exists <- function(con, schema, table){
  if (!nzchar(table)) return(FALSE)
  ok <- DBI::dbGetQuery(con, glue(
    "SELECT EXISTS(
       SELECT 1 FROM information_schema.tables
       WHERE table_schema = {DBI::dbQuoteString(con, schema)}
         AND table_name   = {DBI::dbQuoteString(con, table)}
     ) AS ok"
  ))
  isTRUE(ok$ok[1])
}

detect_table <- function(con, schema){
  # 1) PGTABLE si défini
  t0 <- pg_table()
  if (nzchar(t0) && table_exists(con, schema, t0)) return(t0)

  # 2) indicator_values si existe
  if (table_exists(con, schema, "indicator_values")) return("indicator_values")

  # 3) sinon, première table qui commence par indicator_values%
  hit <- DBI::dbGetQuery(con, glue(
    "SELECT table_name
     FROM information_schema.tables
     WHERE table_schema = {DBI::dbQuoteString(con, schema)}
       AND table_type='BASE TABLE'
       AND table_name ILIKE 'indicator_values%'
     ORDER BY
       CASE WHEN table_name='indicator_values' THEN 0 ELSE 1 END,
       table_name
     LIMIT 1"
  ))
  if (nrow(hit) > 0) return(hit$table_name[1])
  ""  # rien trouvé
}

# ----------------------------
# 5) JSON safe (pq_inet etc.)
# ----------------------------
json_safe_df <- function(df){
  if (!is.data.frame(df) || nrow(df) == 0) return(df)
  for (nm in names(df)) {
    if (inherits(df[[nm]], "pq_inet") || inherits(df[[nm]], "pq_bytea") ||
        inherits(df[[nm]], "POSIXct")  || inherits(df[[nm]], "POSIXt")) {
      df[[nm]] <- as.character(df[[nm]])
    }
  }
  df
}

# ----------------------------
# 6) API Key (désactivée si API_KEY vide)
# ----------------------------
#* @filter apikey
function(req, res){
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""
  if (identical(allowed, "") || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    return(list(error="Unauthorized: invalid API key"))
  }
}

# ----------------------------
# 7) CORS + OPTIONS
# ----------------------------
#* @plumber
function(pr){
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]] |> trimws()
  pr$registerHooks(list(
    preroute = function(req, res){
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
      if (identical(req$REQUEST_METHOD, "OPTIONS")) {
        res$status <- 204
        return("")
      }
      NULL
    }
  ))
  pr
}

# ----------------------------
# 8) Health
# ----------------------------
#* @get /health
function(){ list(status="ok", time=as.character(Sys.time())) }

# ----------------------------
# 9) Debug env (montre EFFECTIVE en prod)
# ----------------------------
#* @serializer unboxedJSON
#* @get /debug/env
function(){
  eff <- effective_db_cfg()
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL","")),
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGSSLMODE = Sys.getenv("PGSSLMODE"),
    PGSCHEMA = pg_schema(),
    PGTABLE = pg_table(),
    EFFECTIVE = list(host=eff$host, port=eff$port, dbname=eff$dbname, user=eff$user, sslmode=eff$sslmode),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN")
  )
}

# ----------------------------
# 10) Debug ping DB (cast inet -> text)
# ----------------------------
#* @serializer unboxedJSON
#* @get /debug/pingdb
function(){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  table  <- detect_table(con, schema)

  info <- DBI::dbGetQuery(con, "
    SELECT current_database() AS db,
           current_user AS usr,
           current_schema() AS schema,
           inet_server_addr()::text AS server_addr,
           inet_server_port() AS server_port,
           version() AS version;
  ")
  info <- json_safe_df(info)

  if (!nzchar(table)) {
    return(list(ok=TRUE, detected=list(schema=schema, table=NULL), result=info,
                error=TRUE,
                message="Aucune table trouvée (indicator_values%). Charge les données dans la base Railway ou définis PGTABLE."))
  }

  list(ok=TRUE, detected=list(schema=schema, table=table), result=info)
}

# ----------------------------
# 11) Export CSV (table dynamique)
# ----------------------------
#* @serializer csv
#* @get /export/csv
function(indicator_code="", ref_area="", start=NA, end=NA){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable (indicator_values%).")

  qry <- glue('SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
               FROM "{schema}"."{table}"
               WHERE 1=1')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  DBI::dbGetQuery(con, qry)
}

# ----------------------------
# 12) Liste indicateurs (table dynamique)
# ----------------------------
#* @serializer unboxedJSON
#* @get /indicators
function(q = ""){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable (indicator_values%).")

  qry <- glue('SELECT DISTINCT indicator_code, indicator_name
               FROM "{schema}"."{table}"
               WHERE indicator_code IS NOT NULL')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%","",q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                        OR    indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, " ORDER BY indicator_code;")
  json_safe_df(DBI::dbGetQuery(con, qry))
}

# ----------------------------
# 13) Values paginé (table dynamique)
# ----------------------------
#* @serializer unboxedJSON
#* @get /values
function(indicator_code="", ref_area="", start=NA, end=NA, limit=1000, offset=0){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable (indicator_values%).")

  limit  <- max(1, min(as.integer(limit), 10000))
  offset <- max(0, as.integer(offset))

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
  rows <- json_safe_df(rows)
  list(total = total, limit = limit, offset = offset, rows = rows)
}

# ----------------------------
# 14) Export XLSX (table dynamique)
# ----------------------------
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code="", ref_area="", start=NA, end=NA){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable (indicator_values%).")

  qry <- glue('SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
               FROM "{schema}"."{table}" WHERE 1=1')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  dat <- DBI::dbGetQuery(con, qry)

  tf <- tempfile(fileext = ".xlsx")
  if (!requireNamespace("writexl", quietly = TRUE)) install.packages("writexl", repos="https://cloud.r-project.org")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ----------------------------
# 15) Metrics (table dynamique)
# ----------------------------
#* @serializer html
#* @get /metrics
function(){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable (indicator_values%).")

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", as.numeric(as.POSIXct(latest$max_ins[[1]])), "\n",
    "onu_api_last_updated_at ",  as.numeric(as.POSIXct(latest$max_upd[[1]])),  "\n"
  )
}