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

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || !nzchar(as.character(x))) y else x

# --- Charger .env si présent (NE BLOQUE PAS en prod Railway) ---
find_env <- function(){
  p1 <- normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE)          # si run depuis /api
  p2 <- normalizePath(file.path(getwd(), ".env"), mustWork = FALSE)               # si run depuis racine
  p3 <- normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)      # fallback
  for (p in c(p1, p2, p3)) if (file.exists(p)) return(p)
  NA_character_
}
.env.path <- find_env()
if (!is.na(.env.path)) {
  dotenv::load_dot_env(.env.path, override = FALSE)
  message("[plumber] .env chargé depuis: ", .env.path)
} else {
  message("[plumber] .env non trouvé (OK si variables Railway déjà définies).")
}

# --- Parser DATABASE_URL (Railway Postgres) ---
parse_database_url <- function(url){
  # Ex: postgres://user:pass@host:5432/dbname?sslmode=require
  url <- sub("^postgresql://", "postgres://", url)
  url <- sub("^postgres://", "", url)

  # Séparer query
  parts <- strsplit(url, "\\?", fixed = FALSE)[[1]]
  main <- parts[1]
  query <- if (length(parts) > 1) parts[2] else ""

  # userinfo@host:port/db
  up <- strsplit(main, "@", fixed = TRUE)[[1]]
  if (length(up) == 2) {
    userinfo <- up[1]
    hostpart <- up[2]
  } else {
    userinfo <- ""
    hostpart <- up[1]
  }

  user <- ""
  password <- ""
  if (nzchar(userinfo)) {
    ui <- strsplit(userinfo, ":", fixed = TRUE)[[1]]
    user <- utils::URLdecode(ui[1])
    password <- if (length(ui) > 1) utils::URLdecode(paste(ui[-1], collapse=":")) else ""
  }

  hp <- strsplit(hostpart, "/", fixed = TRUE)[[1]]
  hostport <- hp[1]
  dbname <- if (length(hp) > 1) utils::URLdecode(paste(hp[-1], collapse="/")) else ""

  host <- hostport
  port <- 5432L
  if (grepl(":", hostport, fixed = TRUE)) {
    hpp <- strsplit(hostport, ":", fixed = TRUE)[[1]]
    host <- hpp[1]
    port <- suppressWarnings(as.integer(hpp[2]))
    if (is.na(port)) port <- 5432L
  }

  sslmode <- NA_character_
  if (nzchar(query)) {
    kvs <- strsplit(query, "&", fixed = TRUE)[[1]]
    for (kv in kvs) {
      x <- strsplit(kv, "=", fixed = TRUE)[[1]]
      k <- tolower(x[1] %||% "")
      v <- utils::URLdecode(x[2] %||% "")
      if (k == "sslmode") sslmode <- v
    }
  }

  list(host=host, port=port, dbname=dbname, user=user, password=password, sslmode=sslmode)
}

# --- Connexion Postgres : préfère DATABASE_URL, sinon PGHOST/PGPORT/... ---
pg_con <- function() {
  db_url <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(db_url)) {
    cfg <- parse_database_url(db_url)
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = cfg$host,
      port     = cfg$port,
      dbname   = cfg$dbname,
      user     = cfg$user,
      password = cfg$password,
      sslmode  = (cfg$sslmode %||% Sys.getenv("PGSSLMODE", "require"))
    )
  } else {
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
}

# ---- API Key (désactivée si API_KEY vide) ----
#* @filter apikey
function(req, res){
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% req$HTTP_X_APIKEY %||% ""
  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    return(list(error=TRUE, message="Unauthorized: invalid API key"))
  }
}

# ---- CORS + gestion OPTIONS (évite erreurs Swagger/502) ----
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
      res$setHeader("Access-Control-Allow-Credentials", "true")

      # IMPORTANT: répondre aux preflight OPTIONS sans passer aux endpoints
      if (identical(toupper(req$REQUEST_METHOD %||% ""), "OPTIONS")) {
        res$status <- 204
        return(FALSE)  # stop routing
      }
      TRUE
    }
  ))
  pr
}

# ---- Health ----
#* @get /health
function(){ list(status="ok", time=as.character(Sys.time())) }

# ---- Debug env (sans secrets) ----
#* @get /debug/env
function(){
  list(
    ENV_PATH_USED = ifelse(is.na(.env.path), "", .env.path),
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", "")),
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGREADUSER = Sys.getenv("PGREADUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGREADPASS_set = nzchar(Sys.getenv("PGREADPASS")),
    PGSSLMODE = Sys.getenv("PGSSLMODE"),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN")
  )
}

# ---- Debug ping DB (doit renvoyer 200 + infos) ----
#* @serializer unboxedJSON
#* @get /debug/pingdb
function(res){
  out <- tryCatch({
    con <- pg_con()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    # Toujours caster en TEXT pour éviter les classes exotiques (inet, etc.)
    x <- DBI::dbGetQuery(con, "
      SELECT
        current_database()::text AS db,
        current_user::text      AS user,
        current_schema()::text  AS schema,
        inet_server_addr()::text AS server_addr,
        inet_server_port()::int  AS server_port,
        version()::text         AS version
    ")
    list(ok = TRUE, info = x[1, , drop = FALSE])
  }, error = function(e){
    res$status <- 500
    list(ok = FALSE, message = conditionMessage(e))
  })

  out
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