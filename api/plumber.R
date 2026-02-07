# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dplyr)
  library(readr)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(as.character(x))) y else x

# -----------------------------
# 1) Charger .env seulement si DATABASE_URL absent (Railway)
# -----------------------------
find_env <- function(){
  cands <- c(
    normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE),
    normalizePath(file.path(getwd(), ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  )
  for (p in cands) if (file.exists(p)) return(p)
  NA_character_
}

if (!nzchar(Sys.getenv("DATABASE_URL", unset = ""))) {
  if (requireNamespace("dotenv", quietly = TRUE)) {
    .env.path <- find_env()
    if (!is.na(.env.path)) {
      dotenv::load_dot_env(.env.path)  # pas d'argument override (compatibilité)
      message("[plumber] .env chargé depuis: ", .env.path)
    } else {
      message("[plumber] DATABASE_URL absent et .env introuvable (ok si vars déjà définies ailleurs).")
    }
  } else {
    message("[plumber] Package dotenv non installé (ok si vars déjà définies ailleurs).")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

# -----------------------------
# 2) Parsing DATABASE_URL (postgres://user:pass@host:port/db?sslmode=require...)
# -----------------------------
parse_database_url <- function(u){
  if (!nzchar(u)) return(NULL)

  # enlever jdbc: si jamais
  u <- sub("^jdbc:", "", u)

  proto <- sub("^(.*?://).*", "\\1", u)
  rest  <- sub("^.*?://", "", u)

  auth_host <- strsplit(rest, "@", fixed = TRUE)[[1]]
  if (length(auth_host) == 2) {
    auth <- auth_host[1]
    hostpath <- auth_host[2]
    up <- strsplit(auth, ":", fixed = TRUE)[[1]]
    user <- up[1]
    pass <- if (length(up) >= 2) paste(up[-1], collapse=":") else ""
  } else {
    user <- ""
    pass <- ""
    hostpath <- auth_host[1]
  }

  hp_q <- strsplit(hostpath, "?", fixed = TRUE)[[1]]
  hp   <- hp_q[1]
  qstr <- if (length(hp_q) >= 2) paste(hp_q[-1], collapse="?") else ""

  h_p_db <- strsplit(hp, "/", fixed = TRUE)[[1]]
  hostport <- h_p_db[1]
  dbname <- if (length(h_p_db) >= 2) paste(h_p_db[-1], collapse="/") else ""

  hp2 <- strsplit(hostport, ":", fixed = TRUE)[[1]]
  host <- hp2[1]
  port <- if (length(hp2) >= 2) suppressWarnings(as.integer(hp2[2])) else NA_integer_

  # query params
  params <- list()
  if (nzchar(qstr)) {
    pairs <- strsplit(qstr, "&", fixed = TRUE)[[1]]
    for (kv in pairs) {
      kv2 <- strsplit(kv, "=", fixed = TRUE)[[1]]
      k <- utils::URLdecode(kv2[1])
      v <- if (length(kv2) >= 2) utils::URLdecode(paste(kv2[-1], collapse="=")) else ""
      params[[k]] <- v
    }
  }

  list(
    host = host,
    port = port,
    dbname = dbname,
    user = user,
    password = pass,
    sslmode = params[["sslmode"]] %||% ""
  )
}

pg_effective <- function(){
  dbu <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(dbu)) {
    p <- parse_database_url(dbu)
    # fallback sslmode
    ssl <- p$sslmode %||% Sys.getenv("PGSSLMODE", unset = "require")
    return(list(
      host = p$host,
      port = p$port %||% as.integer(Sys.getenv("PGPORT", "5432")),
      dbname = p$dbname,
      user = p$user,
      password = p$password,
      sslmode = ssl %||% "require"
    ))
  }

  list(
    host = Sys.getenv("PGHOST", "localhost"),
    port = as.integer(Sys.getenv("PGPORT","5432")),
    dbname = Sys.getenv("PGDATABASE",""),
    user = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER","")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS","")),
    sslmode = Sys.getenv("PGSSLMODE","prefer")
  )
}

pg_schema <- function(){
  Sys.getenv("PGSCHEMA", "public")
}

# détecter une table "indicator_values%" si PGTABLE vide ou incorrect
detect_table <- function(con, schema){
  want <- Sys.getenv("PGTABLE", unset = "")
  if (nzchar(want)) {
    ok <- DBI::dbGetQuery(con, glue(
      "SELECT 1 AS ok
       FROM information_schema.tables
       WHERE table_schema = {DBI::dbQuoteString(con, schema)}
         AND table_name   = {DBI::dbQuoteString(con, want)}
       LIMIT 1;"
    ))
    if (nrow(ok) == 1) return(want)
  }

  # sinon on cherche un candidat
  cand <- DBI::dbGetQuery(con, glue(
    "SELECT table_name
     FROM information_schema.tables
     WHERE table_schema = {DBI::dbQuoteString(con, schema)}
       AND table_name ILIKE 'indicator_values%'
     ORDER BY
       CASE
         WHEN table_name = 'indicator_values' THEN 0
         WHEN table_name = 'indicator_values_all' THEN 1
         WHEN table_name = 'indicator_values_staging' THEN 2
         ELSE 9
       END,
       table_name
     LIMIT 1;"
  ))
  if (nrow(cand) == 1) return(cand$table_name[[1]])
  NULL
}

pg_con <- function(){
  cfg <- pg_effective()
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = cfg$host,
    port     = cfg$port,
    dbname   = cfg$dbname,
    user     = cfg$user,
    password = cfg$password,
    sslmode  = cfg$sslmode
  )
}

# rendre les data.frames JSON-safe (pq_inet, POSIXct, list-cols…)
json_safe_df <- function(df){
  if (!is.data.frame(df) || nrow(df) == 0) return(df)
  for (nm in names(df)) {
    x <- df[[nm]]
    if (inherits(x, "pq_inet") || inherits(x, "pq_bytea") || inherits(x, "difftime")) {
      df[[nm]] <- as.character(x)
    } else if (inherits(x, "POSIXct") || inherits(x, "POSIXt") || inherits(x, "Date")) {
      df[[nm]] <- as.character(x)
    } else if (is.list(x)) {
      df[[nm]] <- vapply(x, function(z) paste0(z, collapse=","), character(1))
    }
  }
  df
}

# -----------------------------
# 3) API Key (désactivée si API_KEY vide)
# -----------------------------
#* @filter apikey
function(req, res){
  allowed <- Sys.getenv("API_KEY", unset = "")
  got <- req$HTTP_X_API_KEY %||% req$HTTP_X_APIKEY %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    if ("setStatus" %in% names(res)) res$setStatus(401) else res$status <- 401
    return(list(error = TRUE, message = "Unauthorized: invalid API key"))
  }
}

# -----------------------------
# 4) CORS + OPTIONS
# -----------------------------
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
      res$setHeader("Access-Control-Max-Age", "86400")

      if (identical(req$REQUEST_METHOD, "OPTIONS")) {
        if ("setStatus" %in% names(res)) res$setStatus(204) else res$status <- 204
        return("")
      }
      NULL
    }
  ))

  pr
}

# -----------------------------
# 5) Endpoints
# -----------------------------

#* @get /health
function(){
  list(status = "ok", time = as.character(Sys.time()))
}

#* @serializer unboxedJSON
#* @get /debug/env
function(){
  eff <- pg_effective()
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", "")),
    PGHOST = Sys.getenv("PGHOST", unset = ""),
    PGPORT = Sys.getenv("PGPORT", unset = ""),
    PGDATABASE = Sys.getenv("PGDATABASE", unset = ""),
    PGUSER = Sys.getenv("PGUSER", unset = ""),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD","")),
    PGSSLMODE = Sys.getenv("PGSSLMODE", unset = ""),
    PGSCHEMA = Sys.getenv("PGSCHEMA", unset = "public"),
    PGTABLE = Sys.getenv("PGTABLE", unset = ""),
    EFFECTIVE = list(
      host = eff$host, port = eff$port, dbname = eff$dbname, user = eff$user, sslmode = eff$sslmode
    ),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", unset = "")
  )
}

#* @serializer unboxedJSON
#* @get /debug/pingdb
function(){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  base <- DBI::dbGetQuery(con, "
    SELECT current_database() AS db,
           current_user AS usr,
           current_schema() AS schema,
           inet_server_addr() AS server_addr,
           inet_server_port() AS server_port,
           version() AS version;
  ")
  base <- json_safe_df(base)

  out <- list(
    ok = TRUE,
    detected = list(schema = schema, table = table),
    result = base
  )

  if (!is.null(table)) {
    n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))
    out$rows_total <- n$n[[1]]
  }

  out
}

#* @get /indicators
#* @serializer unboxedJSON
function(q = "", req, res){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  if (is.null(table)) {
    if ("setStatus" %in% names(res)) res$setStatus(503) else res$status <- 503
    return(list(error = TRUE, message = glue("Aucune table trouvée dans {schema} (attendu: indicator_values%). Charge d'abord les données dans Postgres Railway.")))
  }

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."{table}"
    WHERE indicator_code IS NOT NULL
  ')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%","", q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                             OR  indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, " ORDER BY indicator_code;")

  json_safe_df(DBI::dbGetQuery(con, qry))
}

#* @serializer unboxedJSON
#* @get /values
function(indicator_code="", ref_area="", start=NA, end=NA, limit=1000, offset=0, req, res){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  if (is.null(table)) {
    if ("setStatus" %in% names(res)) res$setStatus(503) else res$status <- 503
    return(list(error = TRUE, message = glue("Aucune table trouvée dans {schema} (attendu: indicator_values%).")))
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

#* @serializer csv
#* @get /export/csv
function(indicator_code="", ref_area="", start=NA, end=NA, req, res){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  if (is.null(table)) {
    if ("setStatus" %in% names(res)) res$setStatus(503) else res$status <- 503
    return(data.frame(error = "No table found (indicator_values%)"))
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

#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code="", ref_area="", start=NA, end=NA, req, res){
  if (!requireNamespace("writexl", quietly = TRUE)) {
    if ("setStatus" %in% names(res)) res$setStatus(500) else res$status <- 500
    return(charToRaw("writexl package missing in image"))
  }

  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  if (is.null(table)) {
    if ("setStatus" %in% names(res)) res$setStatus(503) else res$status <- 503
    return(charToRaw("No table found (indicator_values%)"))
  }

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM "{schema}"."{table}" WHERE 1=1
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

#* @serializer html
#* @get /metrics
function(req, res){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  if (is.null(table)) {
    if ("setStatus" %in% names(res)) res$setStatus(503) else res$status <- 503
    return("onu_api_rows_total 0\n")
  }

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  ins <- suppressWarnings(as.numeric(as.POSIXct(latest$max_ins[[1]])))
  upd <- suppressWarnings(as.numeric(as.POSIXct(latest$max_upd[[1]])))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", (ins %||% NA_real_), "\n",
    "onu_api_last_updated_at ",  (upd %||% NA_real_), "\n"
  )
}