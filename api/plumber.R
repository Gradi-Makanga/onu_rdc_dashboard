# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
})

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || identical(x, "")) y else x

# ---------------------------
# 1) Chargement .env (LOCAL seulement)
#    Sur Railway: on utilise DATABASE_URL en priorité et on ignore les PG* du .env
# ---------------------------
find_env <- function() {
  # priorité: racine projet puis dossier courant
  candidates <- c(
    file.path(getwd(), ".env"),
    file.path(getwd(), "..", ".env"),
    file.path(dirname(getwd()), ".env")
  )
  candidates <- vapply(candidates, normalizePath, mustWork = FALSE, FUN.VALUE = character(1))
  for (p in candidates) if (file.exists(p)) return(p)
  NA_character_
}

# Si DATABASE_URL est déjà présent => Railway => NE PAS charger .env
if (!nzchar(Sys.getenv("DATABASE_URL", unset = ""))) {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    dotenv::load_dot_env(.env.path)  # pas de override=... (selon version dotenv)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] Pas de DATABASE_URL et pas de .env trouvé (OK si variables déjà définies).")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

# ---------------------------
# 2) Parse DATABASE_URL (sans httr2)
#    Format typique: postgres://user:pass@host:port/dbname?sslmode=require
# ---------------------------
parse_database_url <- function(url) {
  url <- trimws(url)
  url <- sub("^postgresql://", "postgres://", url)

  # enlever le schéma
  rest <- sub("^postgres://", "", url)

  # séparer query (?...)
  parts_q <- strsplit(rest, "\\?", fixed = FALSE)[[1]]
  main <- parts_q[1]
  query <- if (length(parts_q) > 1) parts_q[2] else ""

  # userinfo@host:port/db
  # userinfo (optionnel)
  at_split <- strsplit(main, "@", fixed = TRUE)[[1]]
  if (length(at_split) == 2) {
    userinfo <- at_split[1]
    host_db  <- at_split[2]
  } else {
    userinfo <- ""
    host_db  <- at_split[1]
  }

  user <- ""
  password <- ""
  if (nzchar(userinfo)) {
    up <- strsplit(userinfo, ":", fixed = TRUE)[[1]]
    user <- utils::URLdecode(up[1])
    if (length(up) > 1) password <- utils::URLdecode(paste(up[-1], collapse=":"))
  }

  # host:port/dbname
  hp_db <- strsplit(host_db, "/", fixed = TRUE)[[1]]
  hostport <- hp_db[1]
  dbname   <- if (length(hp_db) > 1) utils::URLdecode(paste(hp_db[-1], collapse="/")) else ""

  hp <- strsplit(hostport, ":", fixed = TRUE)[[1]]
  host <- hp[1]
  port <- if (length(hp) > 1) suppressWarnings(as.integer(hp[2])) else NA_integer_

  # query params
  sslmode <- ""
  if (nzchar(query)) {
    kvs <- strsplit(query, "&", fixed = TRUE)[[1]]
    for (kv in kvs) {
      kv2 <- strsplit(kv, "=", fixed = TRUE)[[1]]
      k <- utils::URLdecode(kv2[1] %||% "")
      v <- utils::URLdecode(kv2[2] %||% "")
      if (tolower(k) == "sslmode") sslmode <- v
    }
  }

  list(
    host = host,
    port = port,
    dbname = dbname,
    user = user,
    password = password,
    sslmode = sslmode
  )
}

# ---------------------------
# 3) Connexion PG: priorité DATABASE_URL, sinon PGHOST/PGPORT/...
# ---------------------------
pg_schema <- function() Sys.getenv("PGSCHEMA", unset = "public")

# Table: utilise PGTABLE si fourni, sinon on détecte automatiquement
pg_table_candidates <- function() {
  unique(Filter(nzchar, c(
    Sys.getenv("PGTABLE", unset = ""),
    "indicator_values",
    "indicator_values_all",
    "indicator_values_staging",
    "indicator_values_latest",
    "indicator_value",
    "indicators_values"
  )))
}

table_exists <- function(con, schema, table) {
  q <- "
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1;
  "
  x <- try(DBI::dbGetQuery(con, q, params = list(schema, table)), silent = TRUE)
  is.data.frame(x) && nrow(x) > 0
}

pick_table <- function(con, schema) {
  for (t in pg_table_candidates()) {
    if (table_exists(con, schema, t)) return(t)
  }
  NA_character_
}

pg_con <- function() {
  db_url <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(db_url)) {
    info <- parse_database_url(db_url)
    # sslmode: DATABASE_URL > PGSSLMODE > prefer
    sslmode <- info$sslmode %||% Sys.getenv("PGSSLMODE", unset = "prefer")
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = info$host,
      port     = if (!is.na(info$port)) info$port else 5432L,
      dbname   = info$dbname,
      user     = info$user,
      password = info$password,
      sslmode  = sslmode
    )
  } else {
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = Sys.getenv("PGHOST"),
      port     = as.integer(Sys.getenv("PGPORT", "5432")),
      dbname   = Sys.getenv("PGDATABASE"),
      user     = Sys.getenv("PGUSER"),
      password = Sys.getenv("PGPASSWORD"),
      sslmode  = Sys.getenv("PGSSLMODE", "prefer")
    )
  }
}

# Convertir en JSON “safe” (évite les classes bizarres type pq_inet)
json_safe_df <- function(df) {
  if (!is.data.frame(df)) return(df)
  for (nm in names(df)) {
    x <- df[[nm]]
    if (inherits(x, c("POSIXct","POSIXlt"))) df[[nm]] <- format(x, tz="UTC", usetz=TRUE)
    if (inherits(x, "difftime")) df[[nm]] <- as.numeric(x)
    if (!is.atomic(x) || is.list(x)) df[[nm]] <- vapply(x, as.character, character(1))
    if (inherits(x, "pq_inet")) df[[nm]] <- as.character(x)
  }
  df
}

# ---------------------------
# 4) API KEY filter (désactivée si API_KEY vide)
# ---------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got <- req$HTTP_X_API_KEY %||% req$HTTP_X_APIKEY %||% req$HTTP_AUTHORIZATION %||% ""
  got <- sub("^Bearer\\s+", "", got)

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = "Unauthorized: invalid API key")
  }
}

# ---------------------------
# 5) CORS + OPTIONS
# ---------------------------
#* @plumber
function(pr) {
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]]
  origins <- trimws(origins)

  pr$registerHooks(list(
    preroute = function(req, res) {
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% origins || origin %in% origins) origin else "null"

      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
      res$setHeader("Access-Control-Max-Age", "86400")

      if (identical(req$REQUEST_METHOD, "OPTIONS")) {
        res$status <- 204
        return(list())
      }
      NULL
    }
  ))
  pr
}

# ---------------------------
# Endpoints
# ---------------------------

#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

#* @get /debug/env
function() {
  # Montre les variables (et indique si DATABASE_URL est présent)
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", "")),
    PGHOST   = Sys.getenv("PGHOST", ""),
    PGPORT   = Sys.getenv("PGPORT", ""),
    PGDATABASE = Sys.getenv("PGDATABASE", ""),
    PGUSER   = Sys.getenv("PGUSER", ""),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD", "")),
    PGSSLMODE = Sys.getenv("PGSSLMODE", ""),
    PGSCHEMA = pg_schema(),
    PGTABLE  = Sys.getenv("PGTABLE", ""),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", "")
  )
}

#* @get /debug/pingdb
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  tbl <- pick_table(con, schema)

  info <- DBI::dbGetQuery(con, "
    SELECT current_database() AS db,
           current_user      AS usr,
           current_schema()  AS schema,
           inet_server_addr()::text AS server_addr,
           inet_server_port() AS server_port,
           version()         AS version;
  ")
  info <- json_safe_df(info)

  list(
    ok = TRUE,
    detected = list(schema = schema, table = tbl %||% NA_character_),
    result = info
  )
}

#* @get /debug/tables
function(pattern = "indicator") {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  like <- paste0("%", gsub("%", "", pattern), "%")
  x <- DBI::dbGetQuery(con, "
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog','information_schema')
      AND table_name ILIKE $1
    ORDER BY table_schema, table_name;
  ", params = list(like))
  json_safe_df(x)
}

# ---- Export CSV ----
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- pick_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}. Candidates: {paste(pg_table_candidates(), collapse=', ')}"))

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

# ---- Liste indicateurs ----
#* @get /indicators
function(q = "") {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- pick_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}. Candidates: {paste(pg_table_candidates(), collapse=', ')}"))

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

# ---- JSON paginé ----
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- pick_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}. Candidates: {paste(pg_table_candidates(), collapse=', ')}"))

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

# ---- Export XLSX ----
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- pick_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}. Candidates: {paste(pg_table_candidates(), collapse=', ')}"))

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
    install.packages("writexl", repos = "https://cloud.r-project.org")
  }
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ---- Metrics (texte Prometheus-like) ----
#* @serializer html
#* @get /metrics
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table <- pick_table(con, schema)
  if (is.na(table)) stop(glue("Table introuvable dans {schema}. Candidates: {paste(pg_table_candidates(), collapse=', ')}"))

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  max_ins <- latest$max_ins[[1]]
  max_upd <- latest$max_upd[[1]]

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (!is.na(max_ins)) as.numeric(as.POSIXct(max_ins)) else "NaN", "\n",
    "onu_api_last_updated_at ",  if (!is.na(max_upd)) as.numeric(as.POSIXct(max_upd)) else "NaN", "\n"
  )
}