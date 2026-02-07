# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(as.character(x))) y else x

# ---------------------------
# .env (local only)
# ---------------------------
find_env <- function() {
  cand <- c(
    file.path(getwd(), "..", ".env"),
    file.path(getwd(), ".env"),
    file.path(dirname(getwd()), ".env")
  )
  for (p in cand) {
    p2 <- normalizePath(p, winslash = "/", mustWork = FALSE)
    if (file.exists(p2)) return(p2)
  }
  NA_character_
}

# Sur Railway: si DATABASE_URL est défini => on ignore .env
if (!nzchar(Sys.getenv("DATABASE_URL", ""))) {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    dotenv::load_dot_env(.env.path)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] Pas de DATABASE_URL et .env introuvable (OK si tu injectes les variables autrement).")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

# ---------------------------
# Helpers: parse DATABASE_URL
# ---------------------------
parse_database_url <- function(url) {
  # accepte postgres:// ou postgresql://
  url <- trimws(url)
  url <- sub("^postgresql://", "postgres://", url, ignore.case = TRUE)
  url <- sub("^postgres://", "", url, ignore.case = TRUE)

  # séparer querystring
  qs <- ""
  if (grepl("\\?", url, fixed = FALSE)) {
    parts <- strsplit(url, "\\?", fixed = FALSE)[[1]]
    url <- parts[1]
    qs  <- parts[2] %||% ""
  }

  # userinfo@host:port/db
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

  # host:port/dbname
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

  # sslmode dans querystring (optionnel)
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

  list(host = host, port = port, dbname = db, user = user, password = pass, sslmode = sslmode)
}

pg_schema <- function() Sys.getenv("PGSCHEMA", unset = "public")
pg_table  <- function() Sys.getenv("PGTABLE",  unset = "")

# ---------------------------
# DB connection (Railway-safe)
# ---------------------------
effective_db_cfg <- function() {
  if (nzchar(Sys.getenv("DATABASE_URL", ""))) {
    cfg <- parse_database_url(Sys.getenv("DATABASE_URL"))
    list(
      host    = cfg$host,
      port    = cfg$port,
      dbname  = cfg$dbname,
      user    = cfg$user,
      password= cfg$password,
      sslmode = cfg$sslmode %||% Sys.getenv("PGSSLMODE", unset = "require")
    )
  } else {
    list(
      host    = Sys.getenv("PGHOST", unset = "localhost"),
      port    = suppressWarnings(as.integer(Sys.getenv("PGPORT", unset = "5432"))),
      dbname  = Sys.getenv("PGDATABASE", unset = ""),
      user    = Sys.getenv("PGUSER", unset = Sys.getenv("PGREADUSER", unset = "")),
      password= Sys.getenv("PGPASSWORD", unset = Sys.getenv("PGREADPASS", unset = "")),
      sslmode = Sys.getenv("PGSSLMODE", unset = "prefer")
    )
  }
}

pg_con <- function() {
  cfg <- effective_db_cfg()
  if (is.na(cfg$port) || !nzchar(as.character(cfg$port))) cfg$port <- 5432L
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

table_exists <- function(con, schema, table) {
  if (!nzchar(table)) return(FALSE)
  isTRUE(DBI::dbGetQuery(con, glue(
    "SELECT EXISTS(
       SELECT 1
       FROM information_schema.tables
       WHERE table_schema = {DBI::dbQuoteString(con, schema)}
         AND table_name   = {DBI::dbQuoteString(con, table)}
     ) AS ok"
  ))$ok[1])
}

detect_table <- function(con, schema) {
  # priorités
  candidates <- c(pg_table(), "indicator_values", "indicator_values_all", "indicator_values_staging")
  candidates <- unique(candidates[nzchar(candidates)])

  for (t in candidates) {
    if (table_exists(con, schema, t)) return(t)
  }

  # sinon, prend la première table qui commence par indicator_values
  hit <- DBI::dbGetQuery(con, glue(
    "SELECT table_name
     FROM information_schema.tables
     WHERE table_schema = {DBI::dbQuoteString(con, schema)}
       AND table_type='BASE TABLE'
       AND table_name ILIKE 'indicator_values%'
     ORDER BY table_name"
  ))
  if (nrow(hit) > 0) return(hit$table_name[1])
  ""
}

# JSON safe: convert classes that break swagger/jsonlite
json_safe_df <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0) return(df)
  for (nm in names(df)) {
    # pq_inet / POSIXt / difftime etc -> character
    cls <- class(df[[nm]])
    if (any(cls %in% c("pq_inet", "POSIXct", "POSIXt", "difftime"))) {
      df[[nm]] <- as.character(df[[nm]])
    }
  }
  df
}

# ---------------------------
# API Key filter (no res$status issues)
# ---------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = "Unauthorized: invalid API key")
  }
}

# ---------------------------
# CORS + OPTIONS
# ---------------------------
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
      if (identical(req$REQUEST_METHOD, "OPTIONS")) {
        res$status <- 200
        return(list(ok = TRUE))
      }
      NULL
    }
  ))
  pr
}

# ---------------------------
# Health
# ---------------------------
#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

# ---------------------------
# Debug env (raw + effective)
# ---------------------------
#* @serializer unboxedJSON
#* @get /debug/env
function() {
  eff <- effective_db_cfg()
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", "")),
    PGHOST           = Sys.getenv("PGHOST", unset = ""),
    PGPORT           = Sys.getenv("PGPORT", unset = ""),
    PGDATABASE       = Sys.getenv("PGDATABASE", unset = ""),
    PGUSER           = Sys.getenv("PGUSER", unset = ""),
    PGPASSWORD_set   = nzchar(Sys.getenv("PGPASSWORD", unset = "")),
    PGSSLMODE        = Sys.getenv("PGSSLMODE", unset = ""),
    PGSCHEMA         = pg_schema(),
    PGTABLE          = pg_table(),
    EFFECTIVE = list(
      host    = eff$host,
      port    = eff$port,
      dbname  = eff$dbname,
      user    = eff$user,
      sslmode = eff$sslmode
    ),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", unset = "")
  )
}

# ---------------------------
# Debug ping DB (must work)
# ---------------------------
#* @serializer unboxedJSON
#* @get /debug/pingdb
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)

  # évite pq_inet: cast en text
  q <- glue(
    "SELECT
       current_database() AS db,
       current_user       AS usr,
       current_schema()   AS schema,
       inet_server_addr()::text AS server_addr,
       inet_server_port() AS server_port,
       version()          AS version"
  )

  out <- DBI::dbGetQuery(con, q)
  out <- json_safe_df(out)

  if (!nzchar(table)) {
    return(list(
      ok = TRUE,
      detected = list(schema = schema, table = NULL),
      result = out,
      error = TRUE,
      message = "Aucune table trouvée dans public (attendu: indicator_values%). Charge d'abord les données dans Postgres Railway."
    ))
  }

  list(
    ok = TRUE,
    detected = list(schema = schema, table = table),
    result = out
  )
}

# ---------------------------
# Export CSV
# ---------------------------
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable dans Railway (indicator_values%).")

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

# ---------------------------
# Liste indicateurs
# ---------------------------
#* @serializer unboxedJSON
#* @get /indicators
function(q = "") {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable dans Railway (indicator_values%).")

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

# ---------------------------
# JSON paginé
# ---------------------------
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable dans Railway (indicator_values%).")

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
  rows <- json_safe_df(rows)

  list(total = total, limit = limit, offset = offset, rows = rows)
}

# ---------------------------
# Export XLSX
# ---------------------------
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable dans Railway (indicator_values%).")

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."{table}" WHERE 1=1
  ')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))
  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")

  dat <- DBI::dbGetQuery(con, qry)
  tf  <- tempfile(fileext = ".xlsx")

  if (!requireNamespace("writexl", quietly = TRUE)) {
    install.packages("writexl", repos = "https://cloud.r-project.org")
  }
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ---------------------------
# Metrics (texte)
# ---------------------------
#* @serializer html
#* @get /metrics
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- detect_table(con, schema)
  if (!nzchar(table)) stop("Table introuvable dans Railway (indicator_values%).")

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))
  max_ins <- latest$max_ins[[1]] %||% NA
  max_upd <- latest$max_upd[[1]] %||% NA

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (is.na(max_ins)) "NaN" else as.numeric(as.POSIXct(max_ins)), "\n",
    "onu_api_last_updated_at ",  if (is.na(max_upd)) "NaN" else as.numeric(as.POSIXct(max_upd)), "\n"
  )
} 