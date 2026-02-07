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

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || !nzchar(as.character(x))) y else x

# -------------------------
# .env (LOCAL) : charger seulement si DATABASE_URL absent
# -------------------------
find_env <- function() {
  # priorités: racine projet (/app/.env) puis /app/api/.env puis CWD
  candidates <- c(
    normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE),
    normalizePath(file.path(getwd(), ".env"), mustWork = FALSE),
    normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  )
  for (p in candidates) if (file.exists(p)) return(p)
  NA_character_
}

if (!nzchar(Sys.getenv("DATABASE_URL", ""))) {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    # compat avec anciennes versions de dotenv: PAS de param override
    dotenv::load_dot_env(.env.path)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] DATABASE_URL absent et .env introuvable (OK si vous utilisez Railway Variables).")
  }
} else {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
}

# -------------------------
# Helpers DB
# -------------------------
parse_db_url <- function(x) {
  # Supporte: postgres://user:pass@host:5432/db?sslmode=require
  # et: postgresql://...
  if (!nzchar(x)) return(NULL)

  # enlever "jdbc:" si jamais
  x <- sub("^jdbc:", "", x)

  # extraire query string
  qs <- ""
  if (grepl("\\?", x, fixed = FALSE)) {
    qs <- sub("^[^?]*\\?", "", x)
    x  <- sub("\\?.*$", "", x)
  }

  # scheme://user:pass@host:port/db
  m <- regexec("^(postgres(ql)?):\\/\\/([^@\\/]+)@([^:\\/]+)(:([0-9]+))?\\/(.+)$", x)
  r <- regmatches(x, m)[[1]]
  if (length(r) == 0) return(NULL)

  userpass <- r[4]
  host     <- r[5]
  port     <- r[7] %||% "5432"
  dbname   <- r[8]

  user <- sub(":.*$", "", userpass)
  pass <- sub("^[^:]*:?", "", userpass)
  pass <- utils::URLdecode(pass)

  # params
  sslmode <- NA_character_
  if (nzchar(qs) && grepl("sslmode=", qs, fixed = TRUE)) {
    sslmode <- sub("^.*sslmode=([^&]+).*$", "\\1", qs)
    sslmode <- utils::URLdecode(sslmode)
  }

  list(
    host = host,
    port = as.integer(port),
    dbname = dbname,
    user = user,
    password = pass,
    sslmode = sslmode
  )
}

pg_schema <- function() Sys.getenv("PGSCHEMA", "public")

pg_table <- function() {
  # IMPORTANT: si PGTABLE vide => on force indicator_values
  t <- Sys.getenv("PGTABLE", "")
  if (!nzchar(t)) "indicator_values" else t
}

table_exists <- function(con, schema, table) {
  DBI::dbExistsTable(con, DBI::Id(schema = schema, table = table))
}

# Nettoyage JSON: éviter classes exotiques (pq_inet, POSIXct, etc.)
json_safe_df <- function(df) {
  if (!is.data.frame(df)) return(df)
  for (nm in names(df)) {
    x <- df[[nm]]
    if (inherits(x, "pq_inet") || inherits(x, "inet")) df[[nm]] <- as.character(x)
    if (inherits(x, c("POSIXct", "POSIXlt", "Date"))) df[[nm]] <- as.character(x)
  }
  df
}

pg_con <- function() {
  db_url <- Sys.getenv("DATABASE_URL", "")
  if (nzchar(db_url)) {
    u <- parse_db_url(db_url)
    if (is.null(u)) stop("DATABASE_URL présent mais format non reconnu.")
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = u$host,
      port     = u$port %||% 5432L,
      dbname   = u$dbname,
      user     = u$user,
      password = u$password,
      sslmode  = (u$sslmode %||% Sys.getenv("PGSSLMODE", "require"))
    )
  } else {
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = Sys.getenv("PGHOST", "localhost"),
      port     = as.integer(Sys.getenv("PGPORT", "5432")),
      dbname   = Sys.getenv("PGDATABASE"),
      user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
      password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
      sslmode  = Sys.getenv("PGSSLMODE", "prefer")
    )
  }
}

# -------------------------
# API KEY (désactivé si API_KEY vide)
# -------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got <- req$HTTP_X_API_KEY %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    # Ne pas bricoler res$status (ça a déjà cassé chez toi)
    res$setStatus(401)
    return(list(error = TRUE, message = "Unauthorized: invalid API key"))
  }
}

# -------------------------
# CORS (Railway / navigateur)
# -------------------------
#* @plumber
function(pr) {
  origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",")[[1]] |> trimws()
  pr$registerHooks(list(
    preroute = function(req, res) {
      origin <- req$HTTP_ORIGIN %||% "*"
      allow <- if ("*" %in% origins || origin %in% origins) origin else "null"
      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
      if (identical(req$REQUEST_METHOD, "OPTIONS")) return(list())
    }
  ))
  pr
}

# -------------------------
# Health
# -------------------------
#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

# -------------------------
# Debug env (affiche aussi le host dérivé de DATABASE_URL)
# -------------------------
#* @get /debug/env
function() {
  db_url <- Sys.getenv("DATABASE_URL", "")
  u <- if (nzchar(db_url)) parse_db_url(db_url) else NULL

  list(
    DATABASE_URL_set = nzchar(db_url),
    # valeurs env brutes (peu fiables sur Railway si .env local traîne)
    PGHOST = Sys.getenv("PGHOST", unset = ""),
    PGPORT = Sys.getenv("PGPORT", unset = ""),
    PGDATABASE = Sys.getenv("PGDATABASE", unset = ""),
    PGUSER = Sys.getenv("PGUSER", unset = ""),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD", unset = "")),
    PGSSLMODE = Sys.getenv("PGSSLMODE", unset = ""),

    PGSCHEMA = pg_schema(),
    PGTABLE  = pg_table(),

    # valeurs "effectives" si DATABASE_URL
    EFFECTIVE = if (!is.null(u)) list(
      host = u$host,
      port = u$port,
      dbname = u$dbname,
      user = u$user,
      sslmode = u$sslmode %||% Sys.getenv("PGSSLMODE", "require")
    ) else NULL,

    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", unset = "")
  )
}

# -------------------------
# Debug ping DB
# -------------------------
#* @get /debug/pingdb
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  # cast ::text pour éviter pq_inet dans JSON
  info <- DBI::dbGetQuery(con, "
    SELECT
      current_database() AS db,
      current_user AS usr,
      current_schema() AS schema,
      inet_server_addr()::text AS server_addr,
      inet_server_port() AS server_port,
      version() AS version;
  ") |> json_safe_df()

  exists <- table_exists(con, schema, table)

  list(
    ok = TRUE,
    detected = list(schema = schema, table = table, table_exists = exists),
    result = info
  )
}

# -------------------------
# Liste indicateurs
# -------------------------
#* @get /indicators
function(q = "") {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Mets PGTABLE=indicator_values (ou le bon nom) dans Railway Variables."))
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
  DBI::dbGetQuery(con, qry)
}

# -------------------------
# JSON paginé
# -------------------------
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Mets PGTABLE=indicator_values (ou le bon nom) dans Railway Variables."))
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
  ')) |> json_safe_df()

  list(total = total, limit = limit, offset = offset, rows = rows)
}

# -------------------------
# Export CSV
# -------------------------
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Mets PGTABLE=indicator_values (ou le bon nom) dans Railway Variables."))
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

# -------------------------
# Export XLSX
# -------------------------
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Mets PGTABLE=indicator_values (ou le bon nom) dans Railway Variables."))
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
  dat <- DBI::dbGetQuery(con, qry) |> json_safe_df()

  tf <- tempfile(fileext = ".xlsx")
  if (!requireNamespace("writexl", quietly = TRUE)) {
    install.packages("writexl", repos = "https://cloud.r-project.org")
  }
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# -------------------------
# Metrics (texte)
# -------------------------
#* @serializer html
#* @get /metrics
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Mets PGTABLE=indicator_values (ou le bon nom) dans Railway Variables."))
  }

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", as.numeric(as.POSIXct(latest$max_ins[[1]])), "\n",
    "onu_api_last_updated_at ",  as.numeric(as.POSIXct(latest$max_upd[[1]])),  "\n"
  )
}