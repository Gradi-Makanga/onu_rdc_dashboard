# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dplyr)
  library(dotenv)
})

# ------------------------------------------------------------------
# .env (optionnel en local, ignoré en prod Railway)
# ------------------------------------------------------------------
if (file.exists(".env")) {
  dotenv::load_dot_env(".env")
  message("[plumber] .env chargé depuis .env")
}

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

# ------------------------------------------------------------------
# Connexion Postgres (Railway FIRST)
# ------------------------------------------------------------------
pg_con <- function() {

  # Railway standard
  if (nzchar(Sys.getenv("DATABASE_URL"))) {
    return(
      DBI::dbConnect(
        RPostgres::Postgres(),
        dbname = Sys.getenv("DATABASE_URL")
      )
    )
  }

  # Fallback manuel
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode  = "prefer"
  )
}

# ------------------------------------------------------------------
# Configuration table
# ------------------------------------------------------------------
PGSCHEMA <- Sys.getenv("PGSCHEMA", "public")
PGTABLE  <- Sys.getenv("PGTABLE", "indicator_values")

# ------------------------------------------------------------------
# CORS
# ------------------------------------------------------------------
#* @plumber
function(pr) {
  pr$registerHooks(list(
    preroute = function(req, res) {
      res$setHeader("Access-Control-Allow-Origin", "*")
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    }
  ))
  pr
}

# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------
#* @get /health
function() {
  list(ok = TRUE, time = as.character(Sys.time()))
}

# ------------------------------------------------------------------
# Debug env
# ------------------------------------------------------------------
#* @get /debug/env
function() {
  list(
    PGSCHEMA = PGSCHEMA,
    PGTABLE  = PGTABLE,
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL")),
    PGHOST = Sys.getenv("PGHOST"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER")
  )
}

# ------------------------------------------------------------------
# Debug ping DB (CRITIQUE)
# ------------------------------------------------------------------
#* @get /debug/pingdb
function() {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  res <- DBI::dbGetQuery(con, "
    SELECT
      current_database()  AS db,
      current_user        AS usr,
      current_schema()    AS schema,
      inet_server_addr()  AS server_addr,
      inet_server_port()  AS server_port,
      version()
  ")

  list(
    ok = TRUE,
    config = list(
      schema = PGSCHEMA,
      table  = PGTABLE
    ),
    result = res
  )
}

# ------------------------------------------------------------------
# Liste indicateurs
# ------------------------------------------------------------------
#* @get /indicators
function(q = "") {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  sql <- glue("
    SELECT DISTINCT indicator_code, indicator_name
    FROM {DBI::dbQuoteIdentifier(con, PGSCHEMA)}.{DBI::dbQuoteIdentifier(con, PGTABLE)}
    WHERE indicator_code IS NOT NULL
  ")

  if (nzchar(q)) {
    like <- paste0('%', q, '%')
    sql <- glue("{sql}
      AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
           OR indicator_name ILIKE {DBI::dbQuoteString(con, like)})
    ")
  }

  DBI::dbGetQuery(con, sql)
}

# ------------------------------------------------------------------
# Valeurs paginées
# ------------------------------------------------------------------
#* @get /values
#* @serializer unboxedJSON
function(indicator_code = "", ref_area = "", limit = 1000, offset = 0) {

  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  limit  <- max(1, min(as.integer(limit), 10000))
  offset <- max(0, as.integer(offset))

  where <- "WHERE 1=1"

  if (nzchar(indicator_code))
    where <- glue("{where} AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}")

  if (nzchar(ref_area))
    where <- glue("{where} AND ref_area = {DBI::dbQuoteString(con, ref_area)}")

  total <- DBI::dbGetQuery(con, glue("
    SELECT COUNT(*)::int AS n
    FROM {DBI::dbQuoteIdentifier(con, PGSCHEMA)}.{DBI::dbQuoteIdentifier(con, PGTABLE)}
    {where}
  "))$n[1]

  rows <- DBI::dbGetQuery(con, glue("
    SELECT *
    FROM {DBI::dbQuoteIdentifier(con, PGSCHEMA)}.{DBI::dbQuoteIdentifier(con, PGTABLE)}
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset}
  "))

  list(
    total  = total,
    limit  = limit,
    offset = offset,
    rows   = rows
  )
}

# ------------------------------------------------------------------
# Export CSV
# ------------------------------------------------------------------
#* @serializer csv
#* @get /export/csv
function() {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbGetQuery(con, glue("
    SELECT *
    FROM {DBI::dbQuoteIdentifier(con, PGSCHEMA)}.{DBI::dbQuoteIdentifier(con, PGTABLE)}
    ORDER BY indicator_code, ref_area, period
  "))
}

# ------------------------------------------------------------------
# Export XLSX
# ------------------------------------------------------------------
#* @get /export/xlsx
function() {
  if (!requireNamespace("writexl", quietly = TRUE))
    stop("Package writexl missing")

  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  dat <- DBI::dbGetQuery(con, glue("
    SELECT *
    FROM {DBI::dbQuoteIdentifier(con, PGSCHEMA)}.{DBI::dbQuoteIdentifier(con, PGTABLE)}
  "))

  tf <- tempfile(fileext = ".xlsx")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ------------------------------------------------------------------
# Metrics (Prometheus)
# ------------------------------------------------------------------
#* @serializer html
#* @get /metrics
function() {
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  n <- DBI::dbGetQuery(con, glue("
    SELECT COUNT(*)::bigint AS n
    FROM {DBI::dbQuoteIdentifier(con, PGSCHEMA)}.{DBI::dbQuoteIdentifier(con, PGTABLE)}
  "))$n[1]

  paste0("onu_api_rows_total ", n, "\n")
} 