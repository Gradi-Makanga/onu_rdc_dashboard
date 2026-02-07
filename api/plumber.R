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

# =========================
# Chargement .env (Railway / local)
# =========================
find_env <- function(){
  p <- c(
    file.path(getwd(), ".env"),
    file.path(getwd(), "..", ".env"),
    file.path(dirname(getwd()), ".env")
  )
  p[file.exists(p)][1] %||% NA_character_
}

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x)) y else x

.env.path <- find_env()
if (!is.na(.env.path)) {
  dotenv::load_dot_env(.env.path)
  message("[plumber] .env chargé depuis: ", .env.path)
} else {
  message("[plumber] aucun .env trouvé (variables Railway utilisées)")
}

# =========================
# Connexion PostgreSQL
# =========================
pg_con <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode  = Sys.getenv("PGSSLMODE", "require")
  )
}

# =========================
# API KEY (optionnelle)
# =========================
#* @filter apikey
function(req, res){
  key_expected <- Sys.getenv("API_KEY", "")
  key_got <- req$HTTP_X_API_KEY %||% ""

  if (key_expected == "" || identical(key_expected, key_got)) {
    forward()
  } else {
    res$status <- 401
    list(error = TRUE, message = "Invalid API key")
  }
}

# =========================
# CORS
# =========================
#* @plumber
function(pr){
  pr$registerHooks(list(
    preroute = function(req, res){
      res$setHeader("Access-Control-Allow-Origin", "*")
      res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    }
  ))
  pr
}

# =========================
# Health
# =========================
#* @get /health
function(){
  list(ok = TRUE, time = as.character(Sys.time()))
}

# =========================
# Debug env
# =========================
#* @get /debug/env
function(){
  list(
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGSSLMODE = Sys.getenv("PGSSLMODE")
  )
}

# =========================
# Debug ping DB (SAFE JSON)
# =========================
#* @get /debug/pingdb
function(){
  tryCatch({
    con <- pg_con()
    on.exit(dbDisconnect(con), add = TRUE)

    r <- dbGetQuery(con, "
      SELECT
        current_database() AS db,
        current_user AS usr,
        current_schema() AS schema,
        inet_server_addr()::text AS server_addr,
        inet_server_port() AS server_port,
        version()
    ")

    list(
      ok = TRUE,
      config = list(
        host   = Sys.getenv("PGHOST"),
        port   = as.integer(Sys.getenv("PGPORT", "5432")),
        dbname = Sys.getenv("PGDATABASE"),
        user   = Sys.getenv("PGUSER")
      ),
      result = r
    )
  }, error = function(e){
    list(
      ok = FALSE,
      error = TRUE,
      message = conditionMessage(e)
    )
  })
}

# =========================
# Liste des indicateurs
# =========================
#* @get /indicators
function(q = ""){
  con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  sql <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."indicator_values"
    WHERE indicator_code IS NOT NULL
  ')

  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    sql <- paste0(sql, glue(
      ' AND (indicator_code ILIKE {dbQuoteString(con, like)}
             OR indicator_name ILIKE {dbQuoteString(con, like)})'
    ))
  }

  sql <- paste0(sql, " ORDER BY indicator_code;")
  dbGetQuery(con, sql)
}

# =========================
# Valeurs (JSON paginé)
# =========================
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA,
         limit = 1000, offset = 0){

  con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  limit  <- max(1, min(as.integer(limit), 10000))
  offset <- max(0, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code))
    where <- paste0(where, glue(' AND indicator_code = {dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))
    where <- paste0(where, glue(' AND ref_area = {dbQuoteString(con, ref_area)}'))
  if (!is.na(as.numeric(start)))
    where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(as.numeric(end)))
    where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- dbGetQuery(con, glue(
    'SELECT COUNT(*)::int AS n FROM "{schema}"."indicator_values" {where}'
  ))$n[1]

  rows <- dbGetQuery(con, glue(
    '
    SELECT indicator_code, indicator_name, ref_area,
           period, value, obs_status, source,
           inserted_at, updated_at
    FROM "{schema}"."indicator_values"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset}
    '
  ))

  list(
    total = total,
    limit = limit,
    offset = offset,
    rows = rows
  )
}

# =========================
# Export CSV
# =========================
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA){
  con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  sql <- glue('
    SELECT indicator_code, indicator_name, ref_area,
           period, value, obs_status, source
    FROM "{schema}"."indicator_values"
    WHERE 1=1
  ')

  if (nzchar(indicator_code))
    sql <- paste0(sql, glue(' AND indicator_code = {dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))
    sql <- paste0(sql, glue(' AND ref_area = {dbQuoteString(con, ref_area)}'))
  if (!is.na(as.numeric(start)))
    sql <- paste0(sql, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(as.numeric(end)))
    sql <- paste0(sql, glue(' AND period <= {as.numeric(end)}'))

  sql <- paste0(sql, " ORDER BY indicator_code, ref_area, period")
  dbGetQuery(con, sql)
}

# =========================
# Export XLSX
# =========================
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA){
  if (!requireNamespace("writexl", quietly = TRUE))
    stop("Package writexl manquant")

  dat <- plumber::call_api(
    paste0(Sys.getenv("SELF_URL"), "/export/csv"),
    list(indicator_code = indicator_code,
         ref_area = ref_area,
         start = start,
         end = end)
  )

  tf <- tempfile(fileext = ".xlsx")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# =========================
# Metrics (Prometheus-style)
# =========================
#* @serializer html
#* @get /metrics
function(){
  con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  n <- dbGetQuery(con, glue(
    'SELECT COUNT(*)::bigint AS n FROM "{schema}"."indicator_values"'
  ))$n[1]

  paste0("onu_api_rows_total ", n, "\n")
}