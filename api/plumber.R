# D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/api/plumber.R
library(plumber)
library(DBI); library(RPostgres)
library(readr); library(dplyr); library(glue); library(dotenv)

# --- Localisation dynamique du .env (parent de 'api' en priorité) ---
find_env <- function(){
  p1 <- normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE)
  p2 <- normalizePath(file.path(getwd(), ".env"), mustWork = FALSE)
  p3 <- normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  for (p in c(p1,p2,p3)) if (file.exists(p)) return(p)
  NA_character_
}
.env.path <- find_env()
if (is.na(.env.path)) stop("❌ Fichier .env introuvable (place-le à la racine du projet).")
dotenv::load_dot_env(.env.path)
message("[plumber] .env chargé depuis: ", .env.path)

pg_con <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT","5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER","")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS","")),
    sslmode  = Sys.getenv("PGSSLMODE","prefer")
  )
}
`%||%` <- function(x, y) if (is.null(x) || is.na(x)) y else x

# ---- API Key (désactivée si API_KEY vide) ----
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

# ---- CORS ----
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
    }
  ))
  pr
}

# ---- Health ----
#* @get /health
function(){ list(status="ok", time=as.character(Sys.time())) }

# ---- Debug env ----
#* @get /debug/env
function(){
  list(
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

# ---- Debug ping DB ----
#* @get /debug/pingdb
function(){
  con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbGetQuery(con, 'SELECT current_database() AS db, current_user AS user, current_schema() AS schema, version();')
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
 