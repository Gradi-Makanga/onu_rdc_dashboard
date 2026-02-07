# D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/api/plumber.R

suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(readr)
  library(dplyr)
  library(glue)
  library(dotenv)
  library(jsonlite)
})

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || !nzchar(as.character(x))) y else x

# --- Localisation dynamique du .env (parent de 'api' en priorité) ---
find_env <- function(){
  p1 <- normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE)
  p2 <- normalizePath(file.path(getwd(), ".env"), mustWork = FALSE)
  p3 <- normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  for (p in c(p1, p2, p3)) if (file.exists(p)) return(p)
  NA_character_
}
.env.path <- find_env()
if (!is.na(.env.path)) {
  dotenv::load_dot_env(.env.path)
  message("[plumber] .env chargé depuis: ", .env.path)
} else {
  message("[plumber] ⚠️ .env introuvable (OK si toutes les variables sont définies sur Railway).")
}

# --- Connexion Postgres (support DATABASE_URL ou variables PG*) ---
pg_con <- function() {
  dburl <- Sys.getenv("DATABASE_URL", unset = "")
  sslm  <- Sys.getenv("PGSSLMODE", unset = "require")

  if (nzchar(dburl)) {
    # RPostgres accepte une URL dans dbname
    return(DBI::dbConnect(RPostgres::Postgres(), dbname = dburl, sslmode = sslm))
  }

  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST", unset = ""),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE", unset = ""),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
    sslmode  = sslm
  )
}

# --- helper: rendre un data.frame 100% JSON-safe ---
df_json_safe <- function(df){
  if (is.null(df) || !NROW(df)) return(df)
  # Convertit tout en character (évite pq_inet, POSIXct, etc. qui cassent Swagger/JSON)
  as.data.frame(lapply(df, function(x){
    if (inherits(x, "POSIXt")) format(x, tz = "UTC", usetz = TRUE) else as.character(x)
  }), stringsAsFactors = FALSE)
}

# ---- API Key filter (désactivée si API_KEY vide) ----
#     Important: ne PAS manipuler res$status directement (ça peut casser swagger).
#* @filter apikey
function(req, res){
  # Laisser swagger/docs passer sans clé
  p <- req$PATH_INFO %||% ""
  if (startsWith(p, "/__docs__") || p %in% c("/openapi.json", "/health")) return(forward())

  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- (req$HTTP_X_API_KEY %||% req$HTTP_X_APIKEY) %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    if (!is.null(res$setStatus)) res$setStatus(401) else res$status <- 401
    return(list(error = TRUE, message = "Unauthorized: invalid API key"))
  }
}

# ---- CORS + Error handler ----
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

  pr$setErrorHandler(function(req, res, err){
    if (!is.null(res$setStatus)) res$setStatus(500) else res$status <- 500
    list(error = TRUE, message = conditionMessage(err))
  })

  pr
}

# ---- Health ----
#* @get /health
function(){ list(status = "ok", time = as.character(Sys.time())) }

# ---- Debug env ----
#* @get /debug/env
function(){
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL")),
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGREADUSER = Sys.getenv("PGREADUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGREADPASS_set = nzchar(Sys.getenv("PGREADPASS")),
    PGSSLMODE = Sys.getenv("PGSSLMODE"),
    PGSCHEMA = Sys.getenv("PGSCHEMA", "public"),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN")
  )
}

# ---- Debug ping DB (JSON-safe, sans pq_inet) ----
#* @get /debug/pingdb
function(){
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  # Simple + robuste, renvoie une LIST (pas un data.frame) => swagger OK
  row <- DBI::dbGetQuery(con, "
    SELECT
      current_database()::text AS db,
      current_user::text       AS user,
      current_schema()::text   AS schema,
      version()::text          AS version,
      now()::text              AS server_time
  ")
  if (!NROW(row)) return(list(error = TRUE, message = "No result from ping query"))

  as.list(df_json_safe(row)[1, , drop = FALSE])
}

# ---- Export CSV ----
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."indicator_values"
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
function(q = ""){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{schema}"."indicator_values"
    WHERE indicator_code IS NOT NULL
  ')
  if (nzchar(q)) {
    like <- paste0("%", gsub("%", "", q), "%")
    qry <- paste0(qry, glue(' AND (indicator_code ILIKE {DBI::dbQuoteString(con, like)}
                         OR    indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }
  qry <- paste0(qry, " ORDER BY indicator_code;")
  df_json_safe(DBI::dbGetQuery(con, qry))
}

# ---- JSON paginé ----
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  limit  <- max(1, min(as.integer(limit), 10000))
  offset <- max(0, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::int AS n FROM "{schema}"."indicator_values" {where};'))$n[1]

  rows <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{schema}"."indicator_values"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))

  list(total = total, limit = limit, offset = offset, rows = df_json_safe(rows))
}

# ---- Export XLSX ----
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM "{schema}"."indicator_values" WHERE 1=1
  ')
  if (nzchar(indicator_code)) qry <- paste0(qry, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       qry <- paste0(qry, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) qry <- paste0(qry, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   qry <- paste0(qry, glue(' AND period <= {as.numeric(end)}'))

  qry <- paste0(qry, " ORDER BY indicator_code, ref_area, period;")
  dat <- df_json_safe(DBI::dbGetQuery(con, qry))

  tf <- tempfile(fileext = ".xlsx")
  if (!requireNamespace("writexl", quietly = TRUE)) install.packages("writexl")
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

# ---- Metrics (texte) ----
#* @serializer html
#* @get /metrics
function(){
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."indicator_values";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."indicator_values";'))

  max_ins <- latest$max_ins[[1]] %||% NA
  max_upd <- latest$max_upd[[1]] %||% NA

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (!is.na(max_ins)) as.numeric(as.POSIXct(max_ins)) else NA, "\n",
    "onu_api_last_updated_at ",  if (!is.na(max_upd)) as.numeric(as.POSIXct(max_upd)) else NA, "\n"
  )
}