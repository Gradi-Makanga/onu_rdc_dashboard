# api/plumber.R
library(plumber)
library(DBI)
library(RPostgres)
library(glue)
library(dotenv)

# ==============================
# Load .env (local only)
# ==============================
if (file.exists(".env")) {
  dotenv::load_dot_env(".env")
  message("[plumber] .env chargé depuis .env")
}

# ==============================
# PostgreSQL connection helper
# ==============================
pg_con <- function() {

  # ---- Railway mode (PRIORITY) ----
  db_url <- Sys.getenv("DATABASE_URL", "")
  if (nzchar(db_url)) {
    return(DBI::dbConnect(
      RPostgres::Postgres(),
      dbname = db_url
    ))
  }

  # ---- Local / fallback mode ----
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST", "localhost"),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode  = Sys.getenv("PGSSLMODE", "prefer")
  )
}

# ==============================
# CORS
# ==============================
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

# ==============================
# Health
# ==============================
#* @get /health
function(){
  list(ok = TRUE, time = as.character(Sys.time()))
}

# ==============================
# Debug ENV
# ==============================
#* @get /debug/env
function(){
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL")),
    PGHOST = Sys.getenv("PGHOST"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGSCHEMA = Sys.getenv("PGSCHEMA", "public"),
    PGTABLE = Sys.getenv("PGTABLE", "indicator_values")
  )
}

# ==============================
# Debug Ping DB  ✅
# ==============================
#* @get /debug/pingdb
function(){
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  res <- DBI::dbGetQuery(con, "
    SELECT
      current_database() AS db,
      current_user       AS usr,
      current_schema()   AS schema,
      inet_server_addr() AS server_addr,
      inet_server_port() AS server_port,
      version()
  ")

  list(
    ok = TRUE,
    result = res
  )
}

# ==============================
# Indicators
# ==============================
#* @get /indicators
function(){
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema <- Sys.getenv("PGSCHEMA", "public")
  table  <- Sys.getenv("PGTABLE", "indicator_values")

  DBI::dbGetQuery(con, glue("
    SELECT DISTINCT indicator_code, indicator_name
    FROM \"{schema}\".\"{table}\"
    ORDER BY indicator_code
  "))
}

# ==============================
# Values
# ==============================
#* @get /values
function(limit = 1000, offset = 0){
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema <- Sys.getenv("PGSCHEMA", "public")
  table  <- Sys.getenv("PGTABLE", "indicator_values")

  DBI::dbGetQuery(con, glue("
    SELECT *
    FROM \"{schema}\".\"{table}\"
    ORDER BY indicator_code, ref_area, period
    LIMIT {as.integer(limit)} OFFSET {as.integer(offset)}
  "))
}

# ==============================
# Metrics
# ==============================
#* @serializer html
#* @get /metrics
function(){
  con <- pg_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema <- Sys.getenv("PGSCHEMA", "public")
  table  <- Sys.getenv("PGTABLE", "indicator_values")

  n <- DBI::dbGetQuery(con, glue("
    SELECT COUNT(*) AS n FROM \"{schema}\".\"{table}\"
  "))$n

  paste0("onu_rows_total ", n, "\n")
}