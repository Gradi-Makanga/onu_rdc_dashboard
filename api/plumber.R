# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(readr)
  library(jsonlite)
})

# --------------------------------------------------
# Helpers
# --------------------------------------------------

db_connect <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode  = Sys.getenv("PGSSLMODE", "require")
  )
}

safe_db <- function(expr) {
  tryCatch(expr, error = function(e) {
    list(
      error = TRUE,
      message = conditionMessage(e)
    )
  })
}

# --------------------------------------------------
# Global filters (CORS + JSON)
# --------------------------------------------------

#* @filter cors
function(req, res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
  if (req$REQUEST_METHOD == "OPTIONS") {
    res$status <- 200
    return(list())
  }
  plumber::forward()
}

# --------------------------------------------------
# Health
# --------------------------------------------------

#* Health check
#* @get /health
function() {
  list(
    status = "ok",
    service = "onu_rdc_api",
    time = Sys.time()
  )
}

# --------------------------------------------------
# Debug DB
# --------------------------------------------------

#* Ping database
#* @get /debug/pingdb
function() {
  safe_db({
    con <- db_connect()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    test <- DBI::dbGetQuery(con, "SELECT 1 AS ok")
    list(
      db = "connected",
      result = test
    )
  })
}

# --------------------------------------------------
# Indicators list
# --------------------------------------------------

#* List indicators
#* @get /indicators
function() {
  safe_db({
    con <- db_connect()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    DBI::dbGetQuery(con, "
      SELECT DISTINCT indicator_code, indicator_name
      FROM indicators
      ORDER BY indicator_name
    ")
  })
}

# --------------------------------------------------
# Values
# --------------------------------------------------

#* Indicator values
#* @param indicator_code
#* @get /values
function(indicator_code = NULL) {
  safe_db({
    con <- db_connect()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    if (is.null(indicator_code)) {
      stop("indicator_code is required")
    }

    DBI::dbGetQuery(
      con,
      "
      SELECT *
      FROM indicator_values
      WHERE indicator_code = $1
      ORDER BY year
      ",
      params = list(indicator_code)
    )
  })
}

# --------------------------------------------------
# Export CSV
# --------------------------------------------------

#* Export indicators as CSV
#* @get /export/csv
#* @serializer contentType list(type = "text/csv")
function() {
  safe_db({
    con <- db_connect()
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    df <- DBI::dbGetQuery(con, "
      SELECT *
      FROM indicator_values
      ORDER BY indicator_code, year
    ")

    readr::format_csv(df)
  })
}