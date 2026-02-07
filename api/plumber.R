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

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || identical(x, "")) y else x

# --- Charger .env si présent (NE PAS planter en prod Railway) ---
find_env <- function() {
  cands <- c(
    # cas Railway
    "/app/.env",
    "/app/api/.env",
    # cas projet local (racine puis api/)
    file.path(getwd(), ".env"),
    file.path(getwd(), "..", ".env"),
    file.path(dirname(getwd()), ".env"),
    # fallback: dossier du fichier courant si possible
    file.path("..", ".env")
  )
  cands <- unique(normalizePath(cands, winslash = "/", mustWork = FALSE))
  for (p in cands) if (!is.na(p) && nzchar(p) && file.exists(p)) return(p)
  NA_character_
}

.env.path <- find_env()
if (!is.na(.env.path)) {
  dotenv::load_dot_env(.env.path)   # (pas d'argument override pour certaines versions)
  message("[plumber] .env chargé depuis: ", .env.path)
} else {
  message("[plumber] Aucun .env trouvé (OK si variables Railway déjà définies).")
}

# --- Connexion PG : préfère DATABASE_URL si dispo, sinon variables PG* ---
pg_con <- function() {
  db_url <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(db_url)) {
    # ex: postgres://user:pass@host:port/dbname?sslmode=require
    if (!requireNamespace("httr2", quietly = TRUE)) {
      install.packages("httr2", repos = "https://cloud.r-project.org")
    }
    u <- httr2::url_parse(db_url)
    q <- u$query %||% list()

    return(DBI::dbConnect(
      RPostgres::Postgres(),
      host     = u$hostname %||% Sys.getenv("PGHOST", unset = ""),
      port     = as.integer(u$port %||% Sys.getenv("PGPORT", "5432")),
      dbname   = sub("^/+", "", u$path %||% Sys.getenv("PGDATABASE", unset = "")),
      user     = u$username %||% Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
      password = u$password %||% Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
      sslmode  = q$sslmode %||% Sys.getenv("PGSSLMODE", "prefer")
    ))
  }

  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST", unset = ""),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE", unset = ""),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
    sslmode  = Sys.getenv("PGSSLMODE", "prefer")
  )
}

# --- Rendre le résultat JSON-safe (évite pq_inet / POSIXct qui cassent swagger) ---
json_safe_df <- function(x) {
  if (!is.data.frame(x)) return(x)
  for (nm in names(x)) {
    col <- x[[nm]]
    if (inherits(col, c("POSIXct", "POSIXt", "Date"))) {
      x[[nm]] <- as.character(col)
    } else if (inherits(col, "pq_inet")) {
      x[[nm]] <- as.character(col)
    } else if (is.list(col)) {
      x[[nm]] <- vapply(col, function(z) paste0(z, collapse = ","), character(1))
    }
  }
  x
}

# ---- API Key (désactivée si API_KEY vide) ----
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""

  if (identical(allowed, "") || identical(allowed, got)) {
    forward()
  } else {
    res$setStatus(401L)
    return(list(error = TRUE, message = "Unauthorized: invalid API key"))
  }
}

# ---- CORS ----
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
      res
    }
  ))
  pr
}

# ---- Health ----
#* @get /health
#* @serializer unboxedJSON
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

# ---- Debug env (sans exposer les mots de passe) ----
#* @get /debug/env
#* @serializer unboxedJSON
function() {
  list(
    PGHOST = Sys.getenv("PGHOST"),
    PGPORT = Sys.getenv("PGPORT"),
    PGDATABASE = Sys.getenv("PGDATABASE"),
    PGUSER = Sys.getenv("PGUSER"),
    PGREADUSER = Sys.getenv("PGREADUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGREADPASS_set = nzchar(Sys.getenv("PGREADPASS")),
    PGSSLMODE = Sys.getenv("PGSSLMODE"),
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL")),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN")
  )
}

# ---- Debug ping DB (DOIT tourner) ----
#* @get /debug/pingdb
#* @serializer unboxedJSON
function() {
  # On renvoie un objet clair même en cas d'erreur, pour diagnostiquer Railway
  cfg <- list(
    host   = Sys.getenv("PGHOST", "postgres.railway.internal"),
    port   = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname = Sys.getenv("PGDATABASE", ""),
    user   = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", ""))
  )

  tryCatch({
    con <- pg_con()
    on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

    out <- DBI::dbGetQuery(con, "
      SELECT
        current_database() AS db,
        current_user       AS usr,
        current_schema()   AS schema,
        inet_server_addr() AS server_addr,
        inet_server_port() AS server_port,
        version()          AS version
    ")
    out <- json_safe_df(out)

    list(ok = TRUE, config = cfg, result = out)
  }, error = function(e) {
    list(ok = FALSE, config = cfg, error = TRUE, message = conditionMessage(e))
  })
}

# ---- Export CSV ----
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
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
#* @serializer unboxedJSON
function(q = "") {
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
                          OR indicator_name ILIKE {DBI::dbQuoteString(con, like)})'))
  }

  qry <- paste0(qry, " ORDER BY indicator_code;")
  json_safe_df(DBI::dbGetQuery(con, qry))
}

# ---- JSON paginé ----
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
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

  list(total = total, limit = limit, offset = offset, rows = json_safe_df(rows))
}

# ---- Export XLSX ----
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM "{schema}"."indicator_values"
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

# ---- Metrics (texte) ----
#* @serializer html
#* @get /metrics
function() {
  con <- pg_con(); on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  schema <- Sys.getenv("PGSCHEMA", "public")

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."indicator_values";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."indicator_values";'))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", as.numeric(as.POSIXct(latest$max_ins[[1]])), "\n",
    "onu_api_last_updated_at ",  as.numeric(as.POSIXct(latest$max_upd[[1]])),  "\n"
  )
}