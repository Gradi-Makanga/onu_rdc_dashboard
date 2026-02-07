# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
  library(dplyr)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(as.character(x))) y else x

# -------------------------
# 1) Chargement config (.env en local, DATABASE_URL sur Railway)
# -------------------------
find_env <- function() {
  # ordre: racine du projet, dossier courant, parent
  p1 <- normalizePath(file.path(getwd(), "..", ".env"), mustWork = FALSE)
  p2 <- normalizePath(file.path(getwd(), ".env"), mustWork = FALSE)
  p3 <- normalizePath(file.path(dirname(getwd()), ".env"), mustWork = FALSE)
  for (p in c(p1, p2, p3)) if (file.exists(p)) return(p)
  NA_character_
}

if (nzchar(Sys.getenv("DATABASE_URL", ""))) {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
} else {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    dotenv::load_dot_env(.env.path)  # (pas d'override ici -> compatibilité)
    message("[plumber] .env chargé depuis: ", .env.path)
  } else {
    message("[plumber] Aucun .env trouvé. (OK si variables d'environnement déjà définies)")
  }
}

# -------------------------
# 2) Parse DATABASE_URL
# -------------------------
parse_database_url <- function(u) {
  # ex: postgres://user:pass@host:5432/dbname?sslmode=require
  if (!nzchar(u)) return(NULL)

  u0 <- sub("^postgresql://", "postgres://", u)
  u0 <- sub("^postgres://", "", u0)

  # séparer querystring
  parts <- strsplit(u0, "\\?", fixed = FALSE)[[1]]
  main  <- parts[1]
  qs    <- if (length(parts) > 1) parts[2] else ""

  # userinfo@host:port/db
  # userinfo
  userinfo_host <- strsplit(main, "@", fixed = TRUE)[[1]]
  if (length(userinfo_host) == 2) {
    userinfo <- userinfo_host[1]
    hostdb   <- userinfo_host[2]
  } else {
    userinfo <- ""
    hostdb   <- userinfo_host[1]
  }

  user <- ""
  pass <- ""
  if (nzchar(userinfo)) {
    up <- strsplit(userinfo, ":", fixed = TRUE)[[1]]
    user <- utils::URLdecode(up[1])
    if (length(up) > 1) pass <- utils::URLdecode(paste(up[-1], collapse = ":"))
  }

  host_port_db <- strsplit(hostdb, "/", fixed = TRUE)[[1]]
  hostport <- host_port_db[1]
  dbname   <- if (length(host_port_db) > 1) utils::URLdecode(paste(host_port_db[-1], collapse = "/")) else ""

  hp <- strsplit(hostport, ":", fixed = TRUE)[[1]]
  host <- hp[1]
  port <- if (length(hp) > 1) suppressWarnings(as.integer(hp[2])) else NA_integer_

  # querystring -> list
  q <- list()
  if (nzchar(qs)) {
    kvs <- strsplit(qs, "&", fixed = TRUE)[[1]]
    for (kv in kvs) {
      ab <- strsplit(kv, "=", fixed = TRUE)[[1]]
      k <- utils::URLdecode(ab[1])
      v <- if (length(ab) > 1) utils::URLdecode(paste(ab[-1], collapse = "=")) else ""
      q[[k]] <- v
    }
  }

  list(
    host = host,
    port = if (!is.na(port)) port else NA_integer_,
    dbname = dbname,
    user = user,
    password = pass,
    sslmode = q[["sslmode"]] %||% Sys.getenv("PGSSLMODE", "prefer")
  )
}

# -------------------------
# 3) Connexion Postgres (priorité DATABASE_URL)
# -------------------------
pg_con <- function() {
  dburl <- Sys.getenv("DATABASE_URL", "")

  if (nzchar(dburl)) {
    cfg <- parse_database_url(dburl)
    return(DBI::dbConnect(
      RPostgres::Postgres(),
      host     = cfg$host,
      port     = cfg$port %||% 5432L,
      dbname   = cfg$dbname,
      user     = cfg$user,
      password = cfg$password,
      sslmode  = cfg$sslmode %||% "prefer"
    ))
  }

  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = Sys.getenv("PGHOST"),
    port     = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname   = Sys.getenv("PGDATABASE"),
    user     = Sys.getenv("PGUSER", Sys.getenv("PGREADUSER", "")),
    password = Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS", "")),
    sslmode  = Sys.getenv("PGSSLMODE", "prefer")
  )
}

# -------------------------
# 4) Utilitaires: table/schema auto-detect si besoin
# -------------------------
table_exists <- function(con, schema, table) {
  isTRUE(DBI::dbGetQuery(
    con,
    "SELECT EXISTS (
       SELECT 1
       FROM information_schema.tables
       WHERE table_schema = $1 AND table_name = $2
     ) AS ok;",
    params = list(schema, table)
  )$ok[1])
}

detect_table <- function(con) {
  # 1) si PGTABLE/PGSCHEMA fournis et existent -> OK
  schema0 <- Sys.getenv("PGSCHEMA", "public")
  table0  <- Sys.getenv("PGTABLE",  "indicator_values")
  if (nzchar(schema0) && nzchar(table0) && table_exists(con, schema0, table0)) {
    return(list(schema = schema0, table = table0, mode = "env"))
  }

  # 2) Chercher indicator_values n'importe quel schema
  hit <- DBI::dbGetQuery(
    con,
    "SELECT table_schema, table_name
     FROM information_schema.tables
     WHERE table_name = 'indicator_values'
     ORDER BY CASE WHEN table_schema='public' THEN 0 ELSE 1 END, table_schema
     LIMIT 1;"
  )
  if (nrow(hit) == 1) {
    return(list(schema = hit$table_schema[1], table = hit$table_name[1], mode = "name"))
  }

  # 3) Chercher une table candidate qui a les colonnes minimales attendues
  #    (utile si ta table s'appelle indicator_values_all, etc.)
  cand <- DBI::dbGetQuery(
    con,
    "SELECT c.table_schema, c.table_name
     FROM information_schema.columns c
     WHERE c.column_name IN ('indicator_code','ref_area','period','value')
       AND c.table_schema NOT IN ('pg_catalog','information_schema')
     GROUP BY c.table_schema, c.table_name
     HAVING COUNT(DISTINCT c.column_name) = 4
     ORDER BY CASE WHEN c.table_schema='public' THEN 0 ELSE 1 END, c.table_schema, c.table_name
     LIMIT 1;"
  )
  if (nrow(cand) == 1) {
    return(list(schema = cand$table_schema[1], table = cand$table_name[1], mode = "columns"))
  }

  # 4) Rien trouvé
  list(schema = schema0, table = table0, mode = "not_found")
}

# Convertir les types "exotiques" (inet, etc.) en string pour JSON
json_safe_df <- function(x) {
  if (!is.data.frame(x)) return(x)
  for (nm in names(x)) {
    cls <- class(x[[nm]])
    if (any(grepl("^pq_", cls)) || any(cls %in% c("POSIXct","POSIXt","Date"))) {
      x[[nm]] <- as.character(x[[nm]])
    } else if (is.list(x[[nm]])) {
      x[[nm]] <- vapply(x[[nm]], function(v) paste(v, collapse = ","), character(1))
    }
  }
  x
}

# -------------------------
# 5) Sécurité API_KEY (désactivée si vide) — NE PAS toucher res$status ailleurs
# -------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", "")
  got     <- req$HTTP_X_API_KEY %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = TRUE, message = "Unauthorized: invalid API key")
  }
}

# -------------------------
# 6) CORS (headers corrects)
# -------------------------
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
    }
  ))
  pr
}

# -------------------------
# 7) Endpoints
# -------------------------

#* @get /health
function() {
  list(status = "ok", time = as.character(Sys.time()))
}

#* @get /debug/env
function() {
  list(
    DATABASE_URL_set = nzchar(Sys.getenv("DATABASE_URL", "")),
    PGHOST      = Sys.getenv("PGHOST"),
    PGPORT      = Sys.getenv("PGPORT"),
    PGDATABASE  = Sys.getenv("PGDATABASE"),
    PGUSER      = Sys.getenv("PGUSER"),
    PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
    PGSSLMODE   = Sys.getenv("PGSSLMODE"),
    PGSCHEMA    = Sys.getenv("PGSCHEMA", "public"),
    PGTABLE     = Sys.getenv("PGTABLE", "indicator_values"),
    CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN", "*")
  )
}

#* @get /debug/pingdb
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  cfg <- detect_table(con)

  # ping + info serveur
  rs <- DBI::dbGetQuery(con, "
    SELECT
      current_database() AS db,
      current_user       AS usr,
      current_schema()   AS schema,
      inet_server_addr() AS server_addr,
      inet_server_port() AS server_port,
      version()          AS version;
  ")
  rs <- json_safe_df(rs)

  list(
    ok = TRUE,
    using = list(schema = cfg$schema, table = cfg$table, detected_by = cfg$mode),
    result = rs
  )
}

# ---- Export CSV ----
#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  cfg <- detect_table(con)
  if (identical(cfg$mode, "not_found") && !table_exists(con, cfg$schema, cfg$table)) {
    stop(glue("Table introuvable. Aucune table compatible trouvée. (PGSCHEMA={cfg$schema}, PGTABLE={cfg$table})"))
  }

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{cfg$schema}"."{cfg$table}"
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

  cfg <- detect_table(con)
  if (identical(cfg$mode, "not_found") && !table_exists(con, cfg$schema, cfg$table)) {
    stop(glue("Table introuvable. Aucune table compatible trouvée. (PGSCHEMA={cfg$schema}, PGTABLE={cfg$table})"))
  }

  qry <- glue('
    SELECT DISTINCT indicator_code, indicator_name
    FROM "{cfg$schema}"."{cfg$table}"
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

# ---- JSON paginé ----
#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  cfg <- detect_table(con)
  if (identical(cfg$mode, "not_found") && !table_exists(con, cfg$schema, cfg$table)) {
    stop(glue("Table introuvable. Aucune table compatible trouvée. (PGSCHEMA={cfg$schema}, PGTABLE={cfg$table})"))
  }

  limit  <- max(1L, min(as.integer(limit), 10000L))
  offset <- max(0L, as.integer(offset))

  where <- "WHERE 1=1"
  if (nzchar(indicator_code)) where <- paste0(where, glue(' AND indicator_code = {DBI::dbQuoteString(con, indicator_code)}'))
  if (nzchar(ref_area))       where <- paste0(where, glue(' AND ref_area = {DBI::dbQuoteString(con, ref_area)}'))
  if (!is.na(suppressWarnings(as.numeric(start)))) where <- paste0(where, glue(' AND period >= {as.numeric(start)}'))
  if (!is.na(suppressWarnings(as.numeric(end))))   where <- paste0(where, glue(' AND period <= {as.numeric(end)}'))

  total <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::int AS n FROM "{cfg$schema}"."{cfg$table}" {where};'))$n[1]

  rows <- DBI::dbGetQuery(con, glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source, inserted_at, updated_at
    FROM "{cfg$schema}"."{cfg$table}"
    {where}
    ORDER BY indicator_code, ref_area, period
    LIMIT {limit} OFFSET {offset};
  '))
  rows <- json_safe_df(rows)

  list(total = total, limit = limit, offset = offset, rows = rows)
}

# ---- Export XLSX ----
#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  cfg <- detect_table(con)
  if (identical(cfg$mode, "not_found") && !table_exists(con, cfg$schema, cfg$table)) {
    stop(glue("Table introuvable. Aucune table compatible trouvée. (PGSCHEMA={cfg$schema}, PGTABLE={cfg$table})"))
  }

  qry <- glue('
    SELECT indicator_code, indicator_name, ref_area, period, value, obs_status, source
    FROM "{cfg$schema}"."{cfg$table}"
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
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  cfg <- detect_table(con)
  if (identical(cfg$mode, "not_found") && !table_exists(con, cfg$schema, cfg$table)) {
    stop(glue("Table introuvable. Aucune table compatible trouvée. (PGSCHEMA={cfg$schema}, PGTABLE={cfg$table})"))
  }

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{cfg$schema}"."{cfg$table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{cfg$schema}"."{cfg$table}";'))

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", as.numeric(as.POSIXct(latest$max_ins[[1]])), "\n",
    "onu_api_last_updated_at ",  as.numeric(as.POSIXct(latest$max_upd[[1]])),  "\n"
  )
}