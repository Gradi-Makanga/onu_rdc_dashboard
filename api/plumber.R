# api/plumber.R
suppressPackageStartupMessages({
  library(plumber)
  library(DBI)
  library(RPostgres)
  library(glue)
  library(dotenv)
  library(readr)
  library(dplyr)
})

`%||%` <- function(x, y) if (is.null(x) || is.na(x) || !nzchar(as.character(x))) y else x

# -------------------------
# 1) Charger .env SANS casser Railway
#    - Sur Railway: DATABASE_URL existe => on NE charge PAS .env
#    - En local: on charge .env si présent
# -------------------------
find_env <- function() {
  cands <- c(
    file.path(getwd(), ".env"),
    file.path(getwd(), "..", ".env"),
    file.path(dirname(getwd()), ".env"),
    file.path(getwd(), "api", ".env")
  )
  cands <- unique(normalizePath(cands, winslash = "/", mustWork = FALSE))
  hit <- cands[file.exists(cands)]
  if (length(hit)) hit[[1]] else NA_character_
}

safe_load_env_no_override <- function(path) {
  # Charge uniquement les variables qui ne sont PAS déjà définies dans l'environnement
  if (is.na(path) || !file.exists(path)) return(invisible(FALSE))
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  lines <- trimws(lines)
  lines <- lines[lines != "" & !startsWith(lines, "#")]

  for (ln in lines) {
    # support: KEY=VALUE
    if (!grepl("=", ln, fixed = TRUE)) next
    key <- trimws(sub("=.*$", "", ln))
    val <- sub("^[^=]*=", "", ln)
    val <- trimws(val)

    # enlever guillemets simples/doubles
    if (grepl("^\".*\"$", val) || grepl("^'.*'$", val)) {
      val <- substr(val, 2, nchar(val) - 1)
    }

    if (!nzchar(key)) next
    if (!nzchar(Sys.getenv(key, unset = ""))) {
      Sys.setenv(key = val)
    }
  }
  invisible(TRUE)
}

if (nzchar(Sys.getenv("DATABASE_URL", unset = ""))) {
  message("[plumber] DATABASE_URL détectée => .env ignoré (Railway).")
} else {
  .env.path <- find_env()
  if (!is.na(.env.path)) {
    safe_load_env_no_override(.env.path)
    message("[plumber] .env chargé (sans override) depuis: ", .env.path)
  } else {
    message("[plumber] aucun .env trouvé (ok si variables Railway déjà définies).")
  }
}

# -------------------------
# 2) Parse DATABASE_URL (Railway) -> paramètres Postgres
# -------------------------
parse_database_url <- function(x) {
  # Ex: postgres://user:pass@host:5432/dbname?sslmode=require
  if (!nzchar(x)) return(list())

  # éviter les soucis de caractères spéciaux dans password
  # on récupère grossièrement user/pass/host/port/path/query
  m <- regexec("^(postgres|postgresql)://([^:/?#]+)(:([^@/?#]*))?@([^:/?#]+)(:([0-9]+))?(/([^?#]*))?(\\?(.*))?$", x)
  r <- regmatches(x, m)[[1]]
  if (length(r) == 0) return(list())

  user <- r[3]
  pass <- r[5] %||% ""
  host <- r[6]
  port <- r[8] %||% ""
  dbn  <- r[10] %||% ""
  qstr <- r[12] %||% ""

  qs <- list()
  if (nzchar(qstr)) {
    parts <- strsplit(qstr, "&", fixed = TRUE)[[1]]
    for (p in parts) {
      kv <- strsplit(p, "=", fixed = TRUE)[[1]]
      if (length(kv) >= 1) {
        k <- kv[[1]]
        v <- if (length(kv) >= 2) kv[[2]] else ""
        qs[[k]] <- utils::URLdecode(v)
      }
    }
  }

  list(
    host = host,
    port = if (nzchar(port)) as.integer(port) else 5432L,
    dbname = dbn,
    user = user,
    password = utils::URLdecode(pass),
    sslmode = qs$sslmode %||% ""
  )
}

# -------------------------
# 3) Connexion DB robuste (Railway ou local)
# -------------------------
pg_con <- function() {
  db_url <- Sys.getenv("DATABASE_URL", unset = "")
  if (nzchar(db_url)) {
    p <- parse_database_url(db_url)

    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = p$host,
      port     = p$port %||% 5432L,
      dbname   = p$dbname,
      user     = p$user,
      password = p$password,
      sslmode  = (p$sslmode %||% Sys.getenv("PGSSLMODE", unset = "require")) %||% "require"
    )
  } else {
    # Local / mode .env
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
}

pg_schema <- function() Sys.getenv("PGSCHEMA", unset = "public")
pg_table  <- function() Sys.getenv("PGTABLE",  unset = "indicator_values")

# Helper: table existe ?
table_exists <- function(con, schema, table) {
  q <- "
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1;
  "
  res <- tryCatch(DBI::dbGetQuery(con, q, params = list(schema, table)), error = function(e) NULL)
  is.data.frame(res) && nrow(res) > 0
}

# Helper: rendre JSON-safe (pq_inet, POSIXct, etc.)
json_safe_df <- function(df) {
  if (!is.data.frame(df)) return(df)
  for (nm in names(df)) {
    # pq_inet / pq_* -> character
    if (inherits(df[[nm]], "pq_inet") || inherits(df[[nm]], "pq_byt") || inherits(df[[nm]], "pq_oid")) {
      df[[nm]] <- as.character(df[[nm]])
    }
    if (inherits(df[[nm]], "POSIXct") || inherits(df[[nm]], "POSIXt")) {
      df[[nm]] <- format(df[[nm]], tz = "UTC", usetz = TRUE)
    }
  }
  df
}

# -------------------------
# 4) API Key filter (NE PAS toucher res$status en lecture)
# -------------------------
#* @filter apikey
function(req, res) {
  allowed <- Sys.getenv("API_KEY", unset = "")
  got     <- req$HTTP_X_API_KEY %||% ""

  if (!nzchar(allowed) || identical(allowed, got)) {
    forward()
  } else {
    res$status <- 401
    list(error = TRUE, message = "Unauthorized: invalid API key")
  }
}

# -------------------------
# 5) CORS
# -------------------------
#* @plumber
function(pr) {
  pr$registerHooks(list(
    preroute = function(req, res) {
      allowed_origins <- strsplit(Sys.getenv("CORS_ALLOW_ORIGIN", "*"), ",", fixed = TRUE)[[1]] |> trimws()
      origin <- req$HTTP_ORIGIN %||% "*"
      allow  <- if ("*" %in% allowed_origins || origin %in% allowed_origins) origin else "null"

      res$setHeader("Access-Control-Allow-Origin", allow)
      res$setHeader("Access-Control-Allow-Methods", "GET,OPTIONS")
      res$setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    }
  ))
  pr
}

# -------------------------
# 6) Routes
# -------------------------

#* @get /health
function() {
  list(status = "ok", time = format(Sys.time(), tz = "UTC", usetz = TRUE))
}

#* @get /debug/env
function() {
  # montre ce que l'API UTILISE réellement
  du <- Sys.getenv("DATABASE_URL", unset = "")
  p  <- if (nzchar(du)) parse_database_url(du) else list()

  list(
    DATABASE_URL_set = nzchar(du),
    effective = list(
      host = if (nzchar(du)) p$host else Sys.getenv("PGHOST"),
      port = if (nzchar(du)) p$port else as.integer(Sys.getenv("PGPORT", "5432")),
      dbname = if (nzchar(du)) p$dbname else Sys.getenv("PGDATABASE"),
      user = if (nzchar(du)) p$user else Sys.getenv("PGUSER"),
      sslmode = (p$sslmode %||% Sys.getenv("PGSSLMODE", unset = "")) %||% ""
    ),
    PGSHEMA = pg_schema(),
    PGTABLE = pg_table(),
    raw = list(
      PGHOST = Sys.getenv("PGHOST"),
      PGPORT = Sys.getenv("PGPORT"),
      PGDATABASE = Sys.getenv("PGDATABASE"),
      PGUSER = Sys.getenv("PGUSER"),
      PGPASSWORD_set = nzchar(Sys.getenv("PGPASSWORD")),
      PGSSLMODE = Sys.getenv("PGSSLMODE"),
      CORS_ALLOW_ORIGIN = Sys.getenv("CORS_ALLOW_ORIGIN")
    )
  )
}

#* @get /debug/pingdb
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  # ping + infos serveur
  q <- "SELECT current_database() AS db, current_user AS usr, current_schema() AS schema, inet_server_addr() AS server_addr, inet_server_port() AS server_port, version() AS version;"
  res <- DBI::dbGetQuery(con, q)
  res <- json_safe_df(res)

  list(
    ok = TRUE,
    config = list(
      schema = schema,
      table = table,
      has_table = table_exists(con, schema, table)
    ),
    result = res
  )
}

#* @serializer csv
#* @get /export/csv
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}. Vérifie PGSCHEMA/PGTABLE ou le chargement des données."))
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

#* @get /indicators
function(q = "") {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}."))
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

#* @serializer unboxedJSON
#* @get /values
function(indicator_code = "", ref_area = "", start = NA, end = NA, limit = 1000, offset = 0) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}."))
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
  '))
  rows <- json_safe_df(rows)

  list(total = total, limit = limit, offset = offset, rows = rows)
}

#* @serializer contentType list(type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
#* @get /export/xlsx
function(indicator_code = "", ref_area = "", start = NA, end = NA) {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    stop(glue("Table introuvable: {schema}.{table}."))
  }

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
  tf <- tempfile(fileext = ".xlsx")

  if (!requireNamespace("writexl", quietly = TRUE)) {
    install.packages("writexl", repos = "https://cloud.r-project.org")
  }
  writexl::write_xlsx(dat, tf)
  plumber::include_file(tf)
}

#* @serializer html
#* @get /metrics
function() {
  con <- pg_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

  schema <- pg_schema()
  table  <- pg_table()

  if (!table_exists(con, schema, table)) {
    return(paste0("onu_api_rows_total 0\n"))
  }

  n <- DBI::dbGetQuery(con, glue('SELECT COUNT(*)::bigint AS n FROM "{schema}"."{table}";'))$n[1]
  latest <- DBI::dbGetQuery(con, glue('SELECT max(inserted_at) AS max_ins, max(updated_at) AS max_upd FROM "{schema}"."{table}";'))

  max_ins <- latest$max_ins[[1]]
  max_upd <- latest$max_upd[[1]]

  paste0(
    "onu_api_rows_total ", n, "\n",
    "onu_api_last_inserted_at ", if (!is.null(max_ins) && !is.na(max_ins)) as.numeric(as.POSIXct(max_ins)) else "NaN", "\n",
    "onu_api_last_updated_at ",  if (!is.null(max_upd) && !is.na(max_upd)) as.numeric(as.POSIXct(max_upd)) else "NaN", "\n"
  )
} 