# D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/R/create_views.R
library(DBI); library(RPostgres); library(glue); library(dotenv)
dotenv::load_dot_env("D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/.env")
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host=Sys.getenv("PGHOST"), port=as.integer(Sys.getenv("PGPORT","5432")),
  dbname=Sys.getenv("PGDATABASE"), user=Sys.getenv("PGUSER", Sys.getenv("PGREADUSER","")),
  password=Sys.getenv("PGPASSWORD", Sys.getenv("PGREADPASS","")), sslmode=Sys.getenv("PGSSLMODE","prefer")
)
schema <- Sys.getenv("PGSCHEMA","public")

DBI::dbExecute(con, glue('
CREATE OR REPLACE VIEW "{schema}"."indicator_latest_year" AS
WITH last_year AS (
  SELECT indicator_code, ref_area, MAX(period) AS period
  FROM "{schema}"."indicator_values" 
  WHERE period IS NOT NULL
  GROUP BY indicator_code, ref_area
)
SELECT v.*
FROM "{schema}"."indicator_values" v
JOIN last_year y USING (indicator_code, ref_area, period);
'))

DBI::dbExecute(con, glue('
CREATE OR REPLACE VIEW "{schema}"."indicator_catalogue" AS
SELECT DISTINCT indicator_code, indicator_name
FROM "{schema}"."indicator_values"
WHERE indicator_code IS NOT NULL
ORDER BY indicator_code;
'))

DBI::dbDisconnect(con)
