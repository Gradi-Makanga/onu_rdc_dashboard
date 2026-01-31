# D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/etl/run_etl.R
setwd("D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard")
required <- c("targets","dotenv")
for (p in required) if (!requireNamespace(p, quietly = TRUE)) install.packages(p)
dotenv::load_dot_env(".env")
targets::tar_make()
cat(sprintf("[ETL OK] %s\n", Sys.time()), file = "etl/etl_run.log", append = TRUE)
