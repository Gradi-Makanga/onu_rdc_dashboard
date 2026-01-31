# D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/api/run_api.R
setwd("D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/api")
library(plumber)
pr <- plumber::pr("plumber.R")
message("Running plumber API at http://127.0.0.1:", Sys.getenv("API_PORT","8000"))
pr$run(host = "127.0.0.1", port = as.integer(Sys.getenv("API_PORT","8000")))
