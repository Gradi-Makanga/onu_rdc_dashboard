setwd('D:/Documents/Poste_UN/Projet_Automatisation/onu_rdc_dashboard/ui')

#for (p in c('shiny','httr2','readr','DT')) if (!requireNamespace(p, quietly = TRUE)) install.packages(p)

Sys.setenv(ONU_API_BASE = 'http://127.0.0.1:8000')
Sys.setenv(API_KEY = '')

shiny::runApp('app.R', host = '127.0.0.1', port = 8030, launch.browser = TRUE)

 