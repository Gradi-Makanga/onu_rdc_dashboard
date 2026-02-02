library(shiny)

Sys.setenv(ONU_API_BASE = Sys.getenv("ONU_API_BASE", "http://127.0.0.1:8000"))
Sys.setenv(API_KEY = Sys.getenv("API_KEY", ""))

shiny::runApp(".", host = "0.0.0.0", port = as.integer(Sys.getenv("PORT", 3838)))
