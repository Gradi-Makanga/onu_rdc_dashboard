library(shiny)

# Railway fournit automatiquement le port
port <- as.integer(Sys.getenv("PORT", "3838"))

# Lancer l'application Shiny
runApp(
  appDir = ".",
  host = "0.0.0.0",
  port = port
)