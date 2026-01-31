
library(shiny); library(DBI); library(RPostgres); library(dplyr); library(ggplot2)

pg_con <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("PGHOST"),
    port = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname = Sys.getenv("PGDATABASE"),
    user = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode = Sys.getenv("PGSSLMODE","require")
  )
}

ui <- fluidPage(
  titlePanel("ONU RDC — Indicateurs"),
  sidebarLayout(
    sidebarPanel(
      textInput("ind", "Code indicateur", value = ""),
      actionButton("refresh","Rafraîchir")
    ),
    mainPanel(
      tableOutput("tbl"),
      plotOutput("plt"),
      tags$hr(),
      h4("Téléchargements"),
      tags$ul(
        tags$li(tags$a(href="/api/download/csv", "Dernier export (CSV)")),
        tags$li(tags$a(href="/api/download/parquet", "Dernier export (Parquet)"))
      )
    )
  )
)

server <- function(input, output, session){
  df <- reactiveVal(data.frame())
  observeEvent(input$refresh, {
    con <- pg_con()
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    qry <- "SELECT * FROM datapoints LIMIT 100"
    if (nzchar(input$ind)) qry <- sprintf(
      "SELECT d.* FROM datapoints d JOIN series_meta s ON d.series_id=s.series_id WHERE s.indicator_code='%s' LIMIT 100", input$ind
    )
    df(DBI::dbGetQuery(con, qry))
  })
  output$tbl <- renderTable(df())
  output$plt <- renderPlot({
    d <- df()
    if (nrow(d)) ggplot(d, aes(x = period, y = value, group = series_id)) + geom_line() + theme_minimal()
  })
}

shinyApp(ui, server)
