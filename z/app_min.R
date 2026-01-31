library(shiny)
ui <- fluidPage(titlePanel('Test parse OK'), verbatimTextOutput('x'))
server <- function(input, output, session){ output$x <- renderText({ paste('OK', Sys.time()) }) }
shinyApp(ui, server)