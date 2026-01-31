# ---------- ONU RDC - Indicateurs (nom, source, obs, période) ----------
# Fichier: app.R
suppressPackageStartupMessages({
  library(shiny); library(DT); library(httr2); library(jsonlite)
  library(readr); library(dplyr); library(ggplot2); library(leaflet)
})

API_BASE <- Sys.getenv("ONU_API_BASE", "http://127.0.0.1:8000")
API_KEY  <- Sys.getenv("API_KEY", "")

# ---------- Helpers encodage / utilitaires ----------
nz_chr1 <- function(x) is.character(x) && length(x) > 0 && nzchar(x[1])

._cp1252_map <- c(
  `85`="\u2026", `91`="\u2018", `92`="\u2019", `93`="\u201C", `94`="\u201D",
  `95`="\u2022", `96`="\u2013", `97`="\u2014", `98`="\u02DC", `99`="\u2122",
  `8A`="\u0160", `8C`="\u0152", `8E`="\u017D", `9A`="\u0161", `9C`="\u0153",
  `9E`="\u017E", `9F`="\u0178"
)

decode_angle_hex_once <- function(s){
  if (!length(s)) return(s)
  x <- as.character(s)
  m <- gregexpr("<([0-9a-fA-F]{2})>", x, perl = TRUE)
  if (all(m[[1]] == -1)) return(x)
  hexs <- gsub("^<|>$", "", unlist(regmatches(x, m)))
  repl <- vapply(hexs, function(h){
    H <- toupper(h)
    if (H %in% names(._cp1252_map)) ._cp1252_map[[H]] else intToUtf8(strtoi(H, 16L))
  }, character(1))
  regmatches(x, m) <- list(repl)
  x
}

fix_utf8 <- function(x){
  x <- as.character(x)
  if (any(grepl("<[0-9a-fA-F]{2}>", x, perl = TRUE)))
    x <- vapply(x, decode_angle_hex_once, "", USE.NAMES = FALSE)
  bad <- grepl("<[0-9a-fA-F]{2}>", iconv(x, "", "UTF-8", sub = "byte"), perl = TRUE)
  if (any(bad)) {
    y <- try(iconv(x, from = "latin1", to = "UTF-8"), silent = TRUE)
    if (!inherits(y, "try-error")) x <- y
  }
  x
}

collapse_param <- function(v) {
  v <- v[!is.na(v)]
  v <- unique(as.character(v))
  if (!length(v)) return(NULL)
  paste(v, collapse = ",")
}

api_build_url <- function(path, params = list()){
  base <- paste0(API_BASE, path)
  params <- modifyList(params, list(
    source = if (!is.null(params$source)) collapse_param(params$source) else NULL,
    obs    = if (!is.null(params$obs))    collapse_param(params$obs)    else NULL
  ))
  ks <- names(params)
  if (!length(ks)) return(base)
  vs <- unlist(params, use.names = FALSE)
  enc <- vapply(as.character(vs), function(z) utils::URLencode(z, reserved = TRUE), "")
  paste0(base, "?", paste(paste0(ks, "=", enc), collapse = "&"))
}

api_get_text <- function(path, params=list(), timeout=90){
  url <- api_build_url(path, params)
  req <- request(url)
  if (nzchar(API_KEY)) req <- req_headers(req, "X-API-Key" = API_KEY)
  req <- req_timeout(req, timeout)
  resp <- req_perform(req)
  if (resp_status(resp) >= 400) stop(paste("HTTP", resp_status(resp), "sur", url))
  txt <- httr2::resp_body_string(resp)
  if (!is.character(txt)) txt <- as.character(txt)
  if (length(txt) != 1L) txt <- paste(txt, collapse = "\n")
  txt
}
api_get_csv <- function(path, params=list(), timeout=90){
  txt <- api_get_text(path, params, timeout)
  readr::read_csv(I(as.character(txt)[1]), show_col_types = FALSE)
}
api_get_json <- function(path, params=list(), timeout=60){
  txt <- api_get_text(path, params, timeout)
  jsonlite::fromJSON(as.character(txt)[1])
}

fetch_indicators <- function(query = ""){
  params <- list()
  if (nzchar(query)) params$q <- query
  obj <- try(api_get_json("/indicators", params), silent = TRUE)
  if (inherits(obj, "try-error") || is.null(obj$indicator_code))
    return(tibble(indicator_code = character(0), indicator_name = character(0)))
  tibble(
    indicator_code = as.character(obj$indicator_code),
    indicator_name = fix_utf8(as.character(obj$indicator_name))
  ) |>
    distinct() |>
    arrange(indicator_name)
}

# ---------- UI ----------
ui <- fluidPage(
  # ---- CSS pour fond et style global ----
  tags$head(
    tags$style(HTML("
      body {
        background-color: #e6f0fa; /* bleu clair */
      }
      .title-panel {
        display: flex;
        align-items: center;
        background-color: #004080; /* bleu ONU */
        color: white;
        padding: 10px 20px;
        border-radius: 6px;
        margin-bottom: 15px;
      }
      .title-panel img {
        height: 60px;
        margin-right: 15px;
      }
      .title-panel h2 {
        font-weight: bold;
        color: white;
        margin: 0;
      }
      .well {
        background-color: #f5f8fc;
        border: 1px solid #cfe0f5;
      }
      .btn-primary {
        background-color: #004080 !important;
        border-color: #003366 !important;
      }
      .btn-primary:hover {
        background-color: #0059b3 !important;
      }
    "))
  ),

  # ---- En-tête avec logo ONU + titre ----
  div(class = "title-panel",
      img(src = "rco_logo.jpg", alt = "Logo ONU"),
      h2("ONU RDC - Indicateurs ODD et Socioéconomiques")
  ),

  # ---- Disposition principale ----
  sidebarLayout(
    sidebarPanel(
      selectizeInput("ind_name","Indicateur (nom)", choices=NULL, multiple=FALSE,
                     options=list(placeholder="Tapez quelques lettres...")),
      selectizeInput("source","Source", choices=NULL, multiple=TRUE),
      selectInput("obs","Statut obs", choices=c("", "TRUE", "FALSE"), selected=""),
      sliderInput("years","Période", min = 1960, max = 2035, value = c(2000, 2025), sep = ""),
      actionButton("go","Appliquer", class="btn-primary"),
      tags$hr(),
      downloadButton("dl_csv","Télécharger CSV"),
      downloadButton("dl_xlsx","Télécharger XLSX"),
      tags$hr(), tags$small(span("API_BASE: ", code(API_BASE))),
      tags$br(), tags$small("Dernière requête :"), verbatimTextOutput("last_url", placeholder=TRUE),
      tags$small("Erreur API :"), verbatimTextOutput("last_err", placeholder=TRUE)
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Tableau", h4(textOutput("subtitle")), DTOutput("tbl")),
        tabPanel("Graphique",
          checkboxInput("chart_points","Afficher les points", TRUE),
          plotOutput("plot", height=460)
        ),
        tabPanel("Carte",
          leafletOutput("map", height=520),
          tags$small("Note : couche géo en cours d’intégration.")
        )
      )
    )
  )
)


# ---------- SERVER ----------
server <- function(input, output, session){

  ind_cat <- reactiveVal(tibble(indicator_code=character(0), indicator_name=character(0)))
  observe({
    df <- fetch_indicators("")
    ind_cat(df)
    updateSelectizeInput(session, "ind_name", choices = df$indicator_name, server = TRUE)
  })

  observeEvent(input$ind_name, ignoreInit = TRUE, {
    cur <- isolate(ind_cat())
    typed <- input$ind_name
    if (isTruthy(typed) && (!nrow(cur) || !(typed %in% cur$indicator_name))) {
      if (nchar(typed) >= 2) {
        df <- fetch_indicators(typed)
        all_df <- bind_rows(cur, df) |>
          distinct(indicator_code, indicator_name, .keep_all = TRUE) |>
          arrange(indicator_name)
        ind_cat(all_df)
        updateSelectizeInput(session, "ind_name",
                             choices = all_df$indicator_name,
                             selected = typed, server = TRUE)
      }
    }
  })

  observeEvent(input$ind_name, ignoreInit=TRUE, {
    df <- ind_cat(); if (nrow(df)==0 || !nz_chr1(input$ind_name)) return(NULL)
    code <- df$indicator_code[match(input$ind_name, df$indicator_name)]
    if (!nz_chr1(code)) return(NULL)
    yrs <- isolate(input$years)
    dat <- try(api_get_csv("/export/csv",
                           list(indicator_code = code, start = yrs[1], end = yrs[2])),
               silent=TRUE)
    if (inherits(dat,"try-error") || !is.data.frame(dat) || !nrow(dat)) {
      updateSelectizeInput(session, "source", choices = character(0), selected = character(0), server = TRUE)
      return(NULL)
    }
    src_choices <- sort(unique(na.omit(as.character(dat$source))))
    keep_sel   <- intersect(isolate(input$source), src_choices)
    updateSelectizeInput(session, "source", choices = src_choices, selected = keep_sel, server = TRUE)
  }, priority=10)

  output$subtitle <- renderText({
    nm <- if (nz_chr1(input$ind_name)) input$ind_name else "NA"
    srcs <- if (length(input$source)) paste(input$source, collapse=",") else "Toutes"
    yrs  <- paste0("Période ", input$years[1], " - ", input$years[2])
    paste("Résultats pour", nm, "|", yrs, "| Sources:", srcs)
  })

  last_url <- reactiveVal(""); last_err <- reactiveVal("")
  output$last_url <- renderText({ last_url() })
  output$last_err <- renderText({ last_err() })

  make_params <- reactive({
    df <- ind_cat()
    code <- if (nrow(df) && nz_chr1(input$ind_name))
      df$indicator_code[match(input$ind_name, df$indicator_name)] else NULL
    list(
      indicator_code = code,
      source         = if (length(input$source)) input$source else NULL,
      obs            = if (nz_chr1(input$obs) && input$obs!="") input$obs else NULL,
      start          = input$years[1],
      end            = input$years[2]
    )
  })

  fetch_values <- function(){
    p <- make_params()
    url <- api_build_url("/export/csv", p)
    last_url(url); last_err("")
    out <- try(api_get_csv("/export/csv", p), silent=TRUE)
    if (inherits(out,"try-error")) {
      last_err(as.character(attr(out,"condition")$message))
      return(tibble())
    }
    if (!is.data.frame(out) || !nrow(out)) return(tibble())
    out |>
      mutate(
        period         = suppressWarnings(as.integer(period)),
        value          = suppressWarnings(as.numeric(value)),
        indicator_name = fix_utf8(indicator_name),
        source         = as.character(source),
        obs_status     = if ("obs_status" %in% names(out)) out$obs_status else NA
      ) |>
      arrange(ref_area, period)
  }

  data_rx <- eventReactive(input$go, fetch_values(), ignoreInit=TRUE)

  # ----- TABLEAU : affichage "NA" visuel (sans toucher aux données brutes) -----
  output$tbl <- renderDT({
    dat <- data_rx()
    if (!is.data.frame(dat) || !nrow(dat)) return(DT::datatable(data.frame()))
    dat_disp <- dat |>
      mutate(across(everything(), ~ ifelse(is.na(.), "NA", as.character(.))))
    DT::datatable(dat_disp, options=list(pageLength=10, scrollX=TRUE))
  })

  # ----- GRAPHIQUE -----
output$plot <- renderPlot({
  dat0 <- data_rx()

  # Garde-fous
  if (!is.data.frame(dat0) || nrow(dat0) == 0) {
    plot.new(); text(0.5, 0.5, "Aucune donnée à tracer.", cex = 1.1); return(invisible())
  }

  dat <- dat0 |>
    mutate(
      period = suppressWarnings(as.integer(period)),
      value  = suppressWarnings(as.numeric(value)),
      source = as.character(source)
    ) |>
    filter(!is.na(period))

  # ➜ Ne garder que les années où une valeur existe
  dat_plot <- dat |> filter(!is.na(value))

  if (nrow(dat_plot) == 0) {
    plot.new(); text(0.5, 0.5, "Aucune valeur disponible pour les années sélectionnées.", cex = 1.1); return(invisible())
  }

  years_with_data <- sort(unique(dat_plot$period))

  tryCatch({
    ggplot(dat_plot, aes(x = .data$period, y = .data$value, color = .data$source, group = .data$source)) +
      geom_line(linewidth = 0.8) +
      { if (isTRUE(input$chart_points)) geom_point() } +
      scale_x_continuous(breaks = years_with_data, limits = range(years_with_data)) +
      labs(x = "Année (valeurs disponibles uniquement)", y = "Valeur", color = "Source") +
      theme_minimal(base_size = 12) |>
      print()
  }, error = function(e){
    plot.new(); text(0.5, 0.5, paste("Erreur graphique :", conditionMessage(e)), cex = 1.05)
  })
})



  # ----- CARTE (placeholder) -----
  output$map <- renderLeaflet({
    dat <- data_rx()
    if (!is.data.frame(dat) || !nrow(dat)) {
      leaflet() |> addProviderTiles("CartoDB.Positron")
    } else {
      leaflet() |>
        addProviderTiles("CartoDB.Positron") |>
        addLegend("bottomright", colors="white", labels="(géométrie à intégrer)")
    }
  })

  # ----- TÉLÉCHARGEMENTS (NA préservés) -----
  output$dl_csv <- downloadHandler(
    filename = function(){
      nm <- if (nz_chr1(input$ind_name)) gsub("[^A-Za-z0-9_-]+","_", input$ind_name) else "export"
      src <- if (length(input$source)) paste(input$source, collapse="-") else "ALLSRC"
      paste0(nm, "_", src, "_", input$years[1], "-", input$years[2], ".csv")
    },
    content = function(file){
      p <- make_params()
      txt <- api_get_text("/export/csv", p, 120)
      writeBin(charToRaw(as.character(txt)[1]), file)
    }
  )

  output$dl_xlsx <- downloadHandler(
    filename = function(){
      nm <- if (nz_chr1(input$ind_name)) gsub("[^A-Za-z0-9_-]+","_", input$ind_name) else "export"
      src <- if (length(input$source)) paste(input$source, collapse="-") else "ALLSRC"
      paste0(nm, "_", src, "_", input$years[1], "-", input$years[2], ".xlsx")
    },
    content = function(file){
      p <- make_params(); url <- api_build_url("/export/xlsx", p)
      req <- request(url); if (nzchar(API_KEY)) req <- req_headers(req, "X-API-Key"=API_KEY)
      req <- req_timeout(req, 120); resp <- req_perform(req)
      if (resp_status(resp) >= 400) stop(paste("HTTP XLSX", resp_status(resp)))
      writeBin(resp_body_raw(resp), file)
    }
  )
}

shinyApp(ui, server)
