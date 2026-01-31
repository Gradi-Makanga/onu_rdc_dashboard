# ---------- ONU RDC - Indicateurs (nom, source, obs, période) ----------
# Fichier: app.R
suppressPackageStartupMessages({
  library(shiny); library(DT); library(httr2); library(jsonlite)
  library(readr); library(dplyr); library(ggplot2); library(leaflet)
  library(sf)
  library(stringi)
  library(htmltools)
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

pretty_source <- function(x){
  x <- as.character(x)
  lab <- x
  lab[lab %in% c("worldbank","wb")] <- "World Bank"
  lab[lab %in% c("who")]            <- "WHO"
  lab[lab %in% c("unicef")]         <- "UNICEF"
  lab[lab %in% c("undp")]           <- "UNDP"
  lab[lab %in% c("ilo")]            <- "ILO"
  lab[lab %in% c("imf")]            <- "IMF"
  lab[lab %in% c("faostat","fao")]  <- "FAO"
  lab[lab %in% c("unsd","unsdg")]   <- "UNSD"
  lab[lab %in% c("unesco")]         <- "UNESCO"
  lab[lab %in% c("monusco")]        <- "MONUSCO"
  lab <- gsub("_", " ", lab)
  lab <- tools::toTitleCase(lab)
  lab
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
  ks <- names(params); if (!length(ks)) return(base)
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
  httr2::resp_body_string(resp)
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
  ) |> distinct() |> arrange(indicator_name)
}

# helper pour l’axe des années
year_breaks <- function(yrs) {
  yrs <- sort(unique(as.integer(yrs)))
  if (!length(yrs)) return(yrs)
  if (length(yrs) <= 12) return(yrs)
  b5 <- yrs[yrs %% 5 == 0]
  if (length(b5) >= 4) return(b5)
  yrs
}

# ---------- Groupes ODD & socio-économiques ----------
ODD_GROUPS <- list(
  odd1  = c("IND001","IND002","IND003","IND004","IND135","IND136","IND137","IND138","IND139","IND140"),
  odd2  = c("IND006","IND007","IND008","IND009","IND010","IND012"),
  odd3  = c("IND011","IND013","IND014","IND015","IND016","IND017","IND018","IND019","IND020","IND021",
            "IND022","IND023","IND024","IND025","IND026","IND027","IND028","IND029","IND030","IND031",
            "IND032","IND033","IND034","IND035","IND036"),
  odd4  = c("IND037","IND038","IND039","IND040","IND041","IND042","IND043","IND044","IND045","IND046",
            "IND047","IND131","IND132","IND133","IND134"),
  odd5  = c("IND048","IND049","IND050","IND051","IND052"),
  odd6  = c("IND005"),
  odd7  = c("IND053","IND054","IND055","IND056","IND057","IND058"),
  odd8  = c("IND059","IND060","IND061","IND062","IND063"),
  odd9  = c("IND070","IND072","IND073"),
  odd10 = c("IND074"),
  odd11 = c("IND075","IND076","IND077","IND130"),
  odd12 = character(0),
  odd13 = character(0),
  odd14 = character(0),
  odd15 = character(0),
  odd16 = c("IND078","IND079","IND080","IND081","IND082","IND083"),
  odd17 = c("IND088","IND089","IND090","IND091","IND092","IND144","IND145","IND146","IND147","IND148","IND149","IND150",
"IND151","IND152","IND153","IND154","IND155","IND156","IND157","IND158","IND159","IND160","IND161","IND162")
)

SOCIO_CODES <- c(
  "IND093","IND094","IND095","IND096","IND097","IND098","IND099","IND100","IND101","IND102",
  "IND103","IND104","IND105","IND106","IND107","IND108","IND109","IND110","IND111","IND112",
  "IND113","IND114","IND115","IND116","IND117","IND118","IND119","IND120","IND121","IND122",
  "IND123","IND124","IND125","IND126","IND127","IND128","IND129",
  "IND064","IND065","IND066","IND067","IND068","IND069",
  "IND084","IND085","IND086","IND087","IND141","IND142","IND143","IND144","IND053",
  "IND054","IND055","IND061","IND062","IND063","IND060","IND088","IND089","IND090","IND091","IND092"
)  

# ---------- UI ----------
ui <- fluidPage(
  tags$head(tags$style(HTML("
:root {
  --onu-blue: #0057b7;
  --onu-blue-light: #4aa3ff;
  --bg-light: #eaf3ff;
  --bg-panel: rgba(255, 255, 255, 0.9);
  --bg-input: rgba(255, 255, 255, 0.95);
  --text-dark: #0d203d;
  --text-soft: #364f70;
  --border: #c4d8f2;
  --shadow: 0 6px 20px rgba(0, 50, 100, 0.15);
}
html, body {
  background: linear-gradient(135deg, #f6fbff 0%, #d9e9ff 100%);
  color: var(--text-dark);
  font-family: 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
}
.title-panel {
  display: flex; align-items: center; justify-content: space-between;
  background: linear-gradient(90deg, var(--onu-blue), var(--onu-blue-light));
  border-radius: 14px;
  padding: 14px 22px; color: white;
  box-shadow: var(--shadow);
  position: sticky;
  top: 0;
  z-index: 999;
}
.title-left { display: flex; align-items: center; }
.title-left img { height: 60px; margin-right: 16px; }
.title-left h2 { font-weight: 700; margin: 0; color: #fff; }
.title-right img { height: 50px; margin-left: 14px; border-radius: 6px; }
.well, .panel, .sidebarPanel, .tab-content {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 14px;
  box-shadow: var(--shadow);
  color: var(--text-dark);
}
.tab-content { padding: 12px; }
.sidebarPanel {
  position: sticky;
  top: 90px;
  max-height: calc(100vh - 110px);
  overflow-y: auto;
}
@keyframes neonPulse {
  0%   { box-shadow: 0 0 .55rem rgba(0, 120, 255, .40),
                 0 0 1.2rem rgba(0, 120, 255, .28),
                 0 0 2.2rem rgba(0, 120, 255, .18); }
  50%  { box-shadow: 0 0 .9rem  rgba(0, 140, 255, .60),
                 0 0 1.9rem rgba(0, 140, 255, .42),
                 0 0 3.0rem rgba(0, 140, 255, .26); }
  100% { box-shadow: 0 0 .55rem rgba(0, 120, 255, .40),
                 0 0 1.2rem rgba(0, 120, 255, .28),
                 0 0 2.2rem rgba(0, 120, 255, .18); }
}
.btn-primary {
  background: linear-gradient(90deg, var(--onu-blue), var(--onu-blue-light)) !important;
  color: #fff !important;
  border: none !important;
  border-radius: 12px;
  font-weight: 700;
  letter-spacing: .2px;
  box-shadow: 0 0 0 rgba(0,87,183,0);
  transition: box-shadow .25s ease, transform .06s ease, filter .2s ease;
}
.btn-primary:hover,
.btn-primary:focus {
  transform: translateY(-1px);
  filter: brightness(1.04);
  animation: neonPulse 1.6s ease-in-out infinite alternate;
}
.btn-primary:active {
  transform: translateY(0);
  animation: none;
  box-shadow: 0 0 .4rem rgba(0, 110, 230, .35);
}
@media (prefers-reduced-motion: reduce) {
  .btn-primary:hover, .btn-primary:focus { animation: none; }
}
.btn, .btn-default {
  background: var(--bg-input); border: 1px solid var(--border);
  color: var(--text-dark); border-radius: 10px;
}
.btn:hover { background: #f0f6ff; }
.selectize-control .selectize-input {
  min-height: 40px; background: var(--bg-input);
  border: 1px solid var(--border); border-radius: 10px;
  color: var(--text-dark); font-weight: 500;
}
.selectize-control .selectize-input.focus {
  border-color: var(--onu-blue); box-shadow: 0 0 4px var(--onu-blue-light);
}
.selectize-control .selectize-dropdown {
  background: white; color: var(--text-dark);
  border: 1px solid var(--border);
}
.irs--shiny .irs-bar,
.irs--shiny .irs-from, .irs--shiny .irs-to, .irs--shiny .irs-single {
  background: linear-gradient(90deg, var(--onu-blue), var(--onu-blue-light)) !important;
  color: white !important;
}
.irs--shiny .irs-handle {
  background: white !important; border: 2px solid var(--onu-blue) !important;
}
.nav-tabs>li>a { color: var(--text-soft); font-weight: 500; }
.nav-tabs>li.active>a {
  color: var(--onu-blue); background: #fff;
  border: 1px solid var(--border); border-radius: 10px;
}
table.dataTable thead th {
  background: var(--onu-blue); color: white;
  border-bottom: 2px solid var(--onu-blue-light);
}
table.dataTable tbody td { color: var(--text-dark); }
table.dataTable tbody tr:nth-child(odd) { background-color: #f5f9ff !important; }
table.dataTable tbody tr:nth-child(even){ background-color: #ffffff !important; }
table.dataTable tbody tr:hover { background-color: #eaf2ff !important; }
.dataTables_wrapper .dataTables_paginate .paginate_button.current {
  background: var(--onu-blue) !important; color: #fff !important; border-radius: 6px;
}
.leaflet-container { border-radius: 14px; box-shadow: var(--shadow); }
.btn-landing {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0 !important;
}
.btn-landing img {
  max-width: 100%;
  height: auto;
  border-radius: 14px;
  box-shadow: var(--shadow);
}
.btn-landing:hover img {
  transform: translateY(-2px);
  transition: transform 0.18s ease-out;
}
  "))),


  div(class = "title-panel",
      div(class="title-left",
          img(src = "rco_logo.jpg", alt = "Logo ONU"),
          h2("ONU RDC - Indicateurs ODD et Socioéconomiques")
      ),
      div(class="title-right",
          img(src = "odd.JPG",   alt = "ODD"),
          img(src = "socio.PNG", alt = "Socioéconomique")
      )
  ),

  uiOutput("body_ui")
) 

indicator_tabs <- function(){
  tabsetPanel(
    tabPanel(
      "Tableau",
      h4(textOutput("subtitle")),
      DTOutput("tbl")
    ),
    tabPanel(
      "Graphique",
      fluidRow(
        column(4, checkboxInput("chart_points","Afficher les points", TRUE)),
        column(8, uiOutput("compare_ui"))
      ),
      plotOutput("plot", height=460)
    ),
    tabPanel(
      "Carte",
      uiOutput("map_year_ui"),
      leafletOutput("map", height=520),
      tags$small("Zones sous-nationales colorées, valeurs au centroïde.")
    )
  )
}

# ---------- SERVER ----------
server <- function(input, output, session){

  app_state <- reactiveValues(mode = "home", odd = NULL)
  ind_cat   <- reactiveVal(tibble(indicator_code=character(0), indicator_name=character(0)))

  code_filter <- reactive({
    mode <- app_state$mode
    odd  <- app_state$odd
    if (identical(mode, "odd_indi") && !is.null(odd)) {
      ODD_GROUPS[[odd]]
    } else if (identical(mode, "socio")) {
      SOCIO_CODES
    } else NULL
  })

  observeEvent(input$home_odd,  { app_state$mode <- "odd_select"; app_state$odd <- NULL })
  observeEvent(input$home_socio,{ app_state$mode <- "socio";      app_state$odd <- NULL })
  observeEvent(input$back_home1,{ app_state$mode <- "home";       app_state$odd <- NULL }, ignoreInit=TRUE)
  observeEvent(input$back_odds, { app_state$mode <- "odd_select"; app_state$odd <- NULL }, ignoreInit=TRUE)

  for (k in 1:17) {
    local({
      idx <- k
      btn_id <- paste0("odd", idx, "_btn")
      odd_id <- paste0("odd", idx)
      observeEvent(input[[btn_id]], {
        app_state$mode <- "odd_indi"
        app_state$odd  <- odd_id
      }, ignoreInit = TRUE)
    })
  }

  output$body_ui <- renderUI({
    mode <- app_state$mode
    odd  <- app_state$odd

    if (identical(mode, "home")) {
      fluidRow(
        column(
          12,
          div(
            style = "margin-top:24px; text-align:center;",
            h2("Bienvenue sur le tableau de bord des indicateurs ONU RDC"),
            p(
              "Explorez les Objectifs de Développement Durable et les indicateurs socio-économiques pour la République Démocratique du Congo.",
              style = "font-size:16px; margin-bottom:6px;"
            ),
            p(
              "Cliquez sur l’un des blocs ci-dessous pour commencer l’exploration des données.",
              style = "font-size:15px; color:#364f70;"
            )
          )
        ),
        column(
          6,
          div(style = "text-align:center; margin-top:20px;",
              actionButton(
                "home_odd",
                label = tags$div(
                  tags$img(src = "odd.jpg",   alt = "Indicateurs ODD"),
                  tags$div("Indicateurs ODD",
                           style="margin-top:8px; font-weight:700; font-size:16px; color:#0d203d;")
                ),
                class = "btn-landing"
              )
          )
        ),
        column(
          6,
          div(style = "text-align:center; margin-top:20px;",
              actionButton(
                "home_socio",
                label = tags$div(
                  tags$img(src = "socio.png", alt = "Indicateurs socio-économiques"),
                  tags$div("Indicateurs socio-économiques",
                           style="margin-top:8px; font-weight:700; font-size:16px; color:#0d203d;")
                ),
                class = "btn-landing"
              )
          )
        )
      )
    } else if (identical(mode, "odd_select")) {
      tagList(
        actionButton("back_home1", "Retour à l'accueil", class = "btn btn-default"),
        tags$br(), tags$br(),
        tags$h3("Sélectionner un Objectif de Développement Durable"),
        fluidRow(
          column(3, actionButton("odd1_btn",  label = tags$img(src="odd1.png",  alt = "ODD 1"),  class="btn-landing")),
          column(3, actionButton("odd2_btn",  label = tags$img(src="odd2.png",  alt = "ODD 2"),  class="btn-landing")),
          column(3, actionButton("odd3_btn",  label = tags$img(src="odd3.png",  alt = "ODD 3"),  class="btn-landing")),
          column(3, actionButton("odd4_btn",  label = tags$img(src="odd4.png",  alt = "ODD 4"),  class="btn-landing"))
        ),
        fluidRow(
          column(3, actionButton("odd5_btn",  label = tags$img(src="odd5.png",  alt = "ODD 5"),  class="btn-landing")),
          column(3, actionButton("odd6_btn",  label = tags$img(src="odd6.png",  alt = "ODD 6"),  class="btn-landing")),
          column(3, actionButton("odd7_btn",  label = tags$img(src="odd7.png",  alt = "ODD 7"),  class="btn-landing")),
          column(3, actionButton("odd8_btn",  label = tags$img(src="odd8.png",  alt = "ODD 8"),  class="btn-landing"))
        ),
        fluidRow(
          column(3, actionButton("odd9_btn",  label = tags$img(src="odd9.png",  alt = "ODD 9"),  class="btn-landing")),
          column(3, actionButton("odd10_btn", label = tags$img(src="odd10.png", alt = "ODD 10"), class="btn-landing")),
          column(3, actionButton("odd11_btn", label = tags$img(src="odd11.png", alt = "ODD 11"), class="btn-landing")),
          column(3, actionButton("odd12_btn", label = tags$img(src="odd12.png", alt = "ODD 12"), class="btn-landing"))
        ),
        fluidRow(
          column(3, actionButton("odd13_btn", label = tags$img(src="odd13.png", alt = "ODD 13"), class="btn-landing")),
          column(3, actionButton("odd14_btn", label = tags$img(src="odd14.png", alt = "ODD 14"), class="btn-landing")),
          column(3, actionButton("odd15_btn", label = tags$img(src="odd15.png", alt = "ODD 15"), class="btn-landing")),
          column(3, actionButton("odd16_btn", label = tags$img(src="odd16.png", alt = "ODD 16"), class="btn-landing"))
        ),
        fluidRow(
          column(3, actionButton("odd17_btn", label = tags$img(src="odd17.png", alt = "ODD 17"), class="btn-landing"))
        )
      )
    } else {
      sidebarLayout(
        sidebarPanel(
          selectizeInput("ind_name","Indicateur (nom)", choices=NULL, multiple=FALSE,
                         options=list(placeholder="Choisir un indicateur…")),
          selectizeInput("source","Source", choices=NULL, multiple=TRUE,
                         options=list(placeholder="Choisir la/les source(s)…")),
          sliderInput("years","Période", min = 1960, max = 2035,
                      value = c(2000, 2025), sep = ""),
          actionButton("go","Appliquer", class="btn-primary"),
          tags$hr(),
          downloadButton("dl_csv","Télécharger CSV"),
          downloadButton("dl_plot","Télécharger Graphique"),
          downloadButton("dl_map","Télécharger Carte"),
          tags$hr(),
          if (identical(mode, "odd_indi") && !is.null(odd)) {
            div(
              style = "text-align:center; margin-top:4px;",
              tags$img(
                src   = paste0(odd, ".png"),
                alt   = odd,
                style = "height:80px; border-radius:10px; box-shadow:0 3px 10px rgba(0,0,0,0.18);"
              ),
              div("Objectif de Développement Durable sélectionné",
                  style="margin-top:6px;font-size:12px;color:#364f70;")
            )
          } else NULL
        ),
        mainPanel(
          if (identical(mode, "odd_indi")) {
            tagList(
              actionButton("back_odds", "Retour aux ODD", class = "btn btn-default"),
              tags$br(), tags$br(),
              indicator_tabs()
            )
          } else if (identical(mode, "socio")) {
            tagList(
              actionButton("back_home1", "Retour à l'accueil", class = "btn btn-default"),
              tags$br(), tags$br(),
              indicator_tabs()
            )
          } else {
            indicator_tabs()
          }
        )
      )
    }
  })

  # ---- Sélecteur de comparaison : choix vide par défaut ----
  output$compare_ui <- renderUI({
    df <- ind_cat()
    if (!nrow(df)) return(NULL)

    # On enlève l'indicateur principal de la liste de comparaison
    main_name <- if (nz_chr1(input$ind_name)) input$ind_name else NULL
    choices_names <- df$indicator_name
    if (!is.null(main_name)) {
      choices_names <- choices_names[choices_names != main_name]
    }

    selectInput(
      "compare_ind",
      "Comparer avec :",
      choices  = c("Choisir un indicateur à comparer..." = "", choices_names),
      selected = "",
      width    = "100%"
    )
  })

  observe({
    cf <- code_filter()
    df <- fetch_indicators("")
    if (!is.null(cf)) df <- df[df$indicator_code %in% cf, , drop = FALSE]
    ind_cat(df)
    updateSelectizeInput(session, "ind_name",
                         choices = df$indicator_name,
                         selected = character(0),
                         server = FALSE)
  })

  observeEvent(input$ind_name, ignoreInit=TRUE, {
    df <- ind_cat(); if (nrow(df)==0 || !nz_chr1(input$ind_name)) return(NULL)
    code <- df$indicator_code[match(input$ind_name, df$indicator_name)]
    yrs <- isolate(input$years)
    dat <- try(api_get_csv("/export/csv",
                           list(indicator_code=code, start=yrs[1], end=yrs[2])), silent=TRUE)

    if (inherits(dat,"try-error") || !is.data.frame(dat) || !nrow(dat)) {
      updateSelectizeInput(session, "source",
                           choices=character(0), selected=character(0), server=FALSE)
      return(NULL)
    }

    src_choices <- sort(unique(na.omit(as.character(dat$source))))
    src_labels  <- pretty_source(src_choices)
    choices_nm  <- stats::setNames(src_choices, src_labels)
    keep_sel    <- intersect(isolate(input$source), src_choices)
    sel         <- if (length(keep_sel)) keep_sel else src_choices

    updateSelectizeInput(session, "source",
                         choices=choices_nm, selected=sel, server=FALSE)
  }, priority=10)

  observeEvent(input$source, ignoreInit=TRUE, {
    src <- input$source; yrs <- isolate(input$years)
    if (!length(src)) {
      df_all <- fetch_indicators("")
      cf <- code_filter()
      if (!is.null(cf)) df_all <- df_all[df_all$indicator_code %in% cf, , drop = FALSE]
      ind_cat(df_all)
      updateSelectizeInput(session, "ind_name",
                           choices=df_all$indicator_name, selected=character(0), server=FALSE)
      return(invisible(NULL))
    }
    dat <- try(api_get_csv("/export/csv",
                           list(source=src, start=yrs[1], end=yrs[2])), silent=TRUE)
    if (inherits(dat,"try-error") || !is.data.frame(dat) || !nrow(dat)) return()
    df_ind <- tibble(indicator_code=as.character(dat$indicator_code),
                     indicator_name=fix_utf8(as.character(dat$indicator_name))) |>
      distinct() |> arrange(indicator_name)
    cf <- code_filter()
    if (!is.null(cf)) df_ind <- df_ind[df_ind$indicator_code %in% cf, , drop = FALSE]
    ind_cat(df_ind)
    keep_sel <- intersect(isolate(input$ind_name), df_ind$indicator_name)
    updateSelectizeInput(session, "ind_name",
                         choices=df_ind$indicator_name, selected=keep_sel, server=FALSE)
  }, priority=9)

  output$subtitle <- renderText({
    nm <- if (nz_chr1(input$ind_name)) input$ind_name else "NA"
    srcs <- if (length(input$source)) paste(pretty_source(input$source), collapse=",") else "Toutes"
    yrs  <- paste0("Période ", input$years[1], " - ", input$years[2])
    paste("Résultats pour", nm, "|", yrs, "| Sources:", srcs)
  })

  make_params <- reactive({
    df <- ind_cat()
    code <- if (nrow(df) && nz_chr1(input$ind_name))
      df$indicator_code[match(input$ind_name, df$indicator_name)] else NULL
    list(indicator_code=code,
         source=if(length(input$source)) input$source else NULL,
         start=input$years[1], end=input$years[2])
  })

  fetch_values <- function(){
    p <- make_params()
    out <- try(api_get_csv("/export/csv", p), silent=TRUE)
    if (inherits(out,"try-error") || !nrow(out)) return(tibble())
    out |>
      mutate(period=as.integer(period), value=as.numeric(value),
             indicator_name=fix_utf8(indicator_name),
             source=as.character(source),
             obs_status=as.character(obs_status),
             ref_area=as.character(ref_area)) |>
      arrange(ref_area, period)
  }

  data_rx <- eventReactive(input$go, fetch_values(), ignoreInit=TRUE)

  compare_data_rx <- eventReactive(input$go, {
    if (!nz_chr1(input$compare_ind)) return(tibble())
    df <- ind_cat(); if (!nrow(df)) return(tibble())
    code <- df$indicator_code[match(input$compare_ind, df$indicator_name)]
    if (!nz_chr1(code)) return(tibble())
    yrs <- input$years
    src <- if (length(input$source)) input$source else NULL
    params <- list(indicator_code = code, start = yrs[1], end = yrs[2])
    if (!is.null(src)) params$source <- src
    out <- try(api_get_csv("/export/csv", params), silent = TRUE)
    if (inherits(out,"try-error") || !nrow(out)) return(tibble())
    out |>
      mutate(period=as.integer(period), value=as.numeric(value),
             indicator_name=fix_utf8(indicator_name),
             source=as.character(source),
             obs_status=as.character(obs_status),
             ref_area=as.character(ref_area)) |>
      arrange(ref_area, period)
  }, ignoreInit = TRUE)

  # ==== TABLEAU : colonnes filtrées + renommage obs_status -> Désagrégation ====
  output$tbl <- renderDT({
    dat <- data_rx()
    if (!nrow(dat)) return(DT::datatable(data.frame()))

    # On garde uniquement les colonnes souhaitées
    tbl <- dat |>
      dplyr::select(indicator_name, ref_area, period, value, obs_status, source)

    # Renommer obs_status uniquement pour l'affichage
    colnames(tbl)[colnames(tbl) == "obs_status"] <- "Disaggregation"

    DT::datatable(tbl, options=list(pageLength=10, scrollX=TRUE))
  })

  # --- Graphique (barres homogènes, labels centrés, pas de chevauchement) ---
  build_plot_obj <- function(dat0, chart_points = TRUE, dat_compare = NULL){
    if (!is.data.frame(dat0) || nrow(dat0) == 0) return(NULL)
    dat <- dat0 |>
      dplyr::mutate(
        period = suppressWarnings(as.integer(period)),
        value  = suppressWarnings(as.numeric(value))
      )
    dat <- dat[!is.na(dat$period) & !is.na(dat$value), , drop = FALSE]
    if (!nrow(dat)) return(NULL)

    has_sub <- any(!is.na(dat$ref_area)  & dat$ref_area  != "COD")
    has_obs <- any(!is.na(dat$obs_status) & dat$obs_status != "RDC")

    ## ---------- Helper interne : positionner les barres à la main ----------
    position_bars <- function(df, group_var, cluster_width){
      df <- dplyr::ungroup(df)

      # centre des années sur l’axe X : 1, 2, 3, ...
      years      <- sort(unique(df$period))
      year_index <- seq_along(years)
      year_map   <- setNames(year_index, years)

      df <- df |>
        dplyr::mutate(
          year_center = year_map[as.character(period)]
        ) |>
        dplyr::group_by(period) |>
        dplyr::arrange(.data[[group_var]], .by_group = TRUE) |>
        dplyr::mutate(
          n_in_year = dplyr::n(),
          pos_id    = dplyr::row_number(),
          # offsets symétriques pour chaque barre dans l’année
          offset = ifelse(
            n_in_year == 1,
            0,
            (pos_id - (n_in_year + 1) / 2) * (cluster_width / n_in_year)
          ),
          x_pos = year_center + offset
        ) |>
        dplyr::ungroup()

      list(
        data      = df,
        breaks_x  = year_index,
        labels_x  = years
      )
    }

    ## ---------- CAS 1 : désagrégation territoriale ----------
    if (has_sub) {
      dat_bar <- dat |>
        dplyr::filter(!is.na(ref_area) & ref_area != "COD") |>
        dplyr::group_by(period, ref_area) |>
        dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop")

      if (!nrow(dat_bar)) return(NULL)

      # nombre max de modalités par année -> sert à dimensionner la largeur des barres
      n_per_year <- dat_bar |>
        dplyr::count(period, name = "n_mod")
      n_max <- max(n_per_year$n_mod)

      cluster_width <- 0.8                               # largeur totale allouée à un groupe (année)
      bar_width     <- (cluster_width / n_max) * 0.8     # largeur d'une barre (fonction de n_max)

      pb <- position_bars(dat_bar, "ref_area", cluster_width)
      df_plot <- pb$data

      return(
        ggplot(
          df_plot,
          aes(
            x     = x_pos,
            y     = value,
            fill  = ref_area,
            group = ref_area
          )
        ) +
          geom_col(width = bar_width) +
          geom_text(
            data = df_plot[!is.na(df_plot$value), ],
            aes(label = round(value, 2)),
            vjust = -0.35,
            size  = 3.2,
            color = "black"
          ) +
          scale_x_continuous(
            breaks = pb$breaks_x,
            labels = pb$labels_x,
            expand = expansion(add = 0.25)
          ) +
          scale_y_continuous(expand = expansion(mult = c(0.02, 0.25))) +
          labs(x = "Année", y = "Valeur", fill = "Désagrégation") +
          coord_cartesian(clip = "off") +
          theme_minimal(base_size = 12) +
          theme(
            panel.grid.major = element_line(color = "grey80", linewidth = 0.1),
            panel.grid.minor = element_blank()
          )
      )
    }

    ## ---------- CAS 2 : désagrégation obs_status ----------
    if (has_obs) {
      dat_bar <- dat |>
        dplyr::filter(!is.na(obs_status) & obs_status != "RDC") |>
        dplyr::group_by(period, obs_status) |>
        dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop")

      if (!nrow(dat_bar)) return(NULL)

      n_per_year <- dat_bar |>
        dplyr::count(period, name = "n_mod")
      n_max <- max(n_per_year$n_mod)

      cluster_width <- 0.8
      bar_width     <- (cluster_width / n_max) * 0.8

      pb <- position_bars(dat_bar, "obs_status", cluster_width)
      df_plot <- pb$data

      return(
        ggplot(
          df_plot,
          aes(
            x     = x_pos,
            y     = value,
            fill  = obs_status,
            group = obs_status
          )
        ) +
          geom_col(width = bar_width) +
          geom_text(
            data = df_plot[!is.na(df_plot$value), ],
            aes(label = round(value, 2)),
            vjust = -0.35,
            size  = 3.2,
            color = "black"
          ) +
          scale_x_continuous(
            breaks = pb$breaks_x,
            labels = pb$labels_x,
            expand = expansion(add = 0.25)
          ) +
          scale_y_continuous(expand = expansion(mult = c(0.02, 0.25))) +
          labs(x = "Année", y = "Valeur", fill = "Désagrégation") +
          coord_cartesian(clip = "off") +
          theme_minimal(base_size = 12) +
          theme(
            panel.grid.major = element_line(color = "grey80", linewidth = 0.1),
            panel.grid.minor = element_blank()
          )
      )
    }

    ## ---------- CAS 3 : séries temporelles + comparaison (lignes) ----------
    dat_main <- dat
    dat_main$series <- ifelse(
      nz_chr1(dat_main$indicator_name),
      paste0(dat_main$indicator_name, " (principal)"),
      "Indicateur principal"
    )

    has_compare <- is.data.frame(dat_compare) && nrow(dat_compare) > 0
    if (has_compare) {
      dc <- dat_compare |>
        dplyr::mutate(
          period = suppressWarnings(as.integer(period)),
          value  = suppressWarnings(as.numeric(value))
        )
      dc <- dc[!is.na(dc$period) & !is.na(dc$value), , drop = FALSE]

      if (nrow(dc)) {
        dc$series <- ifelse(
          nz_chr1(dc$indicator_name),
          paste0(dc$indicator_name, " (comparé)"),
          "Indicateur comparé"
        )
        dat_all <- dplyr::bind_rows(dat_main, dc)
        yrs_all <- sort(unique(dat_all$period))

        return(
          ggplot(dat_all, aes(period, value, color = series, group = series)) +
            geom_line(linewidth = 0.8) +
            { if (isTRUE(chart_points)) geom_point() } +
            scale_x_continuous(breaks = year_breaks(yrs_all)) +
            labs(x = "Année", y = "Valeur", color = "Série") +
            theme_minimal(base_size = 12) +
            theme(
              panel.grid.major = element_line(color = "grey80", linewidth = 0.1),
              panel.grid.minor = element_blank()
            )
        )
      }
    }

    ## ---------- CAS 4 : une seule série (ligne simple) ----------
    yrs_main <- sort(unique(dat_main$period))
    ggplot(dat_main, aes(period, value, color = source, group = source)) +
      geom_line(linewidth = 0.8) +
      { if (isTRUE(chart_points)) geom_point() } +
      scale_x_continuous(breaks = year_breaks(yrs_main)) +
      labs(x = "Année", y = "Valeur", color = "Source") +
      theme_minimal(base_size = 12) +
      theme(
        panel.grid.major = element_line(color = "grey80", linewidth = 0.1),
        panel.grid.minor = element_blank()
      )
  }

  output$plot <- renderPlot({
    p <- build_plot_obj(data_rx(), input$chart_points, dat_compare = compare_data_rx())
    if (is.null(p)) { plot.new(); text(0.5,0.5,"Aucune donnée à tracer.",cex=1.1); return() }
    print(p)
  })

  # ===== UI dynamique pour l'Année (carte) =====
  output$map_year_ui <- renderUI({
    dat <- data_rx()
    if (!is.data.frame(dat) || !nrow(dat)) return(NULL)
    yrs <- sort(unique(as.integer(dat$period[!is.na(dat$ref_area) & dat$ref_area != "COD"])))
    if (!length(yrs)) return(NULL)

    tags$div(
      style = "display:flex; align-items:center; gap:8px; margin-bottom:8px;",
      tags$span(tags$b("Année")),
      selectInput("map_year", label = NULL,
                  choices = yrs, selected = max(yrs),
                  width = "120px")
    )
  })

  # ========= CARTE =========
  output$map <- renderLeaflet({
    dat <- data_rx()

    rdc_bounds <- list(w = 12, s = -14, e = 32, n = 6)
    center_lng <- 23.5; center_lat <- -2.5

    shp_dir  <- normalizePath(file.path(getwd(), "www"), winslash = "/")
    shp_path <- file.path(shp_dir, "gadm41_COD_2.shp")
    if (!file.exists(shp_path)) shp_path <- file.path(shp_dir, "gadm41_COD_2.gpkg")

    g <- tryCatch(sf::st_read(shp_path, quiet = TRUE), error = function(e) NULL)
    if (is.null(g) || !inherits(g, "sf")) {
      return(leaflet(options = leafletOptions(zoomControl = TRUE)) |>
               setView(center_lng, center_lat, 6) |>
               addControl("Shapefile introuvable : placer gadm41_COD_2.* dans ui/www",
                          position = "bottomleft"))
    }
    g <- tryCatch(sf::st_transform(g, 4326), error = function(e) g)
    g <- sf::st_make_valid(g)

    g$join_key <- if ("NAME_2" %in% names(g)) g$NAME_2 else NA_character_
    if ("VARNAME_2" %in% names(g)) {
      g$join_key <- ifelse(is.na(g$join_key) | !nzchar(g$join_key), g$VARNAME_2, g$join_key)
    }
    g$join_key   <- toupper(stringi::stri_trans_general(g$join_key, "Latin-ASCII"))
    g$label_name <- if ("NAME_2" %in% names(g)) g$NAME_2 else g$join_key

    m <- leaflet(options = leafletOptions(zoomControl = TRUE, minZoom = 4, maxZoom = 9)) |>
      setView(lng = center_lng, lat = center_lat, zoom = 6) |>
      setMaxBounds(rdc_bounds$w, rdc_bounds$s, rdc_bounds$e, rdc_bounds$n)

    m <- m |>
      addPolygons(data = g, fillOpacity = 0, color = "#000000", weight = 1)

    if (!is.data.frame(dat) || !nrow(dat)) {
      return(m |> addControl("Aucune zone détectée pour cet indicateur.", position = "bottomleft"))
    }

    yrs_all <- sort(unique(as.integer(dat$period[!is.na(dat$ref_area) & dat$ref_area != "COD"])))
    if (!length(yrs_all)) {
      return(m |> addControl("Aucune zone détectée pour cet indicateur.", position = "bottomleft"))
    }
    sel_year <- if (!is.null(input$map_year)) as.integer(input$map_year) else max(yrs_all)

    dat2 <- dat |>
      dplyr::filter(!is.na(ref_area), ref_area != "COD", as.integer(period) == sel_year) |>
      dplyr::mutate(
        ref_area = toupper(stringi::stri_trans_general(ref_area, "Latin-ASCII")),
        value    = suppressWarnings(as.numeric(value)),
        modality = dplyr::coalesce(as.character(obs_status), NA_character_)
      ) |>
      dplyr::transmute(
        join_key = ref_area,
        value, modality, year = sel_year
      )

    if (!nrow(dat2)) {
      return(m |> addControl("Aucune zone détectée pour cet indicicateur.", position = "bottomleft"))
    }

    gj <- dplyr::left_join(g, dat2, by = "join_key")
    gj_on <- gj[!is.na(gj$value) & !sf::st_is_empty(gj), ]
    if (!nrow(gj_on)) {
      return(m |> addControl("Aucune zone détectée pour cet indicateur.", position = "bottomleft"))
    }

    pal <- colorNumeric("Blues", domain = gj_on$value, na.color = "transparent")

    m <- m |>
      addPolygons(
        data = gj_on,
        fillColor = ~pal(value), fillOpacity = 0.75,
        color = "#000000", weight = 1, opacity = 1,
        highlight = highlightOptions(weight = 2, color = "#000000", bringToFront = TRUE)
      )

    cent <- sf::st_point_on_surface(gj_on)
    cent_xy <- sf::st_coordinates(cent)
    m <- m |>
      addLabelOnlyMarkers(
        lng = cent_xy[,1], lat = cent_xy[,2], label = gj_on$label_name,
        labelOptions = labelOptions(
          noHide = TRUE, direction = "center", textOnly = TRUE,
          style = list(
            "color" = "#0b234a", "font-weight" = "700",
            "font-size" = "12px", "text-shadow" = "0 0 6px rgba(255,255,255,.9)"
          )
        )
      )

    m <- m |>
      addLegend(
        position = "bottomright",
        pal = pal, values = gj_on$value,
        title = paste0("Valeur (", sel_year, ")"),
        opacity = 0.75, labFormat = labelFormat(digits = 2)
      )

    leg_rows <- sprintf(
      "<li><b>%s</b>%s : %s (%s)</li>",
      htmlEscape(gj_on$label_name),
      ifelse(is.na(gj_on$modality) | gj_on$modality %in% c("", "RDC"),
             "", paste0(" — ", htmlEscape(gj_on$modality))),
      ifelse(is.finite(gj_on$value), formatC(gj_on$value, format = "f", digits = 2), "NA"),
      sel_year
    )
    legend_html <- HTML(paste0(
      "<div style='background:#ffffffcc;padding:8px 10px;border:1px solid #c7d3e6;border-radius:8px;max-height:180px;overflow:auto;'>",
      "<div style='font-weight:700;margin-bottom:6px;'>Zones & modalités</div>",
      "<ul style='padding-left:18px;margin:0;'>", paste(leg_rows, collapse = ""), "</ul>",
      "</div>"
    ))
    m <- m |> addControl(legend_html, position = "bottomleft")

    bb <- sf::st_bbox(g)
    if (all(is.finite(bb))) {
      bx <- unname(as.numeric(bb[c("xmin","ymin","xmax","ymax")]))
      m  <- m |> fitBounds(bx[1], bx[2], bx[3], bx[4])
    }
    m
  })
 
  output$dl_map <- downloadHandler(
    filename = function(){
      nm <- if (nz_chr1(input$ind_name)) gsub("[^A-Za-z0-9_-]+","_",input$ind_name) else "carte"
      src <- if (length(input$source)) paste(input$source,collapse="-") else "ALLSRC"
      paste0(nm,"_",src,"_",input$years[1],"-",input$years[2],"_map.png")
    },
    content = function(file){
      dat <- data_rx()
      shp_dir  <- normalizePath(file.path(getwd(), "www"), winslash = "/")
      shp_path <- file.path(shp_dir, "gadm41_COD_2.shp")
      if (!file.exists(shp_path)) shp_path <- file.path(shp_dir, "gadm41_COD_2.gpkg")
      g <- tryCatch(sf::st_read(shp_path, quiet = TRUE), error = function(e) NULL)
      if (is.null(g) || !inherits(g, "sf") || !is.data.frame(dat) || !nrow(dat)) {
        stop("Aucune zone détectée pour cet indicateur.")
      }
      g <- tryCatch(sf::st_transform(g, 4326), error = function(e) g)
      g <- sf::st_make_valid(g)
      g$join_key <- if ("NAME_2" %in% names(g)) g$NAME_2 else NA_character_
      if ("VARNAME_2" %in% names(g)) {
        g$join_key <- ifelse(is.na(g$join_key) | !nzchar(g$join_key), g$VARNAME_2, g$join_key)
      }
      g$join_key   <- toupper(stringi::stri_trans_general(g$join_key, "Latin-ASCII"))
      g$label_name <- if ("NAME_2" %in% names(g)) g$NAME_2 else g$join_key

      yrs_all <- sort(unique(as.integer(dat$period[!is.na(dat$ref_area) & dat$ref_area != "COD"])))
      if (!length(yrs_all)) stop("Aucune zone détectée pour cet indicateur.")
      sel_year <- if (!is.null(input$map_year)) as.integer(input$map_year) else max(yrs_all)

      dat2 <- dat |>
        dplyr::filter(!is.na(ref_area), ref_area != "COD", as.integer(period) == sel_year) |>
        dplyr::mutate(
          ref_area = toupper(stringi::stri_trans_general(ref_area, "Latin-ASCII")),
          value    = suppressWarnings(as.numeric(value)),
          modality = dplyr::coalesce(as.character(obs_status), NA_character_)
        ) |>
        dplyr::transmute(
          join_key = ref_area,
          value, modality, year = sel_year
        )
      if (!nrow(dat2)) stop("Aucune zone détectée pour cet indicateur.")

      gj <- dplyr::left_join(g, dat2, by = "join_key")
      gj_on <- gj[!is.na(gj$value) & !sf::st_is_empty(gj), ]
      if (!nrow(gj_on)) stop("Aucune zone détectée pour cet indicateur.")

      cent <- sf::st_point_on_surface(gj_on)
      cent_xy <- sf::st_coordinates(cent)

      p <- ggplot() +
        geom_sf(data = g, fill = NA, color = "black", linewidth = 0.3) +
        geom_sf(data = gj_on, aes(fill = value), color = "black", linewidth = 0.3) +
        scale_fill_gradient(
          name = paste0("Valeur (", sel_year, ")"),
          low = "#deebf7", high = "#08519c", na.value = "transparent"
        ) +
        geom_text(
          data = cbind(as.data.frame(gj_on), cent_xy),
          aes(x = X, y = Y, label = label_name),
          size = 3.2, fontface = "bold", color = "#0b234a"
        ) +
        coord_sf(xlim = c(12, 32), ylim = c(-14, 6), expand = FALSE) +
        theme_minimal(base_size = 12) +
        theme(
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          panel.background = element_rect(fill = "white", colour = NA),
          plot.background  = element_rect(fill = "white", colour = NA),
          legend.position = "right"
        )

      ggsave(file, plot = p, width = 9, height = 6, dpi = 150)
    }
  )

  output$dl_csv <- downloadHandler(
    filename=function(){
      nm <- if (nz_chr1(input$ind_name)) gsub("[^A-Za-z0-9_-]+","_",input$ind_name) else "export"
      src <- if (length(input$source)) paste(input$source,collapse="-") else "ALLSRC"
      paste0(nm,"_",src,"_",input$years[1],"-",input$years[2],".csv")
    },
    content=function(file){
      p <- make_params()
      txt <- api_get_text("/export/csv", p)
      writeBin(charToRaw(as.character(txt)[1]), file)
    }
  )

  output$dl_plot <- downloadHandler(
    filename=function(){
      nm <- if (nz_chr1(input$ind_name)) gsub("[^A-Za-z0-9_-]+","_",input$ind_name) else "graphique"
      src <- if (length(input$source)) paste(input$source,collapse="-") else "ALLSRC"
      paste0(nm,"_",src,"_",input$years[1],"-",input$years[2],".png")
    },
    content=function(file){
      p <- build_plot_obj(data_rx(), input$chart_points, dat_compare = compare_data_rx())
      if (is.null(p)) stop("Aucun graphique à exporter.")
      ggsave(file, plot=p, width=9, height=5, dpi=150)
    }
  )
}

shinyApp(ui, server)
