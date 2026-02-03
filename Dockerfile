# Base R + Shiny
FROM rocker/shiny:4.3.2

# --- System deps for sf / terra / units / s2 / leaflet + RPostgres ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    # build tools
    build-essential g++ make cmake pkg-config \
    # geospatial stack for sf/terra
    gdal-bin libgdal-dev \
    libgeos-dev libproj-dev proj-data proj-bin \
    # units dependency
    libudunits2-dev \
    # common deps
    libssl-dev libcurl4-openssl-dev libxml2-dev \
    # postgres client headers for RPostgres
    libpq-dev \
    # (optional but useful)
    git \
 && rm -rf /var/lib/apt/lists/*

# Force R to use site-library (stable inside rocker images)
ENV R_LIBS_SITE=/usr/local/lib/R/site-library

# --- Install R packages ---
RUN R -e "options(repos=c(CRAN='https://cloud.r-project.org'), Ncpus=2); \
          .libPaths(Sys.getenv('R_LIBS_SITE')); \
          install.packages(c('shiny','httr2','jsonlite','readr','dplyr','ggplot2','DT','DBI','dotenv','RPostgres'), dependencies=TRUE); \
          install.packages('leaflet', dependencies=TRUE); \
          cat('LIBPATHS=', paste(.libPaths(), collapse=' | '), '\n'); \
          cat('leaflet installed? ', requireNamespace('leaflet', quietly=TRUE), '\n'); \
          cat('RPostgres installed? ', requireNamespace('RPostgres', quietly=TRUE), '\n'); \
          if(!requireNamespace('leaflet', quietly=TRUE)) quit(status=1)"

# --- Copy Shiny app ---
# Ton app est dans onu_rdc_dashboard/ui
WORKDIR /srv/shiny-server/onu_rdc_dashboard
COPY ui/ ./ui/

# Shiny Server cherche une app dans /srv/shiny-server/<app>/ (app.R ou server.R/ui.R)
# Si ton fichier s'appelle app.R dans ui/, on pointe directement dessus
WORKDIR /srv/shiny-server/onu_rdc_dashboard/ui

# Expose Shiny port
EXPOSE 3838

# Run shiny-server
CMD ["/usr/bin/shiny-server"]
