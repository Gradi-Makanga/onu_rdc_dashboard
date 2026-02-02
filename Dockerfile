FROM rocker/shiny:4.3.2

# Dépendances système nécessaires (Leaflet + RPostgres + compilation)
RUN apt-get update && apt-get install -y --no-install-recommends \
  libpq-dev \
  libssl-dev \
  libcurl4-openssl-dev \
  libxml2-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Installer les packages R via CRAN
RUN R -e "install.packages(c('shiny','httr2','jsonlite','readr','dplyr','ggplot2','leaflet','DT','RPostgres','DBI','dotenv','sf'), repos='https://cloud.r-project.org')"

EXPOSE 3838

# Lance l'app depuis /app/ui
CMD ["R","-e","shiny::runApp('ui', host='0.0.0.0', port=as.integer(Sys.getenv('PORT',3838)))"] 
