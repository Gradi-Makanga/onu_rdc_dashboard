

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

# Installer les packages R (sans sf d'abord)
RUN R -e "options(Ncpus=2); install.packages(c('shiny','httr2','jsonlite','readr','dplyr','ggplot2','DT','DBI','dotenv'), repos='https://cloud.r-project.org')"

# Leaflet seul (pour isoler l'erreur et garantir l'installation)
RUN R -e "options(Ncpus=2); install.packages('leaflet', repos='https://cloud.r-project.org')"

# RPostgres seul (compile parfois; on l'isole aussi)
RUN R -e "options(Ncpus=2); install.packages('RPostgres', repos='https://cloud.r-project.org')"

# Test
RUN R -e "cat('leaflet installed? ', requireNamespace('leaflet', quietly=TRUE), '\n'); cat('RPostgres installed? ', requireNamespace('RPostgres', quietly=TRUE), '\n')"
RUN R -e "if (!requireNamespace('leaflet', quietly=TRUE)) quit(status=1)"

EXPOSE 3838 

# Lance l'app depuis /app/ui
CMD ["R","-e","shiny::runApp('ui', host='0.0.0.0', port=as.integer(Sys.getenv('PORT',3838)))"] 
