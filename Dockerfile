FROM rocker/shiny:4.3.2

# Installer les dépendances système nécessaires (déjà optimisées)
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Installer les packages R (sf est déjà supporté par rocker)
RUN R -e "install.packages(c( \
  'shiny','httr2','jsonlite','readr','dplyr','ggplot2','leaflet', \
  'DT','RPostgres','DBI','dotenv' \
), repos='https://cloud.r-project.org')"

# Copier le projet
WORKDIR /app
COPY . /app

# Exposer le port Railway
EXPOSE 3838

# Lancer l'app Shiny
CMD ["R", "-e", "shiny::runApp('ui', host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 3838)))"]
