FROM rocker/shiny:4.3.2

# Installer quelques libs système souvent nécessaires
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier tout le projet
COPY . /app

# Installer les packages R nécessaires (à adapter si besoin)
RUN R -e "install.packages(c('shiny','httr2','jsonlite','readr','dplyr','ggplot2','leaflet','DT','RPostgres','DBI','dotenv'), repos='https://cloud.r-project.org')"

# Railway fournit le port via $PORT
EXPOSE 3838

# Lancer ton start.sh
CMD ["bash", "start.sh"]
