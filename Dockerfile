FROM rocker/r-ver:4.3.2

# libs système souvent nécessaires à leaflet, sf, curl, ssl, etc.
RUN apt-get update && apt-get install -y \
  libcurl4-openssl-dev \
  libssl-dev \
  libxml2-dev \
  libgit2-dev \
  libfontconfig1-dev \
  libharfbuzz-dev \
  libfribidi-dev \
  libfreetype6-dev \
  libpng-dev \
  libjpeg-dev \
  libtiff5-dev \
  && rm -rf /var/lib/apt/lists/*

# Installer les packages R nécessaires
RUN R -e "install.packages(c('shiny','leaflet','DT','httr2','jsonlite','readr','dplyr','ggplot2','RPostgres','DBI','dotenv','sf'), repos='https://cloud.r-project.org')"

WORKDIR /app
COPY . /app

EXPOSE 8080

CMD ["bash", "start.sh"]
