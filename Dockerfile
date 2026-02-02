FROM rocker/shiny:4.3.2

# Dépendances système utiles (dont PostgreSQL)
RUN apt-get update && apt-get install -y --no-install-recommends \
  libpq-dev \
  && rm -rf /var/lib/apt/lists/*

# Installer les packages R en binaires Debian (rapide)
RUN apt-get update && apt-get install -y --no-install-recommends \
  r-cran-shiny \
  r-cran-httr2 \
  r-cran-jsonlite \
  r-cran-readr \
  r-cran-dplyr \
  r-cran-ggplot2 \
  r-cran-leaflet \
  r-cran-dt \
  r-cran-rpostgres \
  r-cran-dbi \
  r-cran-dotenv \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

EXPOSE 3838

CMD ["R", "-e", "shiny::runApp('ui', host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 3838)))"]
