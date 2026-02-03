# Image R stable 
FROM rocker/r-ver:4.5.1

# Librairies système nécessaires (SSL, CURL, Postgres, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libpq-dev \
    libicu-dev \
    && rm -rf /var/lib/apt/lists/*

# (Option très utile) repo binaries Posit pour éviter compilation
ENV RSPM="https://packagemanager.posit.co/cran/__linux__/bookworm/latest"

# Installer les packages R nécessaires à l'API
RUN R -e "options(repos=c(CRAN='https://cloud.r-project.org')); \
          install.packages('stringi', type='source'); \
          install.packages(c('plumber','DBI','RPostgres','jsonlite','dotenv','readr','dplyr','httr2'), dependencies=TRUE)"


# Copier le code du repo 
WORKDIR /app
COPY . /app

# Port Railway
EXPOSE 8080

# Lancer l'API
CMD ["R","-e","pr <- plumber::plumb('api/plumber.R'); pr$run(host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 8080)))"]
