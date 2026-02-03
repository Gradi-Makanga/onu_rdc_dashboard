# Image R avec binaries CRAN (stringi OK)
FROM rocker/r2u:4.5.1

# Installer dépendances système légères
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Installer les packages R (binaries ultra rapides)
RUN install.r \
    plumber \
    DBI \
    RPostgres \
    jsonlite \
    httr2 \
    dplyr \
    readr \
    dotenv

# Copier le code
WORKDIR /app
COPY . /app

# Port Railway
EXPOSE 8080

# Lancer l'API
CMD ["R","-e","pr <- plumber::plumb('api/plumber.R'); pr$run(host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 8080)))"]
