FROM rocker/r2u:jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN install.r \
    plumber \
    DBI \
    targets \
    RPostgres \
    jsonlite \
    httr2 \
    dplyr \
    readr \
    dotenv

WORKDIR /app
COPY . /app

EXPOSE 8080

CMD ["R","-e","pr <- plumber::plumb('api/plumber.R'); pr$run(host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 8080)))"]
