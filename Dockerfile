# --------------------------------------------
# Dockerfile - Shiny app (rocker/shiny:4.3.2)
# Fix: force libPaths + install leaflet reliably
# --------------------------------------------

FROM rocker/shiny:4.3.2

# 1) Dépendances système (compilation + curl/ssl/xml + postgres)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libcairo2-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    && rm -rf /var/lib/apt/lists/*

# 2) Forcer où R installe les packages (IMPORTANT)
ENV R_LIBS_USER=/usr/local/lib/R/site-library
RUN mkdir -p ${R_LIBS_USER}

# (Optionnel mais utile sur certains environnements)
ENV RENV_CONFIG_SANDBOX_ENABLED=FALSE

# 3) Installer les packages R (en forçant repo + lib + libPaths)
#    + nettoyer les locks pour éviter "00LOCK-xxxx"
RUN rm -rf ${R_LIBS_USER}/00LOCK* /usr/local/lib/R/site-library/00LOCK* /usr/lib/R/site-library/00LOCK* || true

RUN R -e "options(repos=c(CRAN='https://cloud.r-project.org')); \
          .libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); \
          install.packages(c('shiny','httr2','jsonlite','readr','dplyr','ggplot2','DT','DBI','dotenv'), \
                           lib=Sys.getenv('R_LIBS_USER'));"

# Leaflet séparément + vérification immédiate
RUN rm -rf ${R_LIBS_USER}/00LOCK* || true && \
    R -e "options(repos=c(CRAN='https://cloud.r-project.org')); \
          .libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); \
          install.packages('leaflet', lib=Sys.getenv('R_LIBS_USER')); \
          cat('LIBPATHS=', paste(.libPaths(), collapse=' | '), '\n'); \
          cat('leaflet installed? ', requireNamespace('leaflet', quietly=TRUE), '\n'); \
          if(!requireNamespace('leaflet', quietly=TRUE)) quit(status=1)"

# RPostgres séparément + vérification immédiate
RUN rm -rf ${R_LIBS_USER}/00LOCK* || true && \
    R -e "options(repos=c(CRAN='https://cloud.r-project.org')); \
          .libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); \
          install.packages('RPostgres', lib=Sys.getenv('R_LIBS_USER')); \
          cat('RPostgres installed? ', requireNamespace('RPostgres', quietly=TRUE), '\n'); \
          if(!requireNamespace('RPostgres', quietly=TRUE)) quit(status=1)"

# (Si tu utilises sf, décommente ces 2 lignes)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     gdal-bin libgdal-dev libgeos-dev libproj-dev libudunits2-dev && rm -rf /var/lib/apt/lists/*
# RUN R -e "options(repos=c(CRAN='https://cloud.r-project.org')); .libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); install.packages('sf', lib=Sys.getenv('R_LIBS_USER'))"

# 4) Copier l'app
WORKDIR /app
COPY . /app

# 5) Exposer le port Shiny
EXPOSE 3838

# 6) Lancer l'app
#    IMPORTANT: Railway fournit PORT, donc on le lit via Sys.getenv('PORT', 3838)
CMD ["R", "-e", "shiny::runApp('/app/ui', host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 3838)))"]
