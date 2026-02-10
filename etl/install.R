# install.R
options(repos = c(CRAN = "https://cloud.r-project.org"))

pkgs <- c(
  "targets","tarchetypes",
  "dplyr","purrr","stringr","readr","httr2","jsonlite","tibble","tidyr",
  "DBI","RPostgres","arrow","rvest","xml2","dotenv"
)

to_install <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(to_install)) install.packages(to_install)

cat("Installed packages OK\n")