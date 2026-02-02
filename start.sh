#!/usr/bin/env bash
set -e

export PORT="${PORT:-8080}"
export HOST="0.0.0.0"

Rscript -e "shiny::runApp('ui', host=Sys.getenv('HOST','0.0.0.0'), port=as.integer(Sys.getenv('PORT','8080')))"
