#!/bin/bash
set -e

echo "Starting Shiny..."
cd ui
R -e "shiny::runApp('.', host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 3838)))"
