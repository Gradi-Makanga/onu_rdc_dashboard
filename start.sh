#!/bin/bash
set -e

echo "Working dir:"
pwd
echo "Listing root:"
ls -la

cd ui
echo "Now in:"
pwd
echo "Listing ui:"
ls -la

R -e "shiny::runApp('.', host='0.0.0.0', port=as.integer(Sys.getenv('PORT', 3838)))"
