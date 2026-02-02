#!/bin/bash
set -e
echo "Starting Shiny app..."
cd ui
Rscript run_shiny.R
