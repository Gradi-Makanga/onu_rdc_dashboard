
# pull_now.R — run the pipeline once
if (!requireNamespace("targets", quietly = TRUE)) install.packages("targets")
targets::tar_make()
