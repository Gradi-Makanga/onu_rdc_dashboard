
# utils.R — helpers
normalize_cols <- function(x) {
  names(x) <- tolower(gsub("[ -]+","_",names(x)))
  x
}
safe_num <- function(x) suppressWarnings(as.numeric(x))
now_iso <- function() format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
