# utils_common.R — coalesce vectorisés et helpers  
suppressWarnings({
  library(tibble); library(purrr)
})

# coalesce pour character : prend le 1er non-NA et non-vide "" pour chaque ligne
coalesce_chr <- function(...) {
  args <- list(...)
  n <- max(1L, vapply(args, length, 1L))
  args <- lapply(args, function(x){
    x <- as.character(x)
    if (length(x) == 0) x <- rep(NA_character_, n) else x <- rep_len(x, n)
    x[!nzchar(x)] <- NA_character_   # traite "" comme NA
    x
  })
  out <- args[[1]]
  if (length(args) > 1) {
    for (i in 2:length(args)) {
      idx <- is.na(out)
      out[idx] <- args[[i]][idx]
    }
  }
  out
}

# coalesce pour numeric : idem mais conversion sûre
coalesce_num <- function(...) {
  args <- list(...)
  n <- max(1L, vapply(args, length, 1L))
  args <- lapply(args, function(x){
    x <- suppressWarnings(as.numeric(x))
    if (length(x) == 0) rep(NA_real_, n) else rep_len(x, n)
  })
  out <- args[[1]]
  if (length(args) > 1) {
    for (i in 2:length(args)) {
      idx <- is.na(out)
      out[idx] <- args[[i]][idx]
    }
  }
  out
}
