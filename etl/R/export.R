# R/export.R
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(tibble)
})

# Écrit le CSV d’export et renvoie le chemin du fichier (pour targets, format = "file")
write_exports <- function(df, out_dir = "data_final", filename = "export_latest.csv") {
  # sécurité : forcer le schéma attendu
  cols <- c("indicator_code","ref_area","period","value","obs_status","source")
  if (!all(cols %in% names(df))) {
    df <- df %>%
      tibble::as_tibble() %>%
      mutate(
        indicator_code = .data$indicator_code %||% NA_character_,
        ref_area       = .data$ref_area       %||% NA_character_,
        period         = suppressWarnings(as.integer(.data$period %||% NA_integer_)),
        value          = suppressWarnings(as.numeric(.data$value %||% NA_real_)),
        obs_status     = .data$obs_status     %||% NA_character_,
        source         = .data$source         %||% NA_character_
      ) %>%
      select(any_of(cols))
  } else {
    df <- df %>% select(all_of(cols))
  }

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(out_dir, filename)

  # écrire toujours un header, même si df est vide
  readr::write_csv(df, path, na = "")

  # renvoyer le chemin pour que targets puisse le traquer (format="file")
  path
}

# petit opérateur utilitaire (coalesce pour objets éventuellement NULL)
`%||%` <- function(x, y) if (is.null(x)) y else x
