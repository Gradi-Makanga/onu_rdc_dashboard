# parser_scrape_generic.R — Our World in Data & CSV génériques 
  
`%||%` <- function(a,b) if (is.null(a)) b else a

parse_scrape_generic <- function(indicator_code, url) {
  if (!nzchar(url)) {
    return(tibble::tibble(indicator_code=character(), ref_area=character(), period=integer(),
                          value=numeric(), obs_status=character(), source=character()))
  }

  df <- tryCatch(readr::read_csv(url, show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (is.null(df) || !NROW(df)) {
    return(tibble::tibble(indicator_code=character(), ref_area=character(), period=integer(),
                          value=numeric(), obs_status=character(), source=character()))
  }
  nm <- names(df)

  # Cas OWID mobile network capability: Entity, Code, Year, 2g, 3g, 4g, 5g
  has_caps <- all(c("Entity","Code","Year") %in% nm) && any(tolower(nm) %in% c("2g","3g","4g","5g"))
  if (has_caps) {
    long <- df |>
      tidyr::pivot_longer(cols = dplyr::any_of(c("2g","3g","4g","5g")),
                          names_to = "cap", values_to = "val") |>
      dplyr::filter(!is.na(val))
    out <- long |>
      dplyr::transmute(
        indicator_code = paste0(indicator_code, "_", toupper(cap)),
        ref_area       = as.character(.data$Code),
        period         = suppressWarnings(as.integer(.data$Year)),
        value          = suppressWarnings(as.numeric(.data$val)),
        obs_status     = NA_character_,
        source         = "scrape"
      ) |>
      dplyr::filter(!is.na(ref_area), !is.na(period), !is.na(value))
    return(out)
  }

  # Fallback générique: cherche (value, year, code/iso3)
  vcol <- nm[tolower(nm) == "value"][1]
  ycol <- nm[grepl("^year$", nm, ignore.case = TRUE)][1]
  icol <- nm[grepl("^(code|iso3|iso_3|countryiso3code)$", nm, ignore.case = TRUE)][1]
  if (!is.na(vcol) && !is.na(ycol) && !is.na(icol)) {
    out <- df |>
      dplyr::transmute(
        indicator_code = as.character(indicator_code),
        ref_area       = as.character(.data[[icol]]),
        period         = suppressWarnings(as.integer(.data[[ycol]])),
        value          = suppressWarnings(as.numeric(.data[[vcol]])),
        obs_status     = NA_character_,
        source         = "scrape"
      ) |>
      dplyr::filter(!is.na(ref_area), !is.na(period), !is.na(value))
    return(out)
  }

  tibble::tibble(indicator_code=character(), ref_area=character(), period=integer(),
                 value=numeric(), obs_status=character(), source=character())
}
