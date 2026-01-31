# =========================
# unsdg.R  (parseur UNSDG)
# =========================
# - Normalise les données UNSDG (CSV local ou API)
# - Construit obs_status = "Sexe=F | Tranche âge=15-24 | Résidence=Urbain | ..."
# - Conserve les NA (pour affichage et export ultérieur)
# - Renvoie un tibble standardisé utilisable dans export_latest
#
# Dépendances : readr, dplyr, tidyr, jsonlite, httr2 (API optionnelle), stringr

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(jsonlite)
  library(stringr)
})

# --------- Helpers communs ---------
fix_utf8 <- function(x){
  x <- as.character(x)
  bad <- grepl("<[0-9a-fA-F]{2}>", iconv(x, "", "UTF-8", sub = "byte"), perl = TRUE)
  if (any(bad)) {
    y <- try(iconv(x, from = "latin1", to = "UTF-8"), silent = TRUE)
    if (!inherits(y, "try-error")) x <- y
  }
  x
}

# Construit un libellé de désagrégation lisible à partir des colonnes présentes
build_obs_status <- function(df,
  map = list(
    sex             = "Sexe",
    age             = "Âge",
    age_group       = "Tranche âge",
    location        = "Localisation",
    residence       = "Résidence",
    urban_rural     = "Urb/Rur",
    area_type       = "Milieu",
    education       = "Éducation",
    education_level = "Niv. éduc.",
    wealth_quintile = "Quintile",
    disability      = "Handicap",
    employment      = "Emploi",
    occupation      = "Statut prof",
    subgroup        = "Sous-groupe",
    disaggregation  = "Désaggr."
  )
){
  if (!is.data.frame(df)) stop("build_obs_status: df doit être un data.frame")
  present <- intersect(names(map), names(df))
  if (!length(present)) return(rep(NA_character_, nrow(df)))

  parts <- lapply(present, function(k){
    lab <- map[[k]]
    v <- df[[k]]
    v <- ifelse(is.na(v) | v == "" | (is.factor(v) & is.na(as.character(v))),
                NA_character_, as.character(v))
    ifelse(is.na(v), NA_character_, paste0(lab, "=", v))
  })

  res <- apply(do.call(cbind, parts), 1L, function(row){
    row <- row[!is.na(row) & nzchar(row)]
    if (!length(row)) "" else paste(row, collapse = " | ")
  })
  res <- ifelse(res == "", NA_character_, res)
  res
}

# Nettoyage/standardisation basique des noms de colonnes
clean_names <- function(x){
  x <- gsub("\\.", "_", x)
  x <- gsub("[^A-Za-z0-9_]+", "_", x)
  x <- tolower(x)
  x
}

# Essaie de déduire le trio (code, nom) indicateurs depuis colonnes courantes UNSDG
# On accepte plusieurs variantes de colonnes rencontrées dans les extractions UNSDG
extract_indicator_fields <- function(df){
  cn <- names(df)
  # code indicateur
  cand_code <- c("indicator_code", "indicator", "seriescode", "series_code", "goal_indicator")
  ind_code <- NA_character_
  for (c in cand_code) if (c %in% cn) { ind_code <- df[[c]]; break }

  # nom indicateur
  cand_name <- c("indicator_name", "seriesdescription", "series_description", "indicator_name_fr", "indicator_name_en", "series")
  ind_name <- NA_character_
  for (c in cand_name) if (c %in% cn) { ind_name <- df[[c]]; break }

  list(code = ind_code, name = ind_name)
}

# Déduit ref_area (ISO3 si possible) depuis colonnes classiques (geoareacode, geoareaname, ref_area, iso3, countrycode, ...).
# Si area_lookup fourni (data.frame avec colonnes m49 -> iso3), on l'utilise.
resolve_ref_area <- function(df, area_lookup = NULL){
  cn <- names(df)

  # 1) Si ref_area déjà présent (ISO3), on garde
  if ("ref_area" %in% cn) {
    ra <- as.character(df$ref_area)
    return(ra)
  }

  # 2) ISO3 direct
  for (c in c("iso3", "iso_code3", "country_code")) {
    if (c %in% cn) return(as.character(df[[c]]))
  }

  # 3) M49 -> ISO3 via lookup (optionnel)
  if (!is.null(area_lookup)) {
    cand_m49 <- c("geoareacode", "m49", "country_id")
    for (c in cand_m49) {
      if (c %in% cn) {
        m49 <- as.character(df[[c]])
        j <- match(m49, as.character(area_lookup$m49))
        iso3 <- ifelse(is.na(j), NA_character_, as.character(area_lookup$iso3[j]))
        return(iso3)
      }
    }
  }

  # 4) Sinon, on essaie geoareaname comme valeur brute (pire des cas)
  for (c in c("geoareaname", "country", "country_name", "location")) {
    if (c %in% cn) return(as.character(df[[c]]))
  }

  # 5) À défaut : NA
  rep(NA_character_, nrow(df))
}

# --------- Parseur UNSDG depuis CSV local ---------
# paths : chemin fichier .csv ou dossier contenant plusieurs CSV
# area_lookup : data.frame optionnel avec colonnes m49, iso3 (pour convertir M49->ISO3)
unsdg_read_csv <- function(paths, area_lookup = NULL){
  if (dir.exists(paths)) {
    files <- list.files(paths, pattern = "\\.csv$", full.names = TRUE)
  } else {
    files <- paths
  }
  if (!length(files)) stop("unsdg_read_csv: aucun CSV trouvé.")

  all <- lapply(files, function(f){
    df <- suppressMessages(readr::read_csv(f, show_col_types = FALSE, guess_max = 200000))
    if (!nrow(df)) return(NULL)

    names(df) <- clean_names(names(df))

    # champs indicateurs
    ii <- extract_indicator_fields(df)
    indicator_code <- ii$code
    indicator_name <- ii$name

    # colonnes de valeur/période (variantes fréquentes)
    cand_value  <- c("value", "obs_value", "val", "data_value")
    cand_period <- c("time_period", "timeperiod", "period", "year", "time")

    v  <- NA_real_
    tp <- NA_integer_
    for (c in cand_value)  if (c %in% names(df)) { v  <- df[[c]];  break }
    for (c in cand_period) if (c %in% names(df)) { tp <- df[[c]]; break }

    # colonnes de désagrégation typiques UNSDG
    # (on garde tout ce qu'on trouve — build_obs_status sélectionne automatiquement)
    keep_cols <- intersect(
      c("sex","age","age_group","location","residence","urban_rural","area_type",
        "education","education_level","wealth_quintile","disability","employment",
        "occupation","subgroup","disaggregation"),
      names(df)
    )

    out <- tibble(
      indicator_code  = as.character(indicator_code),
      indicator_name  = fix_utf8(as.character(indicator_name)),
      ref_area        = resolve_ref_area(df, area_lookup),
      period          = suppressWarnings(as.integer(tp)),
      value           = suppressWarnings(as.numeric(v)),
      source          = "unsdg"
    )

    if (length(keep_cols)) {
      out <- bind_cols(out, df[, keep_cols, drop = FALSE] |> mutate(across(everything(), as.character)))
    }

    # obs_status lisible
    out <- out %>% mutate(obs_status = build_obs_status(cur_data_all()))

    # NE PAS SUPPRIMER les NA : on garde tout
    out
  })

  all <- all[!vapply(all, is.null, logical(1))]
  if (!length(all)) return(tibble())
  bind_rows(all)
}

# --------- (Optionnel) UNSDG API (SDMX-JSON) ---------
# Nécessite internet. Si tu n’en as pas besoin, ignore cette partie.
# Exemple d’endpoint : https://unstats.un.org/sdgapi/v1/sdg/Indicator/Data?indicator=1.1.1&area=CD&time=2000-2023
unsdg_from_api <- function(indicator, area = NULL, time = NULL, area_lookup = NULL, timeout = 60){
  if (!requireNamespace("httr2", quietly = TRUE))
    stop("httr2 requis pour l'API UNSDG")

  base <- "https://unstats.un.org/sdgapi/v1/sdg/Indicator/Data"
  qs <- list(indicator = indicator)
  if (!is.null(area)) qs$area <- area
  if (!is.null(time)) qs$time <- time

  url <- paste0(base, "?", paste(paste0(names(qs), "=", utils::URLencode(as.character(qs), TRUE)), collapse = "&"))
  req <- httr2::request(url) |> httr2::req_timeout(timeout)
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) >= 400) stop("UNSDG API HTTP ", httr2::resp_status(resp))
  obj <- jsonlite::fromJSON(httr2::resp_body_string(resp))

  if (is.null(obj$data) || !nrow(obj$data)) return(tibble())

  d <- obj$data
  names(d) <- clean_names(names(d))

  # Indicateur
  ind_code <- if ("indicator" %in% names(d)) d$indicator else indicator
  ind_name <- if ("indicator_name" %in% names(d)) d$indicator_name else if ("series_description" %in% names(d)) d$series_description else NA_character_

  # Valeurs / période
  v  <- NA_real_; tp <- NA_integer_
  for (c in c("value", "obs_value", "val", "data_value")) if (c %in% names(d)) { v  <- d[[c]];  break }
  for (c in c("time_period", "timeperiod", "period", "year", "time")) if (c %in% names(d)) { tp <- d[[c]]; break }

  keep_cols <- intersect(
    c("sex","age","age_group","location","residence","urban_rural","area_type",
      "education","education_level","wealth_quintile","disability","employment",
      "occupation","subgroup","disaggregation"),
    names(d)
  )

  out <- tibble(
    indicator_code  = as.character(ind_code),
    indicator_name  = fix_utf8(as.character(ind_name)),
    ref_area        = resolve_ref_area(d, area_lookup),
    period          = suppressWarnings(as.integer(tp)),
    value           = suppressWarnings(as.numeric(v)),
    source          = "unsdg"
  )
  if (length(keep_cols)) {
    out <- bind_cols(out, d[, keep_cols, drop = FALSE] |> mutate(across(everything(), as.character)))
  }
  out <- out %>% mutate(obs_status = build_obs_status(cur_data_all()))
  out
}

# --------- Wrapper principal à utiliser dans le pipeline ---------
# - Si tu lui passes un chemin fichier/dossier -> lit les CSV
# - Si tu passes indicator="1.1.1" -> interroge l'API (optionnel)
# - area_lookup = data.frame(m49, iso3) si tu veux convertir M49->ISO3
unsdg_load <- function(paths = NULL,
                       use_api = FALSE,
                       indicator = NULL,
                       area = NULL,
                       time = NULL,
                       area_lookup = NULL){
  if (isTRUE(use_api)) {
    if (is.null(indicator)) stop("unsdg_load(use_api=TRUE) nécessite indicator='…'")
    return(unsdg_from_api(indicator, area = area, time = time, area_lookup = area_lookup))
  }
  if (is.null(paths)) stop("unsdg_load: précise 'paths' (fichier .csv ou dossier).")
  unsdg_read_csv(paths, area_lookup = area_lookup)
}
