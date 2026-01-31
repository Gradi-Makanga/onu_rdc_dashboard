# _targets.R — cat -> api_raw -> data_final -> data_named -> export_latest -> export_latest_transformed -> db_upsert
suppressPackageStartupMessages({
  library(targets); library(dplyr); library(readr); library(stringr); library(tidyr)
}) 

# Charger .env
if (requireNamespace("dotenv", quietly = TRUE) && file.exists(".env")) {
  dotenv::load_dot_env(".env")
} else if (file.exists(".env")) {
  readRenviron(".env")
}

tar_option_set(
  packages = c("dplyr","purrr","stringr","readr","httr2","jsonlite","tibble","tidyr",
               "DBI","RPostgres","arrow","rvest","xml2")
)

# Définir un 'out' inoffensif pour neutraliser tout code top-level mal isolé dans R/*.R
out <- list()

# Source toutes tes fonctions (WD = etl/) en filtrant les fichiers parasites (ex: R/_targets.R)
out <- list()  # neutralise d'éventuelles références à 'out' dans R/*.R
src_files <- list.files("R", pattern = "\\.[Rr]$", recursive = TRUE, full.names = TRUE)
src_files <- src_files[basename(src_files) != "_targets.R"]
if (length(src_files)) targets::tar_source(src_files)

# -------- Helpers catalogue: extraction code WB depuis api_endpoint/scrape_url: extraction code WB depuis api_endpoint/scrape_url
extract_wb_code <- function(x) {
  x <- tolower(trimws(as.character(x)))
  if (length(x) == 0) return(character(0))
  code1 <- stringr::str_match(x, "/indicator/([a-z0-9._]+)")[,2]
  code2 <- stringr::str_match(x, "indicator=([a-z0-9._]+)")[,2]
  dplyr::coalesce(code1, code2)
}
col_or_na <- function(df, name) {
  if (name %in% names(df)) df[[name]] else rep(NA_character_, nrow(df))
}
build_indicator_lookup <- function(cat_df) {
  api_col    <- col_or_na(cat_df, "api_endpoint")
  scrape_col <- col_or_na(cat_df, "scrape_url")
  wb_any <- dplyr::coalesce(extract_wb_code(api_col), extract_wb_code(scrape_col))
  cat_df %>%
    dplyr::mutate(
      indicator_code_std = tolower(trimws(as.character(.data$indicator_code))),
      indicator_name_std = dplyr::coalesce(as.character(.data$indicator_name), NA_character_),
      source_std         = tolower(trimws(as.character(.data$source))),
      wb_code            = dplyr::if_else(source_std == "worldbank", wb_any, NA_character_)
    ) %>%
    dplyr::select(indicator_code_std, indicator_name_std, source_std, wb_code) %>%
    dplyr::distinct()
}
add_indicator_names2 <- function(df, cat_df) {
  lk <- build_indicator_lookup(cat_df)
  ind_vec <- dplyr::coalesce(
    tolower(trimws(as.character(col_or_na(df, "indicator")))),
    tolower(trimws(as.character(col_or_na(df, "indicator_id")))),
    tolower(trimws(as.character(col_or_na(df, "series_code")))),
    tolower(trimws(as.character(col_or_na(df, "series_id")))),
    tolower(trimws(as.character(col_or_na(df, "wb_indicator")))),
    tolower(trimws(as.character(col_or_na(df, "wb_code"))))
  )
  df0 <- df %>%
    dplyr::mutate(
      indicator_code = tolower(trimws(as.character(dplyr::coalesce(.data$indicator_code, ind_vec)))),
      source         = tolower(trimws(as.character(.data$source))),
      wb_indicator   = ind_vec
    )
  df_out <- df0 %>%
    dplyr::left_join(lk %>% dplyr::select(indicator_code_std, indicator_name_std),
                     by = c("indicator_code" = "indicator_code_std")) %>%
    dplyr::left_join(
      lk %>% dplyr::filter(!is.na(.data$wb_code), .data$wb_code != "") %>% dplyr::select(wb_code, indicator_name_std),
      by = dplyr::join_by(wb_indicator == wb_code)
    ) %>%
    dplyr::mutate(
      indicator_name = dplyr::coalesce(.data$indicator_name_std.x, .data$indicator_name_std.y),
      indicator_code = toupper(.data$indicator_code)
    ) %>%
    dplyr::select(-dplyr::ends_with(".x"), -dplyr::ends_with(".y")) %>%
    dplyr::select(dplyr::everything(), .data$indicator_name)
  df_out
}

# === Post-traitement à appliquer à export_latest avant to_db ===
postprocess_export <- function(Data) {
  Data_modifie <- Data %>%
    dplyr::group_by(indicator_code) %>%
    dplyr::mutate(row_id = dplyr::row_number()) %>%
    dplyr::filter(!(indicator_code == "IND012" & row_id > 23)) %>%
    dplyr::select(-row_id) %>%
    dplyr::ungroup()

  Data <- Data_modifie %>%
    dplyr::mutate(
      ref_area = dplyr::if_else(
        source == "unsdg" | is.na(ref_area) | ref_area == "",
        "COD",
        ref_area
      )
    ) %>%
    dplyr::mutate(
      ref_area = dplyr::case_when(
        indicator_code == "IND076" & value == 4.1  ~ "KISANGANI",
        indicator_code == "IND076" & value == 4.4  ~ "KOLWEZI",
        indicator_code == "IND076" & value == 4.6  ~ "LUBUMBASHI",
        indicator_code == "IND076" & value == 20.4 ~ "KINSHASA",
        indicator_code == "IND076" & value == 1.7  ~ "KIKWIT",
        indicator_code == "IND076" & value == 44.2 ~ "GOMA",
        indicator_code == "IND076" & value == 6.3  ~ "BUTEMBO",
        TRUE ~ ref_area
      )
    ) %>%
    dplyr::mutate(
      source = dplyr::if_else(indicator_code == "IND083", "Ministry of Finance of DRC", source)
    ) %>%
    dplyr::mutate(
      obs_status = dplyr::if_else(indicator_code == "IND049" & value == 8.4,  "Avant 15 ans", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND049" & value == 29.1, "Avant 18 ans", obs_status),
      source     = dplyr::if_else(indicator_code == "IND049", "MICS 2017 - 2018", source)
    ) %>%
    dplyr::mutate(
      source = dplyr::if_else(indicator_code == "IND089", "IMF", source)
    ) %>%
    dplyr::mutate(
      obs_status = dplyr::if_else(indicator_code == "IND080" & value == 58.88, "Femmes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND080" & value == 68.9,  "Hommes",  obs_status),
      source     = dplyr::if_else(indicator_code == "IND080", "MICS 2017 - 2018", source)
    ) 

  Data <- Data %>%
    dplyr::group_by(period, value) %>%
    dplyr::mutate(
      k = dplyr::row_number(),
      obs_status = dplyr::case_when(
        indicator_code == "IND084" & period == 2015 & value == 29 & k == 1 ~ "National",
        indicator_code == "IND084" & period == 2015 & value == 30 & k == 1 ~ "Hommes",
        indicator_code == "IND084" & period == 2015 & value == 28 & k == 1 ~ "Femmes",
        indicator_code == "IND084" & period == 2016 & value == 36 & k == 1 ~ "National",
        indicator_code == "IND084" & period == 2016 & value == 34 & k == 1 ~ "Hommes",
        indicator_code == "IND084" & period == 2016 & value == 38 & k == 1 ~ "Femmes",
        indicator_code == "IND084" & period == 2017 & value == 42 & k == 1 ~ "National",
        indicator_code == "IND084" & period == 2017 & value == 38 & k == 1 ~ "Hommes",
        indicator_code == "IND084" & period == 2017 & value == 47 & k == 1 ~ "Femmes",
        indicator_code == "IND084" & period == 2022 & value == 26 & k == 1 ~ "National",
        indicator_code == "IND084" & period == 2022 & value == 24 & k == 1 ~ "Hommes",
        indicator_code == "IND084" & period == 2022 & value == 28 & k == 1 ~ "Femmes",
        indicator_code == "IND084" & period == 2023 & value == 30 & k == 1 ~ "National",
        indicator_code == "IND084" & period == 2023 & value == 25 & k == 1 ~ "Hommes",
        indicator_code == "IND084" & period == 2023 & value == 35 & k == 1 ~ "Femmes",
        indicator_code == "IND084" & period == 2024 & value == 36 & k == 1 ~ "National",
        indicator_code == "IND084" & period == 2024 & value == 36 & k == 2 ~ "Hommes",
        indicator_code == "IND084" & period == 2024 & value == 36 & k == 3 ~ "Femmes",
        TRUE ~ obs_status
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-k)

  Data <- Data %>%
    dplyr::mutate(
      obs_status = dplyr::if_else(indicator_code == "IND079" & value == 5.55, "Femmes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND079" & value == 13.2, "Hommes", obs_status),
      source     = dplyr::if_else(indicator_code == "IND079", "MICS 2017 - 2018", source),
      indicator_name = dplyr::if_else(
        indicator_code == "IND079" & (value==5.55 | value==13.2),
        "16.1.3a Proportion de la population ayant subi des violences physiques au cours des 12 derniers mois",
        indicator_name
      ),
      indicator_code = dplyr::if_else(indicator_code == "IND079" & (value==10.66 | value==4.32), "IND127", indicator_code),
      indicator_code = dplyr::if_else(indicator_code == "IND079" & (value==5.66 | value==2.15), "IND128", indicator_code),
      obs_status = dplyr::if_else(indicator_code == "IND127" & value == 10.66, "Hommes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND127" & value == 4.32,  "Femmes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND128" & value == 5.66, "Hommes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND128" & value == 2.15, "Femmes", obs_status),
      indicator_name = dplyr::if_else(indicator_code == "IND127" & (value==10.66 | value==4.32),
        "16.1.3b Proportion de la population victime de vol au cours des 12 derniers mois", indicator_name),
      indicator_name = dplyr::if_else(indicator_code == "IND128" & (value==5.66 | value==2.15),
        "16.1.3c Proportion de la population ayant subi des agressions physiques au cours des 12 derniers mois", indicator_name)
    ) 

  Data <- Data %>%
    dplyr::mutate(
      obs_status = dplyr::if_else(indicator_code == "IND081" & value == 23.12, "Hommes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND081" & value == 21.66, "Femmes", obs_status),
      source     = dplyr::if_else(indicator_code == "IND081", "MICS 2017 - 2018", source),
      indicator_name = dplyr::if_else(indicator_code == "IND081" & (value==23.12 | value==21.66),
        "16.3.1a Taux de signalement des agressions physiques à la police au cours des 12 derniers mois", indicator_name),
      indicator_code = dplyr::if_else(indicator_code == "IND081" & (value==25.75 | value==14.99), "IND129", indicator_code),
      obs_status = dplyr::if_else(indicator_code == "IND129" & value == 25.75, "Hommes", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND129" & value == 14.99, "Femmes", obs_status),
      indicator_name = dplyr::if_else(indicator_code == "IND129" & (value==25.75 | value==14.99),
        "16.3.1b Taux de signalement des vols à la police au cours des 12 derniers mois", indicator_name)
    )

  Data <- Data %>%
    dplyr::mutate(
      obs_status = dplyr::if_else(indicator_code == "IND048" & value == 35.6, " 15 - 49 ans", obs_status),
      obs_status = dplyr::if_else(indicator_code == "IND048" & value == 31.9, "15+ ", obs_status)
    ) %>%
    dplyr::arrange(indicator_code) %>%
    dplyr::mutate(
      obs_status = dplyr::if_else(is.na(obs_status) | obs_status == "" | obs_status=="Float" | obs_status=="no_data", "RDC", obs_status)
    )

Data <- Data %>%
  mutate(
    indicator_name = if_else(
      indicator_code == "IND076",
      "11.2.1 Proportion de la population ayant aisément accès aux transports publics",
      indicator_name
    )
  )


Data <- Data %>%
  mutate(
    ref_area = case_when(
      indicator_code == "IND077" & value == 18  ~ "KINSHASA",
      indicator_code == "IND077" & value == 17.7  ~ "LUBUMBASHI",
      indicator_code == "IND077" & value == 16.2  ~ "BUTEMBO",
      indicator_code == "IND077" & value == 11.2 ~ "KOLWEZI",
      indicator_code == "IND077" & value == 8.8  ~ "KIKWIT",
      TRUE ~ ref_area
    )
  )



Data <- Data %>%
  mutate(
    indicator_name = if_else(
      indicator_code == "IND077",
      "11.7.1a Part moyenne de la surface bâtie des villes qui constitue un espace ouvert à l'usage public pour tous",
      indicator_name
    )
  )


Data <- Data %>%
  mutate(
    ref_area = case_when(
      indicator_code == "IND077" & value == 2.97013  ~ "KISANGANI",
      indicator_code == "IND077" & value == 10.43881  ~ "KIKWIT",
      indicator_code == "IND077" & value == 11.72893  ~ "KOLWEZI",
      indicator_code == "IND077" & value == 13.38906 ~ "BUTEMBO",
      indicator_code == "IND077" & value == 17.31111  ~ "KINSHASA",
      indicator_code == "IND077" & value == 6.78213  ~ "LUBUMBASHI",
      TRUE ~ ref_area
    )
  )


Data <- Data %>%
  mutate(
    indicator_code = case_when(
      indicator_code == "IND077" & value == 2.97013  ~ "IND130",
      indicator_code == "IND077" & value == 10.43881  ~ "IND130",
      indicator_code == "IND077" & value == 11.72893  ~ "IND130",
      indicator_code == "IND077" & value == 13.38906 ~ "IND130",
      indicator_code == "IND077" & value == 17.31111  ~ "IND130",
      indicator_code == "IND077" & value == 6.78213  ~ "IND130",
      TRUE ~ indicator_code
    )
  )


Data <- Data %>%
  mutate(
    indicator_name = if_else(
      indicator_code == "IND130",
      "11.7.1b Part moyenne de la population urbaine ayant un accès facile aux espaces publics ouverts",
      indicator_name
    )
  )


#Indicateur 47  

Data <- Data %>%
  group_by(indicator_code) %>%
  filter(!(indicator_code == "IND047") | row_number() <= 25) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND047" & row_number() <= 4,
      "4.a.1c Proportion d'écoles ayant accès à des ordinateurs à des fins pédagogiques, par niveau d'enseignement (%)",
      indicator_name
    )
  ) %>%
  ungroup()

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    obs_status = case_when(
      indicator_code == "IND047" & row_number() == 1 ~ "Enseignement primaire",
      indicator_code == "IND047" & row_number() == 2 ~ "Enseignement secondaire premier cycle",
      indicator_code == "IND047" & row_number() == 3 ~ "Enseignement secondaire deuxième cycle",
      indicator_code == "IND047" & row_number() == 4 ~ "Enseignement secondaire (global)",
      TRUE ~ obs_status
    )
  ) %>%
  ungroup()



Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND047" & (row_number() >= 5 & row_number() <= 11),
      "IND131",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND047" & (row_number() >= 5 & row_number() <= 14),
      "IND132",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND047" & (row_number() >= 5 & row_number() <= 8),
      "IND133",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND133" & row_number() == 4,
      "IND134",
      indicator_code
    )
  ) %>%
  ungroup()



# Les modalités à assigner dans l’ordre pour les 7 lignes
modalites_131 <- c(
  "Enseignement primaire",
  "Enseignement secondaire deuxième cycle",
  "Enseignement secondaire premier cycle",
  "Enseignement primaire",
  "Enseignement primaire",
  "Enseignement secondaire deuxième cycle",
  "Enseignement secondaire premier cycle"
)

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    # Modifier obs_status pour les 7 lignes de la modalité 131
    obs_status = ifelse(
      indicator_code == "IND131",
      modalites_131[row_number()],
      obs_status
    ),
    
    # Modifier indicator_name pour ces 7 lignes
    indicator_name = ifelse(
      indicator_code == "IND131",
      "4.a.1e Proportion d'écoles ayant accès à l'eau potable de base, par niveau d'éducation (%)",
      indicator_name
    )
  ) %>%
  ungroup()



# Modalités dans l'ordre exact pour les 10 lignes
modalites_132 <- c(
  "Enseignement secondaire premier cycle",
  "Enseignement primaire",
  "Enseignement secondaire deuxième cycle",
  "Enseignement primaire",
  "Enseignement primaire",
  "Enseignement secondaire deuxième cycle",
  "Enseignement secondaire premier cycle",
  "Enseignement secondaire premier cycle",
  "Enseignement secondaire deuxième cycle",
  "Enseignement primaire"
)

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    # Assigner obs_status uniquement aux 10 lignes de IND132
    obs_status = ifelse(
      indicator_code == "IND132",
      modalites_132[row_number()],
      obs_status
    ),
    
    # Mettre indicator_name pour les 10 lignes de IND132
    indicator_name = ifelse(
      indicator_code == "IND132",
      "4.a.1a Proportion d'écoles ayant accès à l'électricité, par niveau d'éducation (%)",
      indicator_name
    )
  ) %>%
  ungroup()


# Modalités pour les 3 lignes de IND133
modalites_133 <- c(
  "Enseignement secondaire premier cycle",
  "Enseignement primaire",
  "Enseignement secondaire deuxième cycle"
)

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    # Modifier obs_status pour IND133
    obs_status = ifelse(
      indicator_code == "IND133",
      modalites_133[row_number()],
      obs_status
    ),
    
    # Modifier indicator_name pour IND133
    indicator_name = ifelse(
      indicator_code == "IND133",
      "4.a.1g Proportion d'écoles disposant d'installations de lavage des mains de base, par niveau d'éducation (%)",
      indicator_name
    )
  ) %>%
  ungroup()



Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    # Modifier obs_status pour la ligne de IND134
    obs_status = ifelse(
      indicator_code == "IND134",
      "Enseignement secondaire (global)",
      obs_status
    ),
    
    # Modifier indicator_name pour IND134
    indicator_name = ifelse(
      indicator_code == "IND134",
      "4.a.1b Proportion d'établissements scolaires ayant accès à Internet à des fins pédagogiques, par niveau d'enseignement (%)",
      indicator_name
    )
  ) %>%
  ungroup()


#Indicateur 114 

Data <- Data %>%
  group_by(indicator_code) %>%
  filter(!(indicator_code == "IND114") | row_number() <= 25) %>%
  ungroup()

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND114" & row_number() <= 4,
      "1.3.1b Proportion de la population pauvre bénéficiant d'une aide sociale en espèces (%)",
      indicator_name
    )
  ) %>%
  ungroup()

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    obs_status = case_when(
      indicator_code == "IND114" & row_number() == 1 ~ "National",
      indicator_code == "IND114" & row_number() == 2 ~ "National",
      indicator_code == "IND114" & row_number() == 3 ~ "Femmes",
      indicator_code == "IND114" & row_number() == 4 ~ "Hommes",
      TRUE ~ obs_status
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND114" & (row_number() >= 5 & row_number() <= 10),
      "IND135",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND135",
      "1.3.1c Proportion de la population couverte par les programmes d'assurance sociale",
      indicator_name
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    obs_status = case_when(
      indicator_code == "IND135" & row_number() == 1 ~ "National",
      indicator_code == "IND135" & row_number() == 2 ~ "les plus pauvres",
      indicator_code == "IND135" & row_number() == 3 ~ "les plus pauvres",
      indicator_code == "IND135" & row_number() == 4 ~ "National",
      indicator_code == "IND135" & row_number() == 5 ~ "National",
      indicator_code == "IND135" & row_number() == 6 ~ "les plus pauvres",
      TRUE ~ obs_status
    )
  ) %>%
  ungroup()





Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND114" & (row_number() >= 5 & row_number() <= 9),
      "IND136",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND136",
      "1.3.1d Proportion d'enfants/de ménages bénéficiant d'une allocation familiale",
      indicator_name
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    obs_status = case_when(
      indicator_code == "IND136" & row_number() == 1 ~ "National (moins de 15 ans)",
      indicator_code == "IND136" & row_number() == 2 ~ "National (moins de 15 ans)",
      indicator_code == "IND136" & row_number() == 3 ~ "National (15 ans +)",
      indicator_code == "IND136" & row_number() == 4 ~ "Femmes (15 ans +)",
      indicator_code == "IND136" & row_number() == 5 ~ "Hommes (15 ans +)",
      TRUE ~ obs_status
    )
  ) %>%
  ungroup()







Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND114" & (row_number() >= 5 & row_number() <= 6),
      "IND137",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND137",
      "1.3.1e Proportion de la population vulnérable bénéficiant d'une aide sociale en espèces",
      indicator_name
    )
  ) %>%
  ungroup()





Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND114" & (row_number() >= 5 & row_number() <= 10),
      "IND138",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND138",
      "1.3.1f Proportion de la population active couverte en cas d'accident du travail",
      indicator_name
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    obs_status = case_when(
      indicator_code == "IND138" & row_number() == 1 ~ "National (15 ans +)",
      indicator_code == "IND138" & row_number() == 2 ~ "National (15 ans +)",
      indicator_code == "IND138" & row_number() == 3 ~ "National (15 ans +)",
      indicator_code == "IND138" & row_number() == 4 ~ "National (15 ans +)",
      indicator_code == "IND138" & row_number() == 5 ~ "Femmes (15 ans +)",
      indicator_code == "IND138" & row_number() == 6 ~ "Hommes (15 ans +)",
      TRUE ~ obs_status
    )
  ) %>%
  ungroup()





Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND114" & row_number() == 5,
      "IND139",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND139",
      "1.3.1g Proportion de la population couverte par au moins une prestation de protection sociale",
      indicator_name
    )
  ) %>%
  ungroup()




Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_code = ifelse(
      indicator_code == "IND114" & row_number() == 5,
      "IND140",
      indicator_code
    )
  ) %>%
  ungroup()


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND140",
      "1.3.1h Proportion de la population ayant atteint l'âge légal de la retraite et percevant une pension",
      indicator_name
    )
  ) %>%
  ungroup()




# Indicateur 37   

Data <- Data %>%
  group_by(indicator_code) %>%
  filter(!(indicator_code == "IND037") | row_number() <= 25) %>%
  ungroup()

Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    indicator_name = ifelse(
      indicator_code == "IND037",
      "4.3.1 Taux de participation à l'éducation et à la formation formelles et non formelles",
      indicator_name
    )
  ) %>%
  ungroup()
 
modalites_037 <- c(
  "Femmes (15-64 ans)",
  "Hommes (15-64 ans)",
  "National (25-54 ans)",
  "Femmes (25-54 ans)",
  "Hommes (55-64 ans)",
  "Hommes (25-54 ans)",
  "National (55-64 ans)",
  "Femmes (55-64 ans)",
  "National (15-64 ans)",
  "Hommes (15-24 ans)",
  "National (15-24 ans)",
  "Femmes (15-24 ans)",
  "Femmes (15-24 ans)",
  "National (15-24 ans)",
  "Hommes (15-24 ans)",
  "National (15-64 ans)",
  "Hommes (25-54 ans)",
  "Femmes (25-54 ans)",
  "National (25-54 ans)",
  "Hommes (15-64 ans)",
  "Femmes (15-64 ans)",
  "Femmes (15-64 ans)",
  "Hommes (15-64 ans)",
  "National (25-54 ans)",
  "Femmes (25-54 ans)"
)


Data <- Data %>%
  group_by(indicator_code) %>%
  mutate(
    obs_status = ifelse(
      indicator_code == "IND037",
      modalites_037[row_number()],
      obs_status
    )
  ) %>%
  ungroup()
 
Data <- Data %>%
  mutate(
    value = case_when(
      indicator_code == "IND107" & period == 1980 ~ 40.039,
      indicator_code == "IND107" & period == 1981 ~ 34.884,
      indicator_code == "IND107" & period == 1982 ~ 37.069,
      indicator_code == "IND107" & period == 1983 ~ 76.730,
      indicator_code == "IND107" & period == 1984 ~ 52.630,
      indicator_code == "IND107" & period == 1985 ~ 23.460,
      indicator_code == "IND107" & period == 1986 ~ 46.730,
      indicator_code == "IND107" & period == 1987 ~ 90.370,
      indicator_code == "IND107" & period == 1988 ~ 82.749,
      indicator_code == "IND107" & period == 1989 ~ 104.065,
      indicator_code == "IND107" & period == 1990 ~ 81.295,
      indicator_code == "IND107" & period == 1991 ~ 2154.440,
      indicator_code == "IND107" & period == 1992 ~ 4129.170,
      indicator_code == "IND107" & period == 1993 ~ 1986.900,
      indicator_code == "IND107" & period == 1994 ~ 23773.100,
      indicator_code == "IND107" & period == 1995 ~ 541.801,
      indicator_code == "IND107" & period == 1996 ~ 617.000,
      indicator_code == "IND107" & period == 1997 ~ 199.000,
      indicator_code == "IND107" & period == 1998 ~ 29.145,
      indicator_code == "IND107" & period == 1999 ~ 284.900,
      indicator_code == "IND107" & period == 2000 ~ 550.000,
      indicator_code == "IND107" & period == 2001 ~ 357.280,
      indicator_code == "IND107" & period == 2002 ~ 25.316,
      indicator_code == "IND107" & period == 2003 ~ 12.817,
      indicator_code == "IND107" & period == 2004 ~ 4.001,
      indicator_code == "IND107" & period == 2005 ~ 21.545,
      indicator_code == "IND107" & period == 2006 ~ 12.845,
      indicator_code == "IND107" & period == 2007 ~ 16.672,
      indicator_code == "IND107" & period == 2008 ~ 17.966,
      indicator_code == "IND107" & period == 2009 ~ 46.101,
      indicator_code == "IND107" & period == 2010 ~ 23.461,
      indicator_code == "IND107" & period == 2011 ~ 14.950,
      indicator_code == "IND107" & period == 2012 ~ 0.857,
      indicator_code == "IND107" & period == 2013 ~ 0.876,
      indicator_code == "IND107" & period == 2014 ~ 1.240,
      indicator_code == "IND107" & period == 2015 ~ 0.738,
      indicator_code == "IND107" & period == 2016 ~ 3.193,
      indicator_code == "IND107" & period == 2017 ~ 35.734,
      indicator_code == "IND107" & period == 2018 ~ 29.266,
      indicator_code == "IND107" & period == 2019 ~ 4.705,
      indicator_code == "IND107" & period == 2020 ~ 11.358,
      indicator_code == "IND107" & period == 2021 ~ 8.989,
      indicator_code == "IND107" & period == 2022 ~ 9.266,
      indicator_code == "IND107" & period == 2023 ~ 19.894,
      indicator_code == "IND107" & period == 2024 ~ 17.653,
      TRUE ~ value
    )
  )

Data <- Data %>%
  mutate(
    source = if_else(
      indicator_code == "IND107",
      "IMF",
      source
    )
  ) 

# --- 1) Coller les nouvelles données ici (copie/colle tel quel) ---
txt <- "
indicator_code\tref_area\tperiod\tvalue\tobs_status\tsource\twb_indicator\tindicator_name
IND145\tCOD\t2015\t23 456 580,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)
IND145\tCOD\t2016\t35 327 647,8333333\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)
IND145\tCOD\t2017\t28 949 487,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)
IND145\tCOD\t2018\t51 101 353,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)
IND145\tCOD\t2019\t48 754 272,6666666\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)
IND145\tCOD\t2020\t31 730 788,1666666\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)
IND145\tCOD\t2021\t54 891 057,1666666\tRDC\tAIDDATA\tNA\tFinancement ODD1 (USD constant)

IND146\tCOD\t2015\t204 917 723,0833330\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)
IND146\tCOD\t2016\t230 021 577,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)
IND146\tCOD\t2017\t333 804 742,8333330\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)
IND146\tCOD\t2018\t417 882 034,6666660\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)
IND146\tCOD\t2019\t554 522 470,5416660\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)
IND146\tCOD\t2020\t490 714 794,1666670\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)
IND146\tCOD\t2021\t609 696 705,4166660\tRDC\tAIDDATA\tNA\tFinancement ODD2 (USD constant)

IND147\tCOD\t2015\t669 293 935,2500000\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)
IND147\tCOD\t2016\t636 146 627,4166670\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)
IND147\tCOD\t2017\t716 266 692,9166670\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)
IND147\tCOD\t2018\t706 936 741,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)
IND147\tCOD\t2019\t1 037 581 097,5833300\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)
IND147\tCOD\t2020\t1 156 462 791,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)
IND147\tCOD\t2021\t851 102 991,4166660\tRDC\tAIDDATA\tNA\tFinancement ODD3 (USD constant)

IND148\tCOD\t2015\t66 713 815,3333333\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)
IND148\tCOD\t2016\t106 544 488,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)
IND148\tCOD\t2017\t132 099 786,3333330\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)
IND148\tCOD\t2018\t162 113 217,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)
IND148\tCOD\t2019\t141 000 623,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)
IND148\tCOD\t2020\t137 925 928,8333330\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)
IND148\tCOD\t2021\t211 207 545,4999990\tRDC\tAIDDATA\tNA\tFinancement ODD4 (USD constant)

IND149\tCOD\t2015\t77 006 598,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)
IND149\tCOD\t2016\t91 337 234,5833332\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)
IND149\tCOD\t2017\t95 451 804,5833332\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)
IND149\tCOD\t2018\t83 514 672,9166666\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)
IND149\tCOD\t2019\t83 013 369,2916666\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)
IND149\tCOD\t2020\t68 023 738,1666666\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)
IND149\tCOD\t2021\t78 112 758,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD5 (USD constant)

IND150\tCOD\t2015\t140 050 552,1666670\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)
IND150\tCOD\t2016\t102 048 077,1666670\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)
IND150\tCOD\t2017\t87 739 185,1666665\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)
IND150\tCOD\t2018\t129 185 873,8333330\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)
IND150\tCOD\t2019\t146 356 132,9166670\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)
IND150\tCOD\t2020\t146 471 368,8333330\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)
IND150\tCOD\t2021\t167 545 118,6666660\tRDC\tAIDDATA\tNA\tFinancement ODD6 (USD constant)

IND151\tCOD\t2015\t188 356 474,8333330\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)
IND151\tCOD\t2016\t218 046 040,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)
IND151\tCOD\t2017\t105 142 992,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)
IND151\tCOD\t2018\t143 052 609,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)
IND151\tCOD\t2019\t52 115 161,2500000\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)
IND151\tCOD\t2020\t34 332 201,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)
IND151\tCOD\t2021\t40 783 203,2500000\tRDC\tAIDDATA\tNA\tFinancement ODD7 (USD constant)

IND152\tCOD\t2015\t33 201 842,1666666\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)
IND152\tCOD\t2016\t47 368 359,3333333\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)
IND152\tCOD\t2017\t53 628 518,3333333\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)
IND152\tCOD\t2018\t61 162 339,8333333\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)
IND152\tCOD\t2019\t57 542 687,6666667\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)
IND152\tCOD\t2020\t27 775 831,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)
IND152\tCOD\t2021\t33 581 684,1666667\tRDC\tAIDDATA\tNA\tFinancement ODD8 (USD constant)

IND153\tCOD\t2015\t107 330 167,3333330\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)
IND153\tCOD\t2016\t96 020 739,5833333\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)
IND153\tCOD\t2017\t101 440 951,4166670\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)
IND153\tCOD\t2018\t77 468 659,0833333\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)
IND153\tCOD\t2019\t91 750 946,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)
IND153\tCOD\t2020\t70 820 690,7500000\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)
IND153\tCOD\t2021\t106 827 674,9166670\tRDC\tAIDDATA\tNA\tFinancement ODD9 (USD constant)

IND154\tCOD\t2015\t6 223 030,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)
IND154\tCOD\t2016\t3 836 417,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)
IND154\tCOD\t2017\t10 689 131,1666667\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)
IND154\tCOD\t2018\t42 299 670,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)
IND154\tCOD\t2019\t35 365 764,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)
IND154\tCOD\t2020\t13 414 037,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)
IND154\tCOD\t2021\t5 584 901,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD10 (USD constant)

IND155\tCOD\t2015\t152 974 900,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)
IND155\tCOD\t2016\t127 037 452,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)
IND155\tCOD\t2017\t148 439 639,3333330\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)
IND155\tCOD\t2018\t95 821 648,6666667\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)
IND155\tCOD\t2019\t87 324 838,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)
IND155\tCOD\t2020\t80 578 399,1666666\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)
IND155\tCOD\t2021\t98 789 961,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD11 (USD constant)

IND156\tCOD\t2015\t16 995 625,6666667\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)
IND156\tCOD\t2016\t13 516 241,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)
IND156\tCOD\t2017\t8 057 069,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)
IND156\tCOD\t2018\t12 606 800,6666667\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)
IND156\tCOD\t2019\t4 842 393,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)
IND156\tCOD\t2020\t1 046 629,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)
IND156\tCOD\t2021\t2 927 625,1666667\tRDC\tAIDDATA\tNA\tFinancement ODD12 (USD constant)

IND157\tCOD\t2015\t7 334 311,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)
IND157\tCOD\t2016\t5 921 563,6666667\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)
IND157\tCOD\t2017\t2 026 789,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)
IND157\tCOD\t2018\t980 410,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)
IND157\tCOD\t2019\t1 997 799,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)
IND157\tCOD\t2020\t2 301 917,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)
IND157\tCOD\t2021\t1 010 151,5000000\tRDC\tAIDDATA\tNA\tFinancement ODD13 (USD constant)

IND158\tCOD\t2015\t1 138 221,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD14 (USD constant)
IND158\tCOD\t2016\t1 146 637,0000000\tRDC\tAIDDATA\tNA\tFinancement ODD14 (USD constant)

IND159\tCOD\t2015\t36 508 806\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)
IND159\tCOD\t2016\t41 857 211\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)
IND159\tCOD\t2017\t48 203 822\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)
IND159\tCOD\t2018\t68 317 481\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)
IND159\tCOD\t2019\t62 678 792\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)
IND159\tCOD\t2020\t83 202 353\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)
IND159\tCOD\t2021\t91 730 148\tRDC\tAIDDATA\tNA\tFinancement ODD15 (USD constant)

IND160\tCOD\t2015\t242 423 934\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)
IND160\tCOD\t2016\t206 651 698\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)
IND160\tCOD\t2017\t245 617 014\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)
IND160\tCOD\t2018\t256 638 615\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)
IND160\tCOD\t2019\t215 544 598\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)
IND160\tCOD\t2020\t232 760 808\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)
IND160\tCOD\t2021\t257 498 306\tRDC\tAIDDATA\tNA\tFinancement ODD16 (USD constant)

IND161\tCOD\t2015\t174 445 290\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)
IND161\tCOD\t2016\t190 072 816\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)
IND161\tCOD\t2017\t127 750 281\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)
IND161\tCOD\t2018\t88 785 003\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)
IND161\tCOD\t2019\t98 302 967\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)
IND161\tCOD\t2020\t90 434 202\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)
IND161\tCOD\t2021\t27 829 131\tRDC\tAIDDATA\tNA\tFinancement ODD17 (USD constant)

IND162\tCOD\t2015\t2 148 371 808\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
IND162\tCOD\t2016\t2 152 900 830\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
IND162\tCOD\t2017\t2 245 307 908\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
IND162\tCOD\t2018\t2 397 867 131\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
IND162\tCOD\t2019\t2 718 693 913\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
IND162\tCOD\t2020\t2 667 996 478\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
IND162\tCOD\t2021\t2 639 118 963\tRDC\tAIDDATA\tNA\tFinancement ensemble ODD (USD constant)
"

# --- 2) Convertir le texte en data.frame ---
new_data <- read_delim(I(txt), delim = "\t", trim_ws = TRUE, show_col_types = FALSE)

# --- 3) Nettoyer value (espaces milliers + virgule décimale) ---
new_data <- new_data %>%
  mutate(
    period = as.integer(period),
    value = value %>%
      as.character() %>%
      str_squish() %>%
      str_replace_all(" ", "") %>%
      str_replace(",", ".") %>%
      na_if("") %>%
      as.numeric()
  )

# --- 4) INSÉRER dans ta base (ajout de nouvelles lignes) ---
Data <- Data %>% bind_rows(new_data)

# (optionnel) éviter doublons exacts si tu relances le code
# Data <- Data %>% distinct()


Data <- Data %>%
  mutate(
    ref_area = case_when(
      indicator_code == "IND179" & value == 0.522  ~ "LUBUMBASHI",
      indicator_code == "IND179" & value == 0.810  ~ "KISANGANI",
      indicator_code == "IND179" & value == 0.786  ~ "KINSHASA",
      indicator_code == "IND179" & value == 0.887  ~ "GOMA",
      indicator_code == "IND179" & value == 1.566  ~ "GOMA",
      indicator_code == "IND179" & value == 0.599  ~ "KINSHASA",
      indicator_code == "IND179" & value == 0.882  ~ "KISANGANI",
      indicator_code == "IND179" & value == 0.805  ~ "LUBUMBASHI",
      TRUE ~ ref_area
    )
  )

Data <- Data %>%
  mutate(
    ref_area = case_when(
      indicator_code == "IND180" & value == 7.00  ~ "BUKAVU",
      indicator_code == "IND180" & value == 2.00  ~ "KINSHASA",
      TRUE ~ ref_area
    )
  )


# (optionnel) trier
Data <- Data %>% arrange(indicator_code)

  Data 
}          

# ------------------- Définition du pipeline -------------------
list(
  tar_target(
    cat,
    {
      path_cat <- if (file.exists("../catalogue/catalogue_enriched.csv")) "../catalogue/catalogue_enriched.csv" else "catalogue/catalogue_enriched.csv"
      load_catalogue(path_cat)
    }
  ),
  tar_target(
    api_raw,
    ingest_api_all(cat)
  ),
  tar_target(
    data_final,
    ensure_schema(api_raw)
  ),
  tar_target(
    data_named,
    add_indicator_names2(data_final, cat)
  ),
  tar_target(
    export_latest,
    {
      # Écrit systématiquement un CSV canonique, sans dépendre d'un objet 'out'
      path <- "data_final/export_latest.csv"
      dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
      readr::write_csv(data_named, path)
      path
    },
    format = "file"
  ),
  tar_target(
    export_latest_transformed,
    {
      df <- readr::read_csv(export_latest, show_col_types = FALSE)
      df2 <- postprocess_export(df)
      readr::write_csv(df2, export_latest)
      export_latest
    },
    format = "file"
  ),
  tar_target(
    db_upsert,
    {
      to_db(export_latest_transformed)
      "ok"
    }
  )
)
