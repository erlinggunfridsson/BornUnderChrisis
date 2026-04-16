# library(targets)
# tar_manifest()
# tar_destroy(destroy = "all") # Kör om
# tar_make()
# tar_read(descriptive_table)
# tar_read(model_table_birth)
# tar_read(model_table_spatial)
# tar_errored() # Om fel

# spells <- tar_read(base_spells) #returnerar objektet direkt
# tar_load(base_spells) #lägger objektet i din globala miljö med namnet base_spells
# indiv_data <- tar_read(indiv_data)

# forsamlingar <- unique(spells[boregprefix == "NOS", .(ddbid, bofrs, bofrsnmn)])[
#   , .N, by = .(bofrs, bofrsnmn)
# ][order(-N)]
# 
# forsamlingar_namn <- (forsamlingar$bofrsnmn)
# 
# forsamlingar_SKE <- c( "SKELLEFTEÅ", "BURTRÄSK", "BYGDEÅ", "BYSKE", "LÖVÅNGER", "NYSÄTRA",  "NORSJÖ", "JÖRN", "ROBERTSFORS", "YTTERSTFORS", "SKELLEFTEÅ SANKT OLOF" "FÄLLFORS")

filter_to_region <- function(spells, parishes) {
  DT <- data.table::copy(spells)
  
  DT <- DT[
    boregprefix == "NOS" &
      bofrsnmn %in% parishes
  ]
  
  DT[]
}

build_base_spells <- function(path) {
  DT <- data.table::as.data.table(readRDS(path))
  DT <- data.table::copy(DT)
  
  data.table::setnames(DT, old = names(DT), new = tolower(names(DT)))
  
  DT[, ddbid := as.character(ddbid)]
  DT[, kon := as.factor(kon)]
  
  DT[, fod_date := as.Date(fod_date)]
  DT[, dod_date := as.Date(dod_date)]
  DT[, bob_date := as.Date(bob_date)]
  DT[, bos_date := as.Date(bos_date)]
  
  DT[, fod_ar := as.integer(format(fod_date, "%Y"))]
  DT[, dod_ar := as.integer(format(dod_date, "%Y"))]
  
  DT <- DT[!is.na(ddbid) & !is.na(fod_date)]
  
  data.table::setorder(DT, ddbid, bob_date, bos_date)
  DT[]
}

collapse_to_individual <- function(DT) {
  DT <- data.table::copy(DT)
  
  out <- DT[
    ,
    .(
      kon = data.table::first(kon),
      fod_date = data.table::first(fod_date),
      dod_date = data.table::first(dod_date),
      fod_ar = data.table::first(fod_ar),
      dod_ar = data.table::first(dod_ar),
      n_spells = .N,
      ever_hisco = any(!is.na(hiscostatus)),
      hiscostatus_first = {
        x <- hiscostatus[!is.na(hiscostatus)]
        if (length(x) == 0L) NA_integer_ else x[1L]
      },
      first_bofrs = data.table::first(bofrs),
      first_bofrsnmn = data.table::first(bofrsnmn),
      first_lat = data.table::first(lat),
      first_lon = data.table::first(lon)
    ),
    by = ddbid
  ]
  
  out[]
}

get_crisis_window <- function(case) {
  if (case == "war") {
    out <- data.table::data.table(
      crisis_case = "war",
      crisis_start = as.Date("1808-01-01"),
      crisis_end   = as.Date("1809-12-31"),
      birth_start  = 1808L,
      birth_end    = 1809L
    )
  } else if (case == "famine") {
    out <- data.table::data.table(
      crisis_case = "famine",
      crisis_start = as.Date("1866-01-01"),
      crisis_end   = as.Date("1868-12-31"),
      birth_start  = 1866L,
      birth_end    = 1868L
    )
  } else {
    stop("Unknown case: ", case, call. = FALSE)
  }
  
  out[]
}

define_birth_exposure <- function(indiv_data, crisis_window, case) {
  DT <- data.table::copy(indiv_data)
  
  birth_start <- crisis_window$birth_start[[1]]
  birth_end   <- crisis_window$birth_end[[1]]
  
  DT[, crisis_case := case]
  DT[, exposed_birth := data.table::between(fod_ar, birth_start, birth_end)]
  
  DT[]
}

exposure_from_spells <- function(spells, crisis_window, case) {
  DT <- data.table::copy(spells)
  
  crisis_start <- crisis_window$crisis_start[[1]]
  crisis_end   <- crisis_window$crisis_end[[1]]
  
  DT[, crisis_case := case]
  
  DT[
    ,
    overlap_crisis := !is.na(bob_date) &
      !is.na(bos_date) &
      !(bos_date < crisis_start | bob_date > crisis_end)
  ]
  
  DT[
    ,
    spell_duration_days := as.numeric(bos_date - bob_date)
  ]
  
  DT[
    ,
    spell_duration_days := fifelse(
      is.na(spell_duration_days),
      NA_real_,
      pmax(spell_duration_days, 0)
    )
  ]
  
  out <- DT[
    ,
    .(
      crisis_case = data.table::first(crisis_case),
      exposed_spatial = any(overlap_crisis, na.rm = TRUE),
      n_overlapping_spells = sum(overlap_crisis, na.rm = TRUE),
      first_crisis_bofrs = {
        x <- bofrs[overlap_crisis]
        if (length(x) == 0L) NA_integer_ else x[1L]
      },
      first_crisis_bofrsnmn = {
        x <- bofrsnmn[overlap_crisis]
        if (length(x) == 0L) NA_character_ else x[1L]
      },
      first_crisis_lat = {
        x <- lat[overlap_crisis]
        if (length(x) == 0L) NA_real_ else x[1L]
      },
      first_crisis_lon = {
        x <- lon[overlap_crisis]
        if (length(x) == 0L) NA_real_ else x[1L]
      }
    ),
    by = ddbid
  ]
  
  out[]
}

combine_individual_and_spell_exposure <- function(indiv_exposure, spell_exposure, case) {
  DT_indiv <- data.table::copy(indiv_exposure)
  DT_spell <- data.table::copy(spell_exposure)
  
  out <- DT_spell[
    DT_indiv,
    on = "ddbid"
  ]
  
  out[, crisis_case := case]
  
  out[is.na(exposed_spatial), exposed_spatial := FALSE]
  out[is.na(n_overlapping_spells), n_overlapping_spells := 0L]
  
  data.table::setcolorder(
    out,
    c(
      "ddbid", "crisis_case", "kon", "fod_date", "dod_date", "fod_ar", "dod_ar",
      "n_spells", "ever_hisco", "hiscostatus_first",
      "first_bofrs", "first_bofrsnmn", "first_lat", "first_lon",
      "exposed_birth", "exposed_spatial", "n_overlapping_spells",
      "first_crisis_bofrs", "first_crisis_bofrsnmn",
      "first_crisis_lat", "first_crisis_lon"
    )[c(
      "ddbid", "crisis_case", "kon", "fod_date", "dod_date", "fod_ar", "dod_ar",
      "n_spells", "ever_hisco", "hiscostatus_first",
      "first_bofrs", "first_bofrsnmn", "first_lat", "first_lon",
      "exposed_birth", "exposed_spatial", "n_overlapping_spells",
      "first_crisis_bofrs", "first_crisis_bofrsnmn",
      "first_crisis_lat", "first_crisis_lon"
    ) %in% names(out)]
  )
  
  out[]
}

make_descriptive_table <- function(dat, case) {
  DT <- data.table::copy(dat)
  
  female_level <- if (is.factor(DT$kon) && length(levels(DT$kon)) >= 2L) levels(DT$kon)[2L] else NA
  
  out <- data.table::data.table(
    case = case,
    n_individuals = nrow(DT),
    n_birth_exposed = DT[, sum(exposed_birth, na.rm = TRUE)],
    n_spatial_exposed = DT[, sum(exposed_spatial, na.rm = TRUE)],
    mean_spells = DT[, mean(n_spells, na.rm = TRUE)],
    median_spells = DT[, stats::median(n_spells, na.rm = TRUE)],
    female_share = if (!is.na(female_level)) DT[, mean(kon == female_level, na.rm = TRUE)] else NA_real_
  )
  
  out[]
}

make_cohort_plot <- function(dat, case) {
  DT <- data.table::copy(dat)
  
  plot_data <- DT[
    ,
    .(n = .N),
    by = .(fod_ar, exposed_birth)
  ]
  
  ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x = fod_ar,
      y = n,
      linetype = exposed_birth
    )
  ) +
    ggplot2::geom_line() +
    ggplot2::labs(
      title = paste("Birth exposure:", case),
      x = "Födelseår",
      y = "Antal"
    )
}

make_spatial_exposure_plot <- function(dat, case) {
  DT <- data.table::copy(dat)
  
  plot_data <- DT[
    ,
    .(n = .N),
    by = .(fod_ar, exposed_spatial)
  ]
  
  ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x = fod_ar,
      y = n,
      linetype = exposed_spatial
    )
  ) +
    ggplot2::geom_line() +
    ggplot2::labs(
      title = paste("Spatial exposure:", case),
      x = "Födelseår",
      y = "Antal"
    )
}

fit_main_model_birth <- function(dat, case) {
  DT <- data.table::copy(dat)
  
  model_data <- DT[
    !is.na(exposed_birth) &
      !is.na(kon) &
      !is.na(n_spells)
  ]
  
  model_data[, kon := as.factor(kon)]
  
  stats::glm(
    exposed_birth ~ kon + n_spells,
    data = model_data,
    family = stats::binomial()
  )
}

fit_main_model_spatial <- function(dat, case) {
  DT <- data.table::copy(dat)
  
  model_data <- DT[
    !is.na(exposed_spatial) &
      !is.na(kon) &
      !is.na(n_spells)
  ]
  
  model_data[, kon := as.factor(kon)]
  
  stats::glm(
    exposed_spatial ~ kon + n_spells,
    data = model_data,
    family = stats::binomial()
  )
}

tidy_model <- function(model, case, model_name) {
  out <- data.table::as.data.table(broom::tidy(model))
  out[, case := case]
  out[, model := model_name]
  out[]
}