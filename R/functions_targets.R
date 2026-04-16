build_base_spells <- function(path) {
  readRDS(path) |>
    dplyr::as_tibble() |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(
      ddbid = as.character(ddbid),
      kon = as.factor(kon),
      fod_date = as.Date(fod_date),
      dod_date = as.Date(dod_date),
      bob_date = as.Date(bob_date),
      bos_date = as.Date(bos_date),
      fod_ar = as.integer(format(fod_date, "%Y")),
      dod_ar = as.integer(format(dod_date, "%Y"))
    ) |>
    dplyr::filter(!is.na(ddbid), !is.na(fod_date))
}

collapse_to_individual <- function(dat) {
  dat |>
    dplyr::arrange(ddbid, bob_date, bos_date) |>
    dplyr::group_by(ddbid) |>
    dplyr::summarise(
      kon = dplyr::first(kon),
      fod_date = dplyr::first(fod_date),
      dod_date = dplyr::first(dod_date),
      fod_ar = dplyr::first(fod_ar),
      dod_ar = dplyr::first(dod_ar),
      n_spells = dplyr::n(),
      ever_hisco = any(!is.na(hiscostatus)),
      hiscostatus_first = dplyr::first(hiscostatus[!is.na(hiscostatus)], default = NA_integer_),
      first_bofrs = dplyr::first(bofrs),
      first_bofrsnmn = dplyr::first(bofrsnmn),
      first_lat = dplyr::first(lat),
      first_lon = dplyr::first(lon),
      .groups = "drop"
    )
}

get_crisis_window <- function(case) {
  if (case == "war") {
    tibble::tibble(
      crisis_case = "war",
      crisis_start = as.Date("1808-01-01"),
      crisis_end   = as.Date("1809-12-31"),
      birth_start  = 1808L,
      birth_end    = 1809L
    )
  } else if (case == "famine") {
    tibble::tibble(
      crisis_case = "famine",
      crisis_start = as.Date("1866-01-01"),
      crisis_end   = as.Date("1868-12-31"),
      birth_start  = 1866L,
      birth_end    = 1868L
    )
  } else {
    stop("Unknown case: ", case, call. = FALSE)
  }
}

define_birth_exposure <- function(indiv_data, crisis_window, case) {
  birth_start <- crisis_window$birth_start[[1]]
  birth_end   <- crisis_window$birth_end[[1]]
  
  indiv_data |>
    dplyr::mutate(
      crisis_case = case,
      exposed_birth = dplyr::between(fod_ar, birth_start, birth_end)
    )
}

exposure_from_spells <- function(spells, crisis_window, case) {
  crisis_start <- crisis_window$crisis_start[[1]]
  crisis_end   <- crisis_window$crisis_end[[1]]
  
  spells |>
    dplyr::mutate(
      crisis_case = case,
      overlap_crisis = !is.na(bob_date) & !is.na(bos_date) &
        !(bos_date < crisis_start | bob_date > crisis_end),
      spell_duration_days = as.numeric(bos_date - bob_date),
      spell_duration_days = dplyr::if_else(
        is.na(spell_duration_days),
        NA_real_,
        pmax(spell_duration_days, 0)
      )
    ) |>
    dplyr::group_by(ddbid) |>
    dplyr::summarise(
      crisis_case = dplyr::first(crisis_case),
      exposed_spatial = any(overlap_crisis, na.rm = TRUE),
      n_overlapping_spells = sum(overlap_crisis, na.rm = TRUE),
      first_crisis_bofrs = dplyr::first(bofrs[overlap_crisis], default = NA_integer_),
      first_crisis_bofrsnmn = dplyr::first(bofrsnmn[overlap_crisis], default = NA_character_),
      first_crisis_lat = dplyr::first(lat[overlap_crisis], default = NA_real_),
      first_crisis_lon = dplyr::first(lon[overlap_crisis], default = NA_real_),
      .groups = "drop"
    )
}

combine_individual_and_spell_exposure <- function(indiv_exposure, spell_exposure, case) {
  indiv_exposure |>
    dplyr::left_join(
      spell_exposure |>
        dplyr::select(
          ddbid,
          exposed_spatial,
          n_overlapping_spells,
          first_crisis_bofrs,
          first_crisis_bofrsnmn,
          first_crisis_lat,
          first_crisis_lon
        ),
      by = "ddbid"
    ) |>
    dplyr::mutate(
      crisis_case = case,
      exposed_spatial = dplyr::coalesce(exposed_spatial, FALSE),
      n_overlapping_spells = dplyr::coalesce(n_overlapping_spells, 0L)
    )
}

make_descriptive_table <- function(dat, case) {
  dat |>
    dplyr::summarise(
      case = case,
      n_individuals = dplyr::n(),
      n_birth_exposed = sum(exposed_birth, na.rm = TRUE),
      n_spatial_exposed = sum(exposed_spatial, na.rm = TRUE),
      mean_spells = mean(n_spells, na.rm = TRUE),
      median_spells = stats::median(n_spells, na.rm = TRUE),
      female_share = mean(kon == levels(kon)[2], na.rm = TRUE)
    )
}

make_cohort_plot <- function(dat, case) {
  dat |>
    dplyr::count(fod_ar, exposed_birth, name = "n") |>
    ggplot2::ggplot(
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
  dat |>
    dplyr::count(fod_ar, exposed_spatial, name = "n") |>
    ggplot2::ggplot(
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
  model_data <- dat |>
    dplyr::filter(
      !is.na(exposed_birth),
      !is.na(kon),
      !is.na(n_spells)
    ) |>
    dplyr::mutate(
      kon = as.factor(kon)
    )
  
  stats::glm(
    exposed_birth ~ kon + n_spells,
    data = model_data,
    family = stats::binomial()
  )
}

fit_main_model_spatial <- function(dat, case) {
  model_data <- dat |>
    dplyr::filter(
      !is.na(exposed_spatial),
      !is.na(kon),
      !is.na(n_spells)
    ) |>
    dplyr::mutate(
      kon = as.factor(kon)
    )
  
  stats::glm(
    exposed_spatial ~ kon + n_spells,
    data = model_data,
    family = stats::binomial()
  )
}

tidy_model <- function(model, case, model_name) {
  broom::tidy(model) |>
    dplyr::mutate(
      case = case,
      model = model_name
    )
}