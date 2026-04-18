library(targets)
library(tarchetypes)

popum_dir <- "/home/erling/gitstuff/POPUM_DDB"

tar_option_set(
  packages = c(
    "data.table",
    "ggplot2",
    "broom",
    "survival"
  )
)

source("R/functions_targets.R")

cases <- data.table::data.table(
  case = c("war", "famine")
)

forsamlingar_ske <- c(
  "SKELLEFTEÅ",
  "BURTRÄSK",
  "BYGDEÅ",
  "BYSKE",
  "LÖVÅNGER",
  "NYSÄTRA",
  "NORSJÖ",
  "JÖRN",
  "ROBERTSFORS",
  "YTTERSTFORS",
  "SKELLEFTEÅ SANKT OLOF",
  "FÄLLFORS"
)

list(
  tar_target(
    raw_individ_file,
    file.path(popum_dir, "POPUM_person.csv"),
    format = "file"
  ),
  
  tar_target(
    raw_boende_file,
    file.path(popum_dir, "POPUM_BOFRSORT.csv"),
    format = "file"
  ),
  
  tar_target(
    raw_kodort_file,
    file.path(popum_dir, "POPUM_KODORTKOD.csv"),
    format = "file"
  ),
  
  tar_target(
    raw_yrke_file,
    file.path(popum_dir, "POPUM_YRKE.csv"),
    format = "file"
  ),
  
  tar_target(
    raw_lyte_file,
    file.path(popum_dir, "POPUM_lyte.csv"),
    format = "file"
  ),
  
  tar_target(
    individ,
    read_individ(raw_individ_file)
  ),
  
  tar_target(
    boende_events,
    read_boende_events(raw_boende_file)
  ),
  
  tar_target(
    kodort,
    read_kodort(raw_kodort_file)
  ),
  
  tar_target(
    yrke,
    read_yrke(raw_yrke_file)
  ),
  
  tar_target(
    lyte,
    read_lyte(raw_lyte_file)
  ),
  
  tar_target(
    boende_raw,
    build_boende(
      individ = individ,
      boende_events = boende_events,
      kodort = kodort,
      yrke = yrke,
      lyte = lyte
    )
  ),
  
  tar_target(
    boende_raw_rds,
    save_rds_target(boende_raw, "data/derived/boende_raw.rds"),
    format = "file"
  ),
  
  tar_target(
    selected_parishes,
    forsamlingar_ske
  ),
  
  tar_target(
    boende_ske,
    filter_to_region_and_parishes(
      spells = boende_raw,
      region_code = "NOS",
      parish_names = selected_parishes
    )
  ),
  
  tar_target(
    boende_ske_rds,
    save_rds_target(boende_ske, "data/derived/boende_ske.rds"),
    format = "file"
  ),
  
  tar_target(
    boende_ske_regularized,
    regularize_spells(boende_ske, gap_years = 2)
  ),
  
  tar_target(
    boende_ske_regularized_rds,
    save_rds_target(boende_ske_regularized, "data/derived/boende_ske_regularized.rds"),
    format = "file"
  ),
  
  tar_target(
    indiv_data,
    collapse_to_individual(boende_ske_regularized, gap_years = 2)
  ),
  
  tar_target(
    case_tbl,
    cases
  ),
  
  tar_target(
    crisis_window,
    get_crisis_window(case_tbl$case),
    pattern = map(case_tbl)
  ),
  
  tar_target(
    spell_exposure,
    exposure_from_spells(boende_ske_regularized, crisis_window, case_tbl$case),
    pattern = map(crisis_window, case_tbl)
  ),
  
  tar_target(
    indiv_exposure,
    define_birth_exposure(indiv_data, crisis_window, case_tbl$case),
    pattern = map(crisis_window, case_tbl)
  ),
  
  tar_target(
    analysis_data,
    combine_individual_and_spell_exposure(
      indiv_exposure = indiv_exposure,
      spell_exposure = spell_exposure,
      case = case_tbl$case
    ),
    pattern = map(indiv_exposure, spell_exposure, case_tbl)
  ),
  
  tar_target(
    descriptive_table,
    make_descriptive_table(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    cohort_plot,
    make_cohort_plot(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    spatial_exposure_plot,
    make_spatial_exposure_plot(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    main_model_birth,
    fit_main_model_birth(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    main_model_spatial,
    fit_main_model_spatial(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    model_table_birth,
    tidy_model(main_model_birth, case_tbl$case, "birth_exposure"),
    pattern = map(main_model_birth, case_tbl)
  ),
  
  tar_target(
    model_table_spatial,
    tidy_model(main_model_spatial, case_tbl$case, "spatial_exposure"),
    pattern = map(main_model_spatial, case_tbl)
  ),
  
  tar_target(
    km_data,
    make_km_data(
      dat = analysis_data,
      case = case_tbl$case,
      years_before = 3,
      years_after = 3
    ),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    km_fit,
    fit_km_curve(km_data),
    pattern = map(km_data)
  ),
  
  tar_target(
    km_tidy,
    tidy_km_fit(km_fit, case_tbl$case),
    pattern = map(km_fit, case_tbl)
  ),
  
  tar_target(
    km_plot_file,
    save_km_plot(
      km_tidy = km_tidy,
      case = case_tbl$case,
      path = file.path("output", case_tbl$case, "figures", "km_plot.png")
    ),
    pattern = map(km_tidy, case_tbl),
    format = "file"
  )
)