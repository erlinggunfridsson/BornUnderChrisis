library(targets)
library(tarchetypes)

tar_option_set(
  packages = c(
    "data.table",
    "ggplot2",
    "broom",
    "tibble"
  )
)

source("R/functions_targets.R")



cases <- data.table::data.table(
  case = c("war", "famine")
)

list(
  tar_target(
    parishes_ske,
    c(
      "SKELLEFTEÅ", "BURTRÄSK", "BYGDEÅ", "BYSKE",
      "LÖVÅNGER", "NYSÄTRA", "NORSJÖ", "JÖRN",
      "ROBERTSFORS", "YTTERSTFORS", "SKELLEFTEÅ SANKT OLOF",
      "FÄLLFORS"
    )
  ),
  
  tar_target(
    boende_file,
    "data/raw/boende.rds",
    format = "file"
  ),
  
  tar_target(
    base_spells_full,
    build_base_spells(boende_file)
  ),
  
  tar_target(
    base_spells,
    filter_to_region(base_spells_full, parishes_ske)
  ),
  
  tar_target(
    indiv_data,
    collapse_to_individual(base_spells)
  ),
  
  tar_target(
    check_parishes,
    unique(base_spells[, .(bofrsnmn)])[order(bofrsnmn)]
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
    exposure_from_spells(base_spells, crisis_window, case_tbl$case),
    pattern = map(crisis_window, case_tbl)
  ),
  
  tar_target(
    indiv_exposure,
    define_birth_exposure(indiv_data, crisis_window, case_tbl$case),
    pattern = map(crisis_window, case_tbl)
  ),
  
  tar_target(
    analysis_data,
    combine_individual_and_spell_exposure(indiv_exposure, spell_exposure, case_tbl$case),
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
    tidy_model(main_model_birth, case_tbl$case, model_name = "birth_exposure"),
    pattern = map(main_model_birth, case_tbl)
  ),
  
  tar_target(
    model_table_spatial,
    tidy_model(main_model_spatial, case_tbl$case, model_name = "spatial_exposure"),
    pattern = map(main_model_spatial, case_tbl)
  )
)