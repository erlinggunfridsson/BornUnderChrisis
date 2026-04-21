library(targets)
library(tarchetypes)

popum_dir <- "/home/erling/gitstuff/POPUM_DDB"
admin_censor_date <- as.Date("1890-01-01")

tar_option_set(
  packages = c("data.table", "ggplot2", "broom", "survival")
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
  tar_target(raw_individ_file, file.path(popum_dir, "POPUM_person.csv"), format = "file"),
  tar_target(raw_boende_file, file.path(popum_dir, "POPUM_BOFRSORT.csv"), format = "file"),
  tar_target(raw_kodort_file, file.path(popum_dir, "POPUM_KODORTKOD.csv"), format = "file"),
  tar_target(raw_yrke_file, file.path(popum_dir, "POPUM_YRKE.csv"), format = "file"),
  tar_target(raw_lyte_file, file.path(popum_dir, "POPUM_lyte.csv"), format = "file"),
  
  tar_target(censor_date, admin_censor_date),
  tar_target(selected_parishes, forsamlingar_ske),
  tar_target(case_tbl, cases),
  
  tar_target(individ, read_individ(raw_individ_file)),
  tar_target(boende_events, read_boende_events(raw_boende_file)),
  tar_target(kodort, read_kodort(raw_kodort_file)),
  tar_target(yrke, read_yrke(raw_yrke_file)),
  tar_target(lyte, read_lyte(raw_lyte_file)),
  
  tar_target(
    boende_raw,
    build_boende(individ, boende_events, kodort, yrke, lyte)
  ),
  
  tar_target(
    boende_ske,
    filter_to_region_and_parishes(
      boende_raw,
      region_code = "NOS",
      parish_names = selected_parishes
    )
  ),
  
  tar_target(
    boende_ske_censored,
    censor_spells(boende_ske, censor_date)
  ),
  
  tar_target(
    boende_ske_regularized,
    regularize_spells(boende_ske_censored, gap_years = 2)
  ),
  
  tar_target(
    indiv_data,
    collapse_to_individual(
      boende_ske_regularized,
      gap_years = 2
    )
  ),
  tar_target(
    sex_ratio_ts,
    make_sex_ratio_time_series(
      indiv_data,
      start_year = 1790,
      end_year = 1890,
      boys_code = 1
    )
  ),
  
  tar_target(
    sex_ratio_plot_file,
    save_sex_ratio_plot(
      sex_ratio_ts,
      file.path("output", "descriptive", "figures", "sex_ratio_1790_1890.png")
      # file.path("output", "descriptive", "figures", "sex_ratio_TEST.png")
    ),
    format = "file"
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
      indiv_exposure,
      spell_exposure,
      case_tbl$case
    ),
    pattern = map(indiv_exposure, spell_exposure, case_tbl)
  ),
  
  tar_target(
    descriptive_table,
    make_descriptive_table(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    km_data,
    make_km_data(
      analysis_data,
      case_tbl$case,
      censor_date,
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
      km_tidy,
      case_tbl$case,
      file.path("output", case_tbl$case, "figures", "km_sex.png")
    ),
    pattern = map(km_tidy, case_tbl),
    format = "file"
  ),
  
  tar_target(
    km_data_legitimacy,
    make_km_data_legitimacy(
      analysis_data,
      case_tbl$case,
      censor_date,
      years_before = 3,
      years_after = 3
    ),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    km_fit_legitimacy,
    fit_km_curve_legitimacy(km_data_legitimacy),
    pattern = map(km_data_legitimacy)
  ),
  
  tar_target(
    km_tidy_legitimacy,
    tidy_km_fit_legitimacy(km_fit_legitimacy, case_tbl$case),
    pattern = map(km_fit_legitimacy, case_tbl)
  ),
  
  tar_target(
    km_plot_file_legitimacy,
    save_km_plot_legitimacy(
      km_tidy_legitimacy,
      case_tbl$case,
      file.path("output", case_tbl$case, "figures", "km_legitimacy.png")
    ),
    pattern = map(km_tidy_legitimacy, case_tbl),
    format = "file"
  ),
  
  tar_target(
    illegitimacy_ts,
    make_illegitimacy_time_series(analysis_data, case_tbl$case),
    pattern = map(analysis_data, case_tbl)
  ),
  
  tar_target(
    illegitimacy_plot_file,
    save_illegitimacy_plot(
      illegitimacy_ts,
      case_tbl$case,
      file.path("output", case_tbl$case, "figures", "illegitimacy_share.png")
    ),
    pattern = map(illegitimacy_ts, case_tbl),
    format = "file"
  ),
  
  tar_target(
    lyte_model_data,
    make_lyte_model_data(
      indiv_data,
      lyte,
      case_tbl$case,
      censor_date,
      years_before = 3,
      years_after = 3
    ),
    pattern = map(case_tbl)
  ),
  
  tar_target(
    lyte_model,
    fit_lyte_model(lyte_model_data),
    pattern = map(lyte_model_data)
  ),
  
  tar_target(
    lyte_model_table,
    data.table::copy(tidy_lyte_model(lyte_model))[, case := case_tbl$case],
    pattern = map(lyte_model, case_tbl)
  ),
  
  tar_target(
    lyte_global_tests,
    data.table::copy(tidy_global_tests(lyte_model))[, case := case_tbl$case],
    pattern = map(lyte_model, case_tbl)
  ),
  
  tar_target(
    lyte_prediction_data,
    make_lyte_prediction_data(lyte_model_data),
    pattern = map(lyte_model_data)
  ),
  
  tar_target(
    lyte_prediction_results,
    predict_lyte_model(lyte_model, lyte_prediction_data),
    pattern = map(lyte_model, lyte_prediction_data)
  ),
  
  tar_target(
    lyte_prediction_plot_file,
    save_lyte_prediction_plot(
      lyte_prediction_results,
      case_tbl$case,
      file.path("output", case_tbl$case, "figures", "lyte_prediction_plot.png")
    ),
    pattern = map(lyte_prediction_results, case_tbl),
    format = "file"
  ),
  
  tarchetypes::tar_render(
    paper_war,
    path = "reports/paper_war.qmd"
  )
  

)
