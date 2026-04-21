parse_popum_date <- function(x) {
  x <- as.character(x)
  x[x %in% c("", "0", "00000000", NA)] <- NA_character_
  x <- ifelse(!is.na(x), sprintf("%08s", x), x)
  x <- gsub(" ", "0", x, fixed = TRUE)
  
  year  <- substr(x, 1, 4)
  month <- substr(x, 5, 6)
  day   <- substr(x, 7, 8)
  
  month[month == "00"] <- "01"
  day[day == "00"] <- "01"
  
  out <- paste0(year, "-", month, "-", day)
  out[is.na(x)] <- NA_character_
  
  as.Date(out)
}

save_rds_target <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(object, path)
  path
}

read_individ <- function(path) {
  DT <- data.table::fread(path, encoding = "UTF-8", na.strings = c("", "NA"))
  DT <- data.table::copy(DT)
  data.table::setnames(DT, names(DT), tolower(names(DT)))
  
  DT[, ddbid := as.character(ddbid)]
  DT[, fod_date := parse_popum_date(foddat)]
  DT[, dod_date := parse_popum_date(doddat)]
  DT[, dop_date := parse_popum_date(dopdat)]
  DT[, beg_date := parse_popum_date(begdat)]
  
  DT[]
}

read_boende_events <- function(path) {
  DT <- data.table::fread(path, encoding = "UTF-8", na.strings = c("", "NA"))
  DT <- data.table::copy(DT)
  data.table::setnames(DT, names(DT), tolower(names(DT)))
  
  DT[, ddbid := as.character(ddbid)]
  DT[, bob_date := parse_popum_date(bobdat)]
  DT[, bos_date := parse_popum_date(bosdat)]
  
  data.table::setorder(DT, ddbid, bonr, bob_date, bos_date)
  DT[]
}

read_kodort <- function(path) {
  DT <- data.table::fread(path, encoding = "UTF-8", na.strings = c("", "NA"))
  DT <- data.table::copy(DT)
  data.table::setnames(DT, names(DT), tolower(names(DT)))
  DT[]
}

read_yrke <- function(path) {
  DT <- data.table::fread(path, encoding = "UTF-8", na.strings = c("", "NA"))
  DT <- data.table::copy(DT)
  data.table::setnames(DT, names(DT), tolower(names(DT)))
  DT[, ddbid := as.character(ddbid)]
  
  if ("yrkebdat" %in% names(DT)) {
    DT[, yrke_bdate := parse_popum_date(yrkebdat)]
  }
  if ("yrkesdat" %in% names(DT)) {
    DT[, yrke_sdate := parse_popum_date(yrkesdat)]
  }
  
  DT[]
}

read_lyte <- function(path) {
  DT <- data.table::fread(path, encoding = "UTF-8", na.strings = c("", "NA"))
  DT <- data.table::copy(DT)
  data.table::setnames(DT, names(DT), tolower(names(DT)))
  DT[, ddbid := as.character(ddbid)]
  
  if ("lytebdat" %in% names(DT)) {
    DT[, lyte_bdate := parse_popum_date(lytebdat)]
  }
  if ("lytesdat" %in% names(DT)) {
    DT[, lyte_sdate := parse_popum_date(lytesdat)]
  }
  
  DT[]
}

build_boende <- function(individ, boende_events, kodort = NULL, yrke = NULL, lyte = NULL) {
  indiv <- data.table::copy(individ)
  bo <- data.table::copy(boende_events)
  
  indiv_keep <- indiv[
    ,
    .(
      ddbid,
      regprefix,
      kon,
      foddat,
      doddat,
      fod_date,
      dod_date,
      ab,
      dodfodd,
      fb
    )
  ]
  
  bo <- indiv_keep[bo, on = "ddbid"]
  
  if (!is.null(kodort)) {
    ort <- data.table::copy(kodort)
    data.table::setnames(ort, names(ort), tolower(names(ort)))
    
    if ("ortkod" %in% names(ort) && "boortkod" %in% names(bo)) {
      ort_small <- unique(
        ort[, .(ortkod, ortnmn, x_ort, y_ort, koordklassind)]
      )
      bo <- ort_small[bo, on = c("ortkod" = "boortkod")]
    }
  }
  
  if (!is.null(yrke)) {
    yr <- data.table::copy(yrke)
    data.table::setnames(yr, names(yr), tolower(names(yr)))
    yr[, ddbid := as.character(ddbid)]
    
    if ("yrkebdat" %in% names(yr) && !"yrke_bdate" %in% names(yr)) {
      yr[, yrke_bdate := parse_popum_date(yrkebdat)]
    }
    
    cols <- c("ddbid", "hiscostatus", "yrkestdnmn")
    if ("yrke_bdate" %in% names(yr)) cols <- c(cols, "yrke_bdate")
    
    yr_small <- yr[, cols, with = FALSE]
    
    if ("yrke_bdate" %in% names(yr_small)) {
      data.table::setorderv(yr_small, c("ddbid", "yrke_bdate"))
      yr_small <- yr_small[, .SD[1], by = ddbid]
    } else {
      yr_small <- unique(yr_small, by = "ddbid")
    }
    
    bo <- yr_small[bo, on = "ddbid"]
  }
  
  if (!is.null(lyte)) {
    ly <- data.table::copy(lyte)
    data.table::setnames(ly, names(ly), tolower(names(ly)))
    ly[, ddbid := as.character(ddbid)]
    
    if ("lytebdat" %in% names(ly) && !"lyte_bdate" %in% names(ly)) {
      ly[, lyte_bdate := parse_popum_date(lytebdat)]
    }
    
    cols <- c("ddbid", "lytestdnmn")
    if ("lyte_bdate" %in% names(ly)) cols <- c(cols, "lyte_bdate")
    
    ly_small <- ly[, cols, with = FALSE]
    
    if ("lyte_bdate" %in% names(ly_small)) {
      data.table::setorderv(ly_small, c("ddbid", "lyte_bdate"))
      ly_small <- ly_small[, .SD[1], by = ddbid]
    } else {
      ly_small <- unique(ly_small, by = "ddbid")
    }
    
    bo <- ly_small[bo, on = "ddbid"]
  }
  
  data.table::setorder(bo, ddbid, bonr, bob_date, bos_date)
  bo[]
}

filter_to_region_and_parishes <- function(spells, region_code, parish_names) {
  DT <- data.table::copy(spells)
  
  DT <- DT[
    boregprefix == region_code &
      bofrsnmn %in% parish_names
  ]
  
  data.table::setorder(DT, ddbid, bonr, bob_date, bos_date)
  DT[]
}

censor_spells <- function(DT, censor_date = as.Date("1890-01-01")) {
  DT <- data.table::copy(DT)
  
  DT <- DT[is.na(bob_date) | bob_date < censor_date]
  
  DT[
    !is.na(bos_date) & bos_date >= censor_date,
    bos_date := censor_date
  ]
  
  DT[]
}

regularize_spells <- function(DT, gap_years = 2) {
  DT <- data.table::copy(DT)
  gap_days <- round(365.25 * gap_years)
  
  data.table::setorder(DT, ddbid, bob_date, bos_date, bonr)
  
  out <- DT[
    ,
    {
      x <- data.table::copy(.SD)
      n <- nrow(x)
      
      if (n == 0L) return(x)
      
      if (n >= 2L) {
        for (i in 1:(n - 1)) {
          end_i <- x$bos_date[i]
          start_next <- x$bob_date[i + 1]
          
          if (!is.na(end_i) && !is.na(start_next) && end_i >= start_next) {
            mid_num <- floor((as.numeric(end_i) + as.numeric(start_next)) / 2)
            mid_date <- as.Date(mid_num, origin = "1970-01-01")
            x$bos_date[i] <- mid_date
            x$bob_date[i + 1] <- mid_date + 1
          }
        }
      }
      
      x[, gap_after_prev_days := as.numeric(bob_date - data.table::shift(bos_date) - 1)]
      x[1, gap_after_prev_days := NA_real_]
      
      x[, large_gap_after_prev := !is.na(gap_after_prev_days) & gap_after_prev_days > gap_days]
      
      first_gap_row <- which(x$large_gap_after_prev)[1]
      had_gap <- !is.na(first_gap_row)
      
      if (had_gap) {
        time_to_gap_days <- as.numeric(x$bob_date[first_gap_row] - x$bob_date[1])
        time_to_gap_years <- time_to_gap_days / 365.25
      } else {
        time_to_gap_days <- NA_real_
        time_to_gap_years <- NA_real_
      }
      
      x[, had_large_gap := had_gap]
      x[, time_to_first_large_gap_days := time_to_gap_days]
      x[, time_to_first_large_gap_years := time_to_gap_years]
      
      x[]
    },
    by = ddbid
  ]
  
  out[]
}

collapse_to_individual <- function(DT, gap_years = 2, censor_date = as.Date("1890-01-01")) {
  DT <- data.table::copy(DT)
  gap_days <- round(365.25 * gap_years)
  
  has_boortkod <- "boortkod" %in% names(DT)
  has_ortkod <- "ortkod" %in% names(DT)
  has_boortnmn <- "boortnmn" %in% names(DT)
  has_ortnmn <- "ortnmn" %in% names(DT)
  
  out <- DT[
    ,
    {
      x <- data.table::copy(.SD)
      
      gap_idx <- which(!is.na(x$gap_after_prev_days) & x$gap_after_prev_days > gap_days)
      
      if (length(gap_idx) > 0L) {
        first_gap_row <- gap_idx[1L]
        x_epi <- x[1:(first_gap_row - 1)]
        had_gap <- TRUE
        time_to_gap_days <- as.numeric(x$bob_date[first_gap_row] - x$bob_date[1])
        time_to_gap_years <- time_to_gap_days / 365.25
      } else {
        x_epi <- x
        had_gap <- FALSE
        time_to_gap_days <- NA_real_
        time_to_gap_years <- NA_real_
      }
      
      first_row <- 1L
      last_row <- nrow(x_epi)
      
      first_type <- x_epi$bobtyp[first_row]
      last_type <- x_epi$bostyp[last_row]
      last_bos <- x_epi$bos_date[last_row]
      
      ab_vals <- x_epi$ab[!is.na(x_epi$ab)]
      dodfodd_vals <- x_epi$dodfodd[!is.na(x_epi$dodfodd)]
      fb_vals <- x_epi$fb[!is.na(x_epi$fb)]
      
      first_ortkod <- if (has_boortkod) x_epi$boortkod[first_row] else if (has_ortkod) x_epi$ortkod[first_row] else NA_integer_
      first_ortnmn <- if (has_boortnmn) x_epi$boortnmn[first_row] else if (has_ortnmn) x_epi$ortnmn[first_row] else NA_character_
      
      list(
        kon = data.table::first(x_epi$kon),
        fod_date = data.table::first(x_epi$fod_date),
        dod_date = data.table::first(x_epi$dod_date),
        fod_ar = as.integer(format(data.table::first(x_epi$fod_date), "%Y")),
        dod_ar = as.integer(format(data.table::first(x_epi$dod_date), "%Y")),
        
        n_spells = nrow(x_epi),
        n_spells_total = nrow(x),
        
        first_bofrs = x_epi$bofrs[first_row],
        first_bofrsnmn = x_epi$bofrsnmn[first_row],
        first_boortkod = first_ortkod,
        first_boortnmn = first_ortnmn,
        first_boregprefix = x_epi$boregprefix[first_row],
        
        first_bob_date = x_epi$bob_date[first_row],
        first_bos_date = x_epi$bos_date[first_row],
        entry_type = first_type,
        
        last_bob_date = x_epi$bob_date[last_row],
        last_bos_date = last_bos,
        exit_type = last_type,
        
        entered_as_birth = first_type == 2L,
        exited_by_death = (last_type == 2L) & !is.na(last_bos) & (last_bos < censor_date),
        censored_admin = !is.na(last_bos) & (last_bos >= censor_date),
        
        ab = if (length(ab_vals) == 0L) NA_integer_ else ab_vals[1L],
        legitim = if (length(ab_vals) == 0L) NA else ab_vals[1L] == 1L,
        oakta = if (length(ab_vals) == 0L) NA else ab_vals[1L] == 2L,
        trolovningsbarn = if (length(ab_vals) == 0L) NA else ab_vals[1L] == 3L,
        
        dodfodd = if (length(dodfodd_vals) == 0L) NA_integer_ else dodfodd_vals[1L],
        is_dodfodd = if (length(dodfodd_vals) == 0L) NA else dodfodd_vals[1L] == 1L,
        
        fb = if (length(fb_vals) == 0L) NA_integer_ else fb_vals[1L],
        
        had_large_gap = had_gap,
        time_to_first_large_gap_days = time_to_gap_days,
        time_to_first_large_gap_years = time_to_gap_years
      )
    },
    by = ddbid
  ]
  
  out[]
}

get_crisis_window <- function(case) {
  if (case == "war") {
    data.table::data.table(
      crisis_case = "war",
      crisis_start = as.Date("1808-01-01"),
      crisis_end = as.Date("1809-12-31"),
      birth_start = 1808L,
      birth_end = 1809L
    )
  } else if (case == "famine") {
    data.table::data.table(
      crisis_case = "famine",
      crisis_start = as.Date("1866-01-01"),
      crisis_end = as.Date("1868-12-31"),
      birth_start = 1866L,
      birth_end = 1868L
    )
  } else {
    stop("Unknown case: ", case, call. = FALSE)
  }
}

define_birth_exposure <- function(indiv_data, crisis_window, case) {
  DT <- data.table::copy(indiv_data)
  birth_start <- crisis_window$birth_start[[1]]
  birth_end <- crisis_window$birth_end[[1]]
  
  DT[, crisis_case := case]
  DT[, exposed_birth := data.table::between(fod_ar, birth_start, birth_end)]
  DT[]
}

exposure_from_spells <- function(spells, crisis_window, case) {
  DT <- data.table::copy(spells)
  crisis_start <- crisis_window$crisis_start[[1]]
  crisis_end <- crisis_window$crisis_end[[1]]
  
  DT[, crisis_case := case]
  DT[, overlap_crisis := !is.na(bob_date) & !is.na(bos_date) & !(bos_date < crisis_start | bob_date > crisis_end)]
  
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
      }
    ),
    by = ddbid
  ]
  
  out[]
}

combine_individual_and_spell_exposure <- function(indiv_exposure, spell_exposure, case) {
  DT_indiv <- data.table::copy(indiv_exposure)
  DT_spell <- data.table::copy(spell_exposure)
  
  out <- DT_spell[DT_indiv, on = "ddbid"]
  out[, crisis_case := case]
  out[is.na(exposed_spatial), exposed_spatial := FALSE]
  out[is.na(n_overlapping_spells), n_overlapping_spells := 0L]
  
  out[]
}

make_descriptive_table <- function(dat, case) {
  DT <- data.table::copy(dat)
  
  data.table::data.table(
    case = case,
    n_individuals = nrow(DT),
    n_birth_exposed = DT[, sum(exposed_birth, na.rm = TRUE)],
    n_spatial_exposed = DT[, sum(exposed_spatial, na.rm = TRUE)],
    mean_spells = DT[, mean(n_spells, na.rm = TRUE)],
    median_spells = DT[, stats::median(n_spells, na.rm = TRUE)],
    share_entered_as_birth = DT[, mean(entered_as_birth, na.rm = TRUE)],
    share_exited_by_death = DT[, mean(exited_by_death, na.rm = TRUE)],
    share_oakta = DT[, mean(oakta, na.rm = TRUE)],
    share_had_large_gap = DT[, mean(had_large_gap, na.rm = TRUE)],
    share_censored_admin = DT[, mean(censored_admin, na.rm = TRUE)]
  )
}

make_cohort_plot <- function(dat, case) {
  DT <- data.table::copy(dat)
  plot_data <- DT[, .(n = .N), by = .(fod_ar, exposed_birth)]
  
  ggplot2::ggplot(
    plot_data,
    ggplot2::aes(x = fod_ar, y = n, linetype = exposed_birth)
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
  plot_data <- DT[, .(n = .N), by = .(fod_ar, exposed_spatial)]
  
  ggplot2::ggplot(
    plot_data,
    ggplot2::aes(x = fod_ar, y = n, linetype = exposed_spatial)
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
  model_data <- DT[!is.na(exposed_birth) & !is.na(kon) & !is.na(n_spells)]
  model_data[, kon := as.factor(kon)]
  
  stats::glm(
    exposed_birth ~ kon + n_spells + oakta + had_large_gap,
    data = model_data,
    family = stats::binomial()
  )
}

fit_main_model_spatial <- function(dat, case) {
  DT <- data.table::copy(dat)
  model_data <- DT[!is.na(exposed_spatial) & !is.na(kon) & !is.na(n_spells)]
  model_data[, kon := as.factor(kon)]
  
  stats::glm(
    exposed_spatial ~ kon + n_spells + oakta + had_large_gap,
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

make_km_data <- function(dat, case, censor_date = as.Date("1890-01-01"), years_before = 3, years_after = 3) {
  DT <- data.table::copy(dat)
  
  if (case == "war") {
    crisis_start_year <- 1808L
    crisis_end_year <- 1809L
  } else if (case == "famine") {
    crisis_start_year <- 1866L
    crisis_end_year <- 1868L
  } else {
    stop("Unknown case: ", case, call. = FALSE)
  }
  
  DT <- DT[
    !is.na(fod_ar) &
      fod_ar >= (crisis_start_year - years_before) &
      fod_ar <= (crisis_end_year + years_after)
  ]
  
  DT <- DT[
    !is.na(fod_date) &
      !is.na(last_bos_date) &
      !is.na(kon)
  ]
  
  DT[, event := as.integer(
    exited_by_death == TRUE &
      last_bos_date < censor_date
  )]
  
  DT[, time_days := as.numeric(last_bos_date - fod_date)]
  DT <- DT[!is.na(time_days) & time_days >= 0]
  DT[, time_years := time_days / 365.25]
  
  DT[, cohort := as.factor(fod_ar)]
  DT[, sex := fifelse(
    as.integer(as.character(kon)) == 1L, "Boys",
    fifelse(as.integer(as.character(kon)) == 2L, "Girls", NA_character_)
  )]
  DT <- DT[!is.na(sex)]
  
  DT[]
}

fit_km_curve <- function(km_data) {
  DT <- data.table::copy(km_data)
  
  survival::survfit(
    survival::Surv(time_years, event) ~ cohort + sex,
    data = DT
  )
}

tidy_km_fit <- function(km_fit, case) {
  s <- summary(km_fit)
  
  out <- data.table::data.table(
    time = s$time,
    surv = s$surv,
    n_risk = s$n.risk,
    n_event = s$n.event,
    n_censor = s$n.censor,
    strata = as.character(s$strata)
  )
  
  out[, cohort := sub("^cohort=([0-9]+), sex=.*$", "\\1", strata)]
  out[, sex := sub("^cohort=[0-9]+, sex=(.*)$", "\\1", strata)]
  out[, cohort := as.integer(cohort)]
  out[, case := case]
  out[]
}

save_km_plot <- function(km_tidy, case, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  ymin <- floor(min(km_tidy$surv, na.rm = TRUE) * 10) / 10
  ymax <- 1
  
  p <- ggplot2::ggplot(
    km_tidy,
    ggplot2::aes(
      x = time,
      y = surv,
      group = cohort,
      color = factor(cohort)
    )
  ) +
    ggplot2::geom_step(linewidth = 0.7) +
    ggplot2::facet_wrap(~ sex) +
    ggplot2::labs(
      title = paste("Kaplan–Meier by birth year and sex:", case),
      x = "Age (years)",
      y = "Survival probability",
      color = "Birth year"
    ) +
    ggplot2::scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = 0.05)
    ) +
    ggplot2::scale_x_continuous(
      breaks = seq(0, max(km_tidy$time, na.rm = TRUE), by = 10)
    )
  
  ggplot2::ggsave(
    filename = path,
    plot = p,
    width = 10,
    height = 6,
    dpi = 300
  )
  
  path
}

make_km_data_legitimacy <- function(dat, case, censor_date = as.Date("1890-01-01"), years_before = 3, years_after = 3) {
  DT <- data.table::copy(dat)
  
  if (case == "war") {
    crisis_start_year <- 1808L
    crisis_end_year <- 1809L
  } else if (case == "famine") {
    crisis_start_year <- 1866L
    crisis_end_year <- 1868L
  } else {
    stop("Unknown case: ", case, call. = FALSE)
  }
  
  DT <- DT[
    !is.na(fod_ar) &
      fod_ar >= (crisis_start_year - years_before) &
      fod_ar <= (crisis_end_year + years_after)
  ]
  
  DT <- DT[
    !is.na(fod_date) &
      !is.na(last_bos_date)
  ]
  
  DT[, event := as.integer(
    exited_by_death == TRUE &
      last_bos_date < censor_date
  )]
  
  DT[, time_days := as.numeric(last_bos_date - fod_date)]
  DT <- DT[!is.na(time_days) & time_days >= 0]
  DT[, time_years := time_days / 365.25]
  
  DT[, cohort := as.factor(fod_ar)]
  DT[, legitimacy := fifelse(
    legitim %in% TRUE, "Legitimate",
    fifelse(oakta %in% TRUE, "Illegitimate", NA_character_)
  )]
  DT <- DT[!is.na(legitimacy)]
  
  DT[]
}

fit_km_curve_legitimacy <- function(km_data) {
  DT <- data.table::copy(km_data)
  
  survival::survfit(
    survival::Surv(time_years, event) ~ cohort + legitimacy,
    data = DT
  )
}

tidy_km_fit_legitimacy <- function(km_fit, case) {
  s <- summary(km_fit)
  
  out <- data.table::data.table(
    time = s$time,
    surv = s$surv,
    n_risk = s$n.risk,
    n_event = s$n.event,
    n_censor = s$n.censor,
    strata = as.character(s$strata)
  )
  
  out[, cohort := sub("^cohort=([0-9]+), legitimacy=.*$", "\\1", strata)]
  out[, legitimacy := sub("^cohort=[0-9]+, legitimacy=(.*)$", "\\1", strata)]
  out[, cohort := as.integer(cohort)]
  out[, case := case]
  out[]
}

save_km_plot_legitimacy <- function(km_tidy, case, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  ymin <- floor(min(km_tidy$surv, na.rm = TRUE) * 10) / 10
  ymax <- 1
  
  p <- ggplot2::ggplot(
    km_tidy,
    ggplot2::aes(
      x = time,
      y = surv,
      group = cohort,
      color = factor(cohort)
    )
  ) +
    ggplot2::geom_step(linewidth = 0.7) +
    ggplot2::facet_wrap(~ legitimacy) +
    ggplot2::labs(
      title = paste("Kaplan–Meier by birth year and legitimacy:", case),
      x = "Age (years)",
      y = "Survival probability",
      color = "Birth year"
    ) +
    ggplot2::scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = 0.05)
    ) +
    ggplot2::scale_x_continuous(
      breaks = seq(0, max(km_tidy$time, na.rm = TRUE), by = 10)
    )
  
  ggplot2::ggsave(
    filename = path,
    plot = p,
    width = 10,
    height = 6,
    dpi = 300
  )
  
  path
}

make_illegitimacy_time_series <- function(dat, case, years_before = 10, years_after = 10) {
  DT <- data.table::copy(dat)
  
  if (case == "war") {
    crisis_start_year <- 1808L
    crisis_end_year   <- 1809L
  } else if (case == "famine") {
    crisis_start_year <- 1866L
    crisis_end_year   <- 1868L
  } else {
    stop("Unknown case: ", case, call. = FALSE)
  }
  
  DT <- DT[
    !is.na(fod_ar) &
      fod_ar >= (crisis_start_year - years_before) &
      fod_ar <= (crisis_end_year + years_after) &
      !is.na(oakta)
  ]
  
  out <- DT[
    ,
    .(
      n_births = .N,
      n_illegitimate = sum(oakta %in% TRUE, na.rm = TRUE),
      share_illegitimate = mean(oakta %in% TRUE, na.rm = TRUE)
    ),
    by = .(fod_ar)
  ]
  
  out[, case := case]
  data.table::setorder(out, fod_ar)
  out[]
}

save_illegitimacy_plot <- function(ts_data, case, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  ymax <- max(ts_data$share_illegitimate, na.rm = TRUE)
  ymax <- ceiling(ymax * 100) / 100
  
  p <- ggplot2::ggplot(
    ts_data,
    ggplot2::aes(x = fod_ar, y = share_illegitimate)
  ) +
    ggplot2::geom_line(linewidth = 0.8) +
    ggplot2::geom_point(size = 1.5) +
    ggplot2::labs(
      title = paste("Share of illegitimate births:", case),
      x = "Birth year",
      y = "Share illegitimate"
    ) +
    ggplot2::scale_y_continuous(
      limits = c(0, ymax),
      breaks = seq(0, ymax, by = 0.01)
    )
  
  ggplot2::ggsave(path, p, width = 8, height = 5, dpi = 300)
  path
}

save_birth_counts_plot <- function(ts_data, case, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  p <- ggplot2::ggplot(
    ts_data,
    ggplot2::aes(x = fod_ar, y = n_births)
  ) +
    ggplot2::geom_line(linewidth = 0.8) +
    ggplot2::geom_point(size = 1.5) +
    ggplot2::labs(
      title = paste("Birth counts over time:", case),
      x = "Birth year",
      y = "Number of births"
    ) +
    ggplot2::scale_x_continuous(
      breaks = seq(min(ts_data$fod_ar, na.rm = TRUE), max(ts_data$fod_ar, na.rm = TRUE), by = 1)
    )
  
  ggplot2::ggsave(
    filename = path,
    plot = p,
    width = 8,
    height = 5,
    dpi = 300
  )
  
  path
}

make_lyte_model_data <- function(indiv_data, lyte, case, censor_date = as.Date("1890-01-01"),
                                 years_before = 3, years_after = 3) {
  
  DT <- data.table::copy(indiv_data)
  ly <- data.table::copy(lyte)
  
  ly[, ddbid := as.character(ddbid)]
  
  if (!"lyte_bdate" %in% names(ly)) {
    if ("lytebdat" %in% names(ly)) {
      ly[, lyte_bdate := parse_popum_date(lytebdat)]
    } else {
      stop("No lyte date column found.", call. = FALSE)
    }
  }
  
  ly_first <- ly[
    !is.na(lyte_bdate),
    .(first_lyte_date = min(lyte_bdate)),
    by = ddbid
  ]
  
  DT <- ly_first[DT, on = "ddbid"]
  
  if (case == "war") {
    crisis_start <- 1808L
    crisis_end   <- 1809L
  } else if (case == "famine") {
    crisis_start <- 1866L
    crisis_end   <- 1868L
  } else {
    stop("Unknown case")
  }
  
  DT <- DT[
    fod_ar >= (crisis_start - years_before) &
      fod_ar <= (crisis_end + years_after)
  ]
  
  DT[, age_lyte := as.numeric(first_lyte_date - fod_date) / 365.25]
  DT[, age_end  := as.numeric(last_bos_date - fod_date) / 365.25]
  
  DT[, lyte_before_20 := !is.na(age_lyte) & age_lyte <= 20]
  
  DT <- DT[
    lyte_before_20 == TRUE |
      (!is.na(age_end) & age_end >= 20)
  ]
  
  DT[, kon := as.factor(kon)]
  DT[, fod_ar := as.integer(fod_ar)]
  
  DT[, legitimacy := fifelse(
    legitim %in% TRUE, "Legitimate",
    fifelse(oakta %in% TRUE, "Illegitimate", NA_character_)
  )]
  DT[, legitimacy := factor(legitimacy)]
  
  DT[]
}

fit_lyte_model <- function(dat) {
  DT <- data.table::copy(dat)
  
  model_data <- DT[
    !is.na(lyte_before_20) &
      !is.na(kon) &
      !is.na(fod_ar) &
      !is.na(oakta)
  ]
  
  stats::glm(
    lyte_before_20 ~ kon + factor(fod_ar) + oakta,
    data = model_data,
    family = stats::binomial()
  )
}

tidy_lyte_model <- function(model) {
  out <- broom::tidy(model, conf.int = TRUE)
  
  if ("estimate" %in% names(out)) {
    out$odds_ratio <- exp(out$estimate)
  }
  if ("conf.low" %in% names(out)) {
    out$conf.low.or <- exp(out$conf.low)
  }
  if ("conf.high" %in% names(out)) {
    out$conf.high.or <- exp(out$conf.high)
  }
  
  data.table::as.data.table(out)
}

tidy_global_tests <- function(model) {
  out <- as.data.frame(drop1(model, test = "Chisq"))
  out$term <- rownames(out)
  rownames(out) <- NULL
  
  pcol <- grep("^Pr\\(", names(out), value = TRUE)
  
  data.table::as.data.table(out)[
    term != "<none>",
    .(
      term = term,
      df = if ("Df" %in% names(out)) Df else NA_real_,
      statistic = if ("LRT" %in% names(out)) LRT else NA_real_,
      p.value = if (length(pcol) == 1) get(pcol) else NA_real_
    )
  ]
}

format_p_value <- function(p) {
  ifelse(
    is.na(p),
    NA_character_,
    ifelse(p < 0.001, "<0.001", sprintf("%.3f", p))
  )
}

format_lyte_table <- function(dt, digits = 3) {
  dt <- data.table::copy(data.table::as.data.table(dt))
  
  num_cols <- intersect(
    c(
      "estimate",
      "std.error",
      "statistic",
      "conf.low",
      "conf.high",
      "odds_ratio",
      "conf.low.or",
      "conf.high.or"
    ),
    names(dt)
  )
  
  if (length(num_cols) > 0) {
    dt[, (num_cols) := lapply(.SD, round, digits), .SDcols = num_cols]
  }
  
  if ("p.value" %in% names(dt)) {
    dt[, p.value := format_p_value(p.value)]
  }
  
  dt
}

format_global_tests <- function(dt, digits = 2) {
  dt <- data.table::copy(data.table::as.data.table(dt))
  
  num_cols <- intersect(c("df", "statistic"), names(dt))
  if (length(num_cols) > 0) {
    dt[, (num_cols) := lapply(.SD, round, digits), .SDcols = num_cols]
  }
  
  if ("p.value" %in% names(dt)) {
    dt[, p.value := format_p_value(p.value)]
  }
  
  dt
}

tidy_global_tests <- function(model) {
  out <- as.data.frame(drop1(model, test = "Chisq"))
  out$term <- rownames(out)
  rownames(out) <- NULL
  
  pcol <- grep("^Pr\\(", names(out), value = TRUE)
  statcol <- intersect(c("LRT", "AIC"), names(out))
  
  data.table::as.data.table(out)[
    term != "<none>",
    .(
      term = term,
      df = if ("Df" %in% names(out)) Df else NA_real_,
      statistic = if ("LRT" %in% names(out)) LRT else NA_real_,
      p.value = if (length(pcol) == 1) get(pcol) else NA_real_
    )
  ]
}

make_lyte_prediction_data <- function(dat) {
  DT <- data.table::copy(dat)
  
  years <- sort(unique(DT$fod_ar))
  sexes <- sort(unique(DT$kon))
  
  pred <- data.table::CJ(
    fod_ar = years,
    kon = sexes
  )
  
  # sätt oakta till referens (t.ex. FALSE)
  pred[, oakta := FALSE]
  
  pred[]
}

predict_lyte_model <- function(model, newdata) {
  ND <- data.table::copy(newdata)
  
  p <- stats::predict(
    model,
    newdata = ND,
    type = "response",
    se.fit = TRUE
  )
  
  ND[, fit := p$fit]
  ND[, se.fit := p$se.fit]
  
  lp <- stats::predict(model, newdata = ND, type = "link", se.fit = TRUE)
  ND[, fit_link := lp$fit]
  ND[, se_link := lp$se.fit]
  ND[, conf.low := stats::plogis(fit_link - 1.96 * se_link)]
  ND[, conf.high := stats::plogis(fit_link + 1.96 * se_link)]
  
  ND[]
}

save_lyte_prediction_plot <- function(pred_data, case, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  DT <- data.table::copy(pred_data)
  
  # Snygga könsetiketter
  DT[, kon_label := factor(
    kon,
    levels = c(1, 2),
    labels = c("Men", "Women")
  )]
  
  # Dynamisk y-axel
  ymax <- max(DT$conf.high, na.rm = TRUE)
  ymax <- ceiling(ymax * 1000) / 1000
  
  p <- ggplot2::ggplot(
    DT,
    ggplot2::aes(
      x = fod_ar,
      y = fit,
      color = kon_label,
      fill = kon_label
    )
  ) +
    ggplot2::geom_ribbon(
      ggplot2::aes(ymin = conf.low, ymax = conf.high),
      alpha = 0.15,
      color = NA
    ) +
    ggplot2::geom_line(linewidth = 0.9) +
    ggplot2::labs(
      title = paste("Predicted probability of lyte before age 20:", case),
      x = "Birth year",
      y = "Predicted probability",
      color = "Sex",
      fill = "Sex"
    ) +
    ggplot2::scale_y_continuous(
      limits = c(0, ymax),
      breaks = seq(0, ymax, by = 0.005)
    )
  
  ggplot2::ggsave(
    filename = path,
    plot = p,
    width = 9,
    height = 5.5,
    dpi = 300
  )
  
  path
}