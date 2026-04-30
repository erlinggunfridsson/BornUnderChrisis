# Add these functions at the end of your existing R/functions_targets.R
# This is a patch file, not a complete replacement.

censor_spells <- function(DT, censor_date) {
  DT <- data.table::copy(data.table::as.data.table(DT))

  if (!"bos_date" %in% names(DT)) {
    stop("DT must contain bos_date", call. = FALSE)
  }

  if (!inherits(censor_date, "Date")) {
    censor_date <- as.Date(censor_date)
  }

  if ("bob_date" %in% names(DT)) {
    DT <- DT[is.na(bob_date) | bob_date <= censor_date]
  }

  DT[!is.na(bos_date) & bos_date > censor_date, bos_date := censor_date]

  DT
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

clean_lyte_terms <- function(dt) {
  dt <- data.table::copy(data.table::as.data.table(dt))

  if (!"term" %in% names(dt)) {
    return(dt)
  }

  dt[term == "(Intercept)", term := "Intercept"]
  dt[term == "kon1", term := "Sex: category 1"]
  dt[term == "kon2", term := "Sex: category 2"]
  dt[term == "oaktaTRUE", term := "Illegitimate birth"]
  dt[grepl("^factor\\(fod_ar\\)", term),
     term := sub("^factor\\(fod_ar\\)", "Birth year: ", term)]

  dt
}

clean_global_terms <- function(dt) {
  dt <- data.table::copy(data.table::as.data.table(dt))

  if (!"term" %in% names(dt)) {
    return(dt)
  }

  dt[term == "kon", term := "Sex"]
  dt[term == "oakta", term := "Illegitimate birth"]
  dt[term == "factor(fod_ar)", term := "Birth year"]

  dt
}
