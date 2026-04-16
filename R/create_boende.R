#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(anytime)
  library(sp)
  library(here)
  library(fs)
})

# =====================
# Configuration
# =====================
# Set your POPUM source directory here.
popum_dir <- "~/gitstuff/POPUM_DDB"
output_dir <- here::here("data", "raw")

input_files <- c(
  individ = file.path(popum_dir, "POPUM_person.csv"),
  boende  = file.path(popum_dir, "POPUM_BOFRSORT.csv"),
  kodort  = file.path(popum_dir, "POPUM_KODORTKOD.csv"),
  yrke    = file.path(popum_dir, "POPUM_YRKE.csv"),
  lyten   = file.path(popum_dir, "POPUM_lyte.csv")
)

output_files <- c(
  rdata = file.path(output_dir, "boende.Rdata"),
  rds   = file.path(output_dir, "boende.rds")
)

# =====================
# Helper functions
# =====================

check_input_files <- function(paths) {
  if (is.list(paths)) {
    paths <- unlist(paths, use.names = TRUE)
  }
  if (!is.character(paths)) {
    stop("`paths` must be a character vector or list of file paths.", call. = FALSE)
  }
  missing_idx <- !file.exists(paths)
  if (any(missing_idx)) {
    missing <- names(paths)[missing_idx]
    stop(
      "Missing input file(s): ",
      paste(sprintf("%s = %s", missing, paths[missing_idx]), collapse = "; "),
      call. = FALSE
    )
  }
  invisible(paths)
}

check_output_paths <- function(paths) {
  if (is.list(paths)) {
    paths <- unlist(paths, use.names = TRUE)
  }
  if (!is.character(paths)) {
    stop("`paths` must be a character vector or list of file paths.", call. = FALSE)
  }
  dirs <- unique(dirname(paths))
  for (d in dirs) {
    fs::dir_create(d)
  }
  invisible(paths)
}

normalize_yyyymmdd_chr <- function(x, default_month_day = c("0101", "1231"),
                                   missing_day = c("01", "30")) {
  default_month_day <- match.arg(default_month_day)
  missing_day <- match.arg(missing_day)

  x <- as.character(x)
  x[x %in% c("", "0", "00000000")] <- NA_character_

  has_year_only <- !is.na(x) & nchar(x) >= 8 & substr(x, 5, 8) == "0000"
  if (any(has_year_only)) {
    x[has_year_only] <- paste0(substr(x[has_year_only], 1, 4), default_month_day)
  }

  has_missing_day <- !is.na(x) & nchar(x) >= 8 & substr(x, 7, 8) == "00"
  if (any(has_missing_day)) {
    x[has_missing_day] <- paste0(substr(x[has_missing_day], 1, 6), missing_day)
  }

  x
}

as_anydate_safe <- function(x) {
  suppressWarnings(anydate(x))
}

normalize_interval_dates <- function(dt, start_col, end_col, start_date_col, end_date_col,
                                     end_day_for_missing = "30") {
  dt[, (start_col) := normalize_yyyymmdd_chr(get(start_col), default_month_day = "0101", missing_day = "01")]
  dt[, (end_col)   := normalize_yyyymmdd_chr(get(end_col),   default_month_day = "1231", missing_day = end_day_for_missing)]
  dt[, (start_date_col) := as_anydate_safe(get(start_col))]
  dt[, (end_date_col)   := as_anydate_safe(get(end_col))]
  invisible(dt)
}

SWEREF99toWGS84 <- function(coords) {
  coords <- as.data.frame(coords)
  coords$id <- seq_len(nrow(coords))
  coords_org <- coords
  coords <- na.omit(coords)

  if (nrow(coords) == 0) {
    coords_org$lon <- NA_real_
    coords_org$lat <- NA_real_
    return(coords_org)
  }

  tmp <- data.frame(coords.x = coords$X_ORT, coords.y = coords$Y_ORT)
  coordinates(tmp) <- ~ coords.x + coords.y
  proj4string(tmp) <- CRS("+init=epsg:3021")
  coords_wgs84 <- spTransform(tmp, CRS("+init=epsg:4326"))
  coords_wgs84 <- data.frame(
    lon = coords_wgs84@coords[, 1],
    lat = coords_wgs84@coords[, 2],
    id  = coords$id
  )

  merge(coords_org, coords_wgs84, by = "id", all.x = TRUE)
}

coerce_id_to_character <- function(dt, id_col = "DDBID") {
  if (!id_col %in% names(dt)) {
    if ("UTTAGSID" %in% names(dt)) {
      data.table::setnames(dt, "UTTAGSID", id_col)
    } else {
      stop(sprintf("No ID column found. Expected '%s' or 'UTTAGSID'.", id_col), call. = FALSE)
    }
  }
  dt[, (id_col) := as.character(get(id_col))]
  invisible(dt)
}

# =====================
# Main pipeline
# =====================

create_boende <- function(input_files, output_files) {
  input_files <- check_input_files(input_files)
  output_files <- check_output_paths(output_files)

  message("Reading source files...")
  individ <- fread(input_files[["individ"]])
  boende  <- fread(input_files[["boende"]])
  kodort  <- fread(input_files[["kodort"]])
  yrke    <- fread(input_files[["yrke"]])
  lyten   <- fread(input_files[["lyten"]])

  coerce_id_to_character(individ, "DDBID")
  coerce_id_to_character(boende,  "DDBID")
  coerce_id_to_character(yrke,    "DDBID")
  coerce_id_to_character(lyten,   "DDBID")

  if ("FODHEMFRSNMN" %in% names(individ)) {
    individ[, FODHEMFRSNMN := trimws(FODHEMFRSNMN)]
  }

  message("Transforming coordinates...")
  coords_in  <- as.data.frame(kodort[, .(X_ORT, Y_ORT)])
  coords_out <- SWEREF99toWGS84(coords_in)[, c("lat", "lon")]
  kodort     <- cbind(kodort, coords_out)
  if (all(c("X_ORT", "Y_ORT") %in% names(kodort))) {
    kodort[, c("X_ORT", "Y_ORT") := NULL]
  }

  if (!"BOORTKOD" %in% names(boende)) {
    stop("Column 'BOORTKOD' is missing in boende input.", call. = FALSE)
  }

  boende[, ORTKOD := BOORTKOD]
  boende <- kodort[, .(ORTKOD, ORTNMN, lat, lon)][boende, on = "ORTKOD"]
  drop_cols <- intersect(c("ORTKOD", "ORTNMN"), names(boende))
  if (length(drop_cols) > 0) {
    boende[, (drop_cols) := NULL]
  }

  message("Merging person variables...")
  required_person_cols <- c("DDBID", "KON", "FODDAT", "DODDAT")
  if (!all(required_person_cols %in% names(individ))) {
    stop("individ must contain DDBID, KON, FODDAT, and DODDAT.", call. = FALSE)
  }
  boende <- individ[, ..required_person_cols][boende, on = "DDBID"]

  message("Normalizing birth and death dates...")
  boende[, FODDAT := normalize_yyyymmdd_chr(FODDAT, default_month_day = "0101", missing_day = "01")]
  boende[, DODDAT := normalize_yyyymmdd_chr(DODDAT, default_month_day = "1231", missing_day = "30")]
  boende[, FOD_DATE := as_anydate_safe(FODDAT)]
  boende[, DOD_DATE := as_anydate_safe(DODDAT)]

  message("Normalizing residence intervals...")
  normalize_interval_dates(boende, "BOBDAT", "BOSDAT", "BOB_DATE", "BOS_DATE", end_day_for_missing = "30")

  if (!all(c("DDBID", "BONR") %in% names(boende))) {
    stop("boende must contain DDBID and BONR for ordering/fixing overlaps.", call. = FALSE)
  }

  setorder(boende, DDBID, BONR)
  boende[, nyobs      := DDBID == shift(DDBID, 1L, type = "lag")]
  boende[, problemdat := BOB_DATE < shift(BOS_DATE, 1L, type = "lag")]
  boende[, nystart    := shift(BOS_DATE, 1L, type = "lag")]
  boende[, BOB_DATE := as.Date(ifelse(nyobs & problemdat, nystart, BOB_DATE), origin = "1970-01-01")]
  if (nrow(boende) > 0) {
    boende[1, BOB_DATE := as_anydate_safe(BOBDAT[1])]
  }
  boende[, c("nyobs", "problemdat", "nystart") := NULL]

  message("Adding poverty indicator from yrke...")
  normalize_interval_dates(yrke, "YRKEBDAT", "YRKESDAT", "YRKE_BDATE", "YRKE_SDATE", end_day_for_missing = "30")
  setorder(yrke, DDBID, YRKE_BDATE)
  fattiga <- yrke[HISCOSTATUS == 13, .SD[1], by = DDBID]
  setkey(boende, DDBID)
  setkey(fattiga, DDBID)
  boende <- fattiga[, .(DDBID, HISCOSTATUS, YRKE_BDATE)][boende, on = "DDBID"]

  message("Adding disability information from lyten...")
  if (!all(c("LYTEBDAT", "LYTESDAT") %in% names(lyten))) {
    stop("lyten must contain LYTEBDAT and LYTESDAT.", call. = FALSE)
  }
  lyten[, lytenBDAT := as.character(LYTEBDAT)]
  lyten[, lytenSDAT := as.character(LYTESDAT)]
  normalize_interval_dates(lyten, "lytenBDAT", "lytenSDAT", "lyten_BDATE", "lyten_SDATE", end_day_for_missing = "30")
  setorder(lyten, DDBID, lyten_BDATE)
  lytiga <- lyten[, .SD[1], by = DDBID]

  setkey(boende, DDBID)
  setkey(lytiga, DDBID)
  lyte_cols <- intersect(c("DDBID", "lyten_BDATE", "LYTESTDNMN"), names(lytiga))
  boende <- lytiga[, ..lyte_cols][boende, on = "DDBID"]

  message("Saving outputs...")
  save(boende, file = output_files[["rdata"]])
  saveRDS(boende, file = output_files[["rds"]])

  message("Done.")
  message("Saved RData: ", output_files[["rdata"]])
  message("Saved RDS:   ", output_files[["rds"]])

  invisible(boende)
}

boende <- create_boende(input_files = input_files, output_files = output_files)
