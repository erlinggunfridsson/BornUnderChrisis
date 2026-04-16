# install.packages(c("fs", "here"))  # kör vid behov

library(fs)
library(here)
library(purrr)
# ---- Definiera mappstruktur ----

dirs <- c(
  "R",
  "_targets",
  "data/raw",
  "data/interim",
  "data/derived",
  "output/war/tables",
  "output/war/figures",
  "output/war/models",
  "output/famine/tables",
  "output/famine/figures",
  "output/famine/models",
  "reports",
  "logs"
)

# Skapa mappar (idempotent)
walk(dirs, ~ dir_create(here(.x)))

# ---- Hjälpfunktion för filer ----

write_if_missing <- function(path, content = "") {
  full_path <- here(path)
  if (!file_exists(full_path)) {
    writeLines(content, full_path)
  }
}

# ---- _targets.R ----

write_if_missing(
  "_targets.R",
  c(
    "library(targets)",
    "library(tarchetypes)",
    "",
    "tar_option_set(",
    "  packages = c(",
    '    \"dplyr\", \"readr\", \"purrr\", \"tibble\",',
    '    \"tidyr\", \"stringr\", \"ggplot2\", \"broom\", \"rmarkdown\"',
    "  )",
    ")",
    "",
    "source(\"R/functions_targets.R\")",
    "",
    "cases <- tibble::tibble(",
    '  case = c(\"war\", \"famine\")',
    ")",
    "",
    "list()"
  )
)

# ---- Funktioner ----

write_if_missing(
  "R/functions_targets.R",
  c(
    "read_raw_data <- function(path) {",
    "  readr::read_csv(path, show_col_types = FALSE)",
    "}",
    "",
    "build_base_data <- function(path) {",
    "  readr::read_csv(path, show_col_types = FALSE) |>",
    "    dplyr::rename_with(tolower)",
    "}"
  )
)

# ---- RMarkdown-mallar ----

write_if_missing(
  "reports/paper_template.Rmd",
  c(
    "---",
    "title: \"Born under crisis\"",
    "output: html_document",
    "params:",
    "  case: \"war\"",
    "---",
    "",
    "```{r}",
    "case <- params$case",
    "```",
    "",
    "# Introduktion",
    "",
    "Detta är en parameteriserad rapport.",
    "",
    "# Resultat",
    "",
    "Här kommer analysen för `r case`."
  )
)

write_if_missing(
  "reports/war_paper.Rmd",
  c(
    "---",
    "title: \"Finska kriget\"",
    "output: html_document",
    "---",
    "",
    "# Introduktion",
    "",
    "Artikel om finska kriget."
  )
)

write_if_missing(
  "reports/famine_paper.Rmd",
  c(
    "---",
    "title: \"Storsvagåren\"",
    "output: html_document",
    "---",
    "",
    "# Introduktion",
    "",
    "Artikel om svältkrisen."
  )
)

# ---- Gitkeep-filer ----

gitkeep_files <- c(
  "data/raw/.gitkeep",
  "data/interim/.gitkeep",
  "data/derived/.gitkeep",
  "output/war/tables/.gitkeep",
  "output/war/figures/.gitkeep",
  "output/war/models/.gitkeep",
  "output/famine/tables/.gitkeep",
  "output/famine/figures/.gitkeep",
  "output/famine/models/.gitkeep",
  "logs/.gitkeep"
)

walk(gitkeep_files, ~ write_if_missing(.x, ""))

# ---- Klart ----

message("Projektstruktur skapad.") 
