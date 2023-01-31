# Instantiate feasibility counts cohorts
info(logger, "- getting cohort definitions")

FC_cohorts <- CDMConnector::readCohortSet(here(
  "JSONS",
  "All_cohorts"
))

info(logger, "- getting cohorts")


cdm <- CDMConnector::generateCohortSet(cdm, FC_cohorts,
                                       cohortTableName = cohort_table_name,
                                       overwrite = TRUE
)

info(logger, "- got cohorts")
