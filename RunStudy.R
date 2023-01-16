# Create output folder if it doesn't exist
if (!file.exists(output.folder)){
  dir.create(output.folder, recursive = TRUE)}

start <- Sys.time()

# Start log
log_file <- paste0(output.folder, "/log.txt")
logger <- create.logger()
logfile(logger) <- log_file
level(logger) <- "INFO"

# Instantiate study cohorts
info(logger, 'INSTANTIATING STUDY COHORTS')
source(here("InstantiateStudyCohorts.R"))
info(logger, 'GOT STUDY COHORTS')

# Count cohorts
# FC_counts <- cdm$lcstudyathon_fc_klg %>% group_by(cohort_definition_id) %>% tally()
longcovidCounts <- cdm[[cohort_table_name]] %>% 
  group_by(cohort_definition_id) %>% 
  tally() %>% 
  collect() %>% 
  right_join(FC_cohorts, by = c("cohort_definition_id"="cohortId")) %>% mutate(n = as.numeric(n)) %>% mutate(n = if_else(is.na(n), 0, n)) %>% select(cohortName,n)
info(logger, 'GOT COUNTS')

# Create zip file
zipName <- paste0(db.name,"_FCresults")
tempDir <- zipName
tempDirCreated <- FALSE
if (!dir.exists(tempDir)) {
  dir.create(tempDir)
  tempDirCreated <- TRUE
}

write.csv(longcovidCounts,
          file = file.path(
            tempDir,
            paste0(db.name,"_counts.csv")
          ),
          row.names = FALSE
)

zip::zip(zipfile = file.path(output.folder, paste0(zipName, ".zip")),
         files = list.files(tempDir, full.names = TRUE))
if (tempDirCreated) {
  unlink(tempDir, recursive = TRUE)
}
info(logger, 'SAVED RESULTS IN THE OUTPUT FOLDER')

print("Done!")
print("If all has worked, there should now be a zip file with your results in the output folder to share")
print("Thank you for running the study!")
Sys.time() - start
readLines(log_file)