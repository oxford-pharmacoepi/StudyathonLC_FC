library("DBI")
library("RPostgres")
library("dplyr")
library("dbplyr")
library("CirceR")
library("CohortGenerator")
library("DatabaseConnector")
library("here")

# Connect to database "OHDSI" way
server <- Sys.getenv("DB_SERVER_aurum_protocol_059")
user <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")
port <- Sys.getenv("DB_PORT")
host <- Sys.getenv("DB_HOST")
connectionDetails <- DatabaseConnector::downloadJdbcDrivers("postgresql",here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms="postgresql", server = server, user = user, password = password, port = port, pathToDriver = here::here())

#Check 
#tbl(db, sql("SELECT * FROM public.person limit 1"))

# empty df with cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using your cohorts
cohortJsonFiles <- list.files(path =paste0(here::here(), "/JSONS/Feasibility_counts_cohorts"), full.names = TRUE)

for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
  cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = i,
                                                       cohortName = cohortName, 
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))
}


# Generate cohort set against your dataset. cohortsGenerated contains list of cohortIds successfully generated against the CDM

cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "my_cohort_table")
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                    cohortDatabaseSchema = "results",
                                    cohortTableNames = cohortTableNames)
# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
cdmDatabaseSchema = "public",
cohortDatabaseSchema = "results",
cohortTableNames = cohortTableNames,
cohortDefinitionSet = cohortsToCreate)

# Get the cohort counts
cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
cohortDatabaseSchema = "results",
cohortTable = cohortTableNames$cohortTable)
print(cohortCounts)

# check with Shiny App for them (https://dpa-pde-oxford.shinyapps.io/DementiaDusCohortDiagnosticsShiny/), and counts are 8606 in Aurum, indeed! Yey!


# dementia added

# PASC, influenza downloaded

# covid-19 infection, reinfection and negative test missing
# nco missing
# long covid missing
# diabetes missing 
# cancer missing
# other post covid missing



# Non OHDSI way of connecting, by DBI, which might be more efficient for other purposes

#server_dbi <- Sys.getenv("DB_SERVER_DBI_aurum_protocol_059")
#db <- dbConnect(RPostgres::Postgres(),
#            dbname = server_dbi,
#            port = port,
#            host = host,
#            user = user,
#            password = password)

# cohort <- tbl(db, sql("SELECT * FROM results.my_cohort_table")) # we can query and see a table
# cohort %>% filter(lubridate::year(cohort_start_date) == 2019) # filter for a given year
# cohort2019 <- cohort2019 %>% compute() # to create a temporary table in the database, not only a query
# cohort2019 <- cohort2019 %>% collect() # to bring the table to the local system 

# cdm <- CDMConnector::cdm_from_con(db, cdm_schema = "public", write_schema = "results", cohort_tables = "my_cohort_table") # easier syntaxis
# cdm$person
# cdm$my_cohort_table
# cohort <-cdm$my_cohort_table %>% inner_join(cdm$person %>% rename("subject_id" = "person_id")) # things we can look at 
