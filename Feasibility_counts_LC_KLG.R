# Required packages
library("DBI")
library("RPostgres")
library("dplyr")
library("dbplyr")
library("CirceR")
library("CohortGenerator")
library("DatabaseConnector")
library("here")

# Change the following parameters with your own database information
server <- Sys.getenv("DB_SERVER_aurum_protocol_059")
# server <- Sys.getenv("DB_SERVER_aurum_202106")
user <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")
port <- Sys.getenv("DB_PORT")
host <- Sys.getenv("DB_HOST")

# Create database connection
connectionDetails <- DatabaseConnector::downloadJdbcDrivers("postgresql",here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms="postgresql", server = server, user = user, password = password, port = port, pathToDriver = here::here())

# Run this line of code for the function that gets the feasibility counts (defined afterwards)
# Please ensure you have the cohort json files in a folder called "(PATH)/JSONS/Feasibility_counts_cohorts", where (PATH) is the current working directory
FC_counts <- getFCounts(connectionDetails)
# Share the object "FC_counts" for feasibility assessment


################################################################################

# Function that gets the counts for all cohorts
# MISSING: directly ask for data type (diagnoses, drugs...) first??
getFCounts <- function(con) {
  # Empty df with cohorts to generate
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
  
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "LCStudyathon_FC_KLG")
  CohortGenerator::createCohortTables(connectionDetails = con,
                                      cohortDatabaseSchema = "results",
                                      cohortTableNames = cohortTableNames)
  # Generate the cohorts
  cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = con,
                                                         cdmDatabaseSchema = "public",
                                                         cohortDatabaseSchema = "results",
                                                         cohortTableNames = cohortTableNames,
                                                         cohortDefinitionSet = cohortsToCreate)
  
  # Get the cohort counts
  cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = con,
                                                   cohortDatabaseSchema = "results",
                                                   cohortTable = cohortTableNames$cohortTable)
  return(cohortCounts)
}
