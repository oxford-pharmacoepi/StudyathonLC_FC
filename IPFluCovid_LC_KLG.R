library("DBI")
library("RPostgres")
library("dplyr")
library("dbplyr")
library("CirceR")
library("CohortGenerator")
library("DatabaseConnector")
library("here")
library("ggplot2")

# Connect to database "OHDSI" way
server <- Sys.getenv("DB_SERVER_gold_202007")
#server <- Sys.getenv("DB_SERVER_aurum_protocol_059")
user <- Sys.getenv("DB_USER")
password <- Sys.getenv("DB_PASSWORD")
port <- Sys.getenv("DB_PORT")
host <- Sys.getenv("DB_HOST")
connectionDetails <- DatabaseConnector::downloadJdbcDrivers("postgresql",here::here())
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms="postgresql", server = server, user = user, password = password, port = port, pathToDriver = here::here())

# Empty df with cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using your cohorts
cohortJsonFiles <- list.files(path =paste0(here::here(), "/JSONS/Influenza_covid_cohorts"), full.names = TRUE)

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

# Generate cohort set against dataset. cohortsGenerated contains list of cohortIds successfully generated against the CDM
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
#cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
#                                                 cohortDatabaseSchema = "results",
#                                                 cohortTable = cohortTableNames$cohortTable)
#print(cohortCounts)


# Work with the cohort more easily
server_dbi <- Sys.getenv("DB_SERVER_DBI_gold_202007")
# server_dbi <- Sys.getenv("DB_SERVER_DBI_aurum_protocol_059")
db <- dbConnect(RPostgres::Postgres(),
            dbname = server_dbi,
            port = port,
            host = host,
            user = user,
            password = password)

 cohort <- tbl(db, sql("SELECT * FROM results.my_cohort_table")) 
 
 start_date <- "2017-01-01"
 end_date <- "2022-01-01"
 
 # select 1 for year calculations, select 2 for monthly calculations
 time_period <- 1
 
 
 # run like: compute_IC_FluCovid(time_period,start_date,end_date). Function defined below.
 IC_FluCovid <- compute_IC_FluCovid(time_period, start_date, end_date)
 
 
 # function definition
 compute_IC_FluCovid <- function(time_period, start_date, end_date) {
   
 start_year <- lubridate::year(start_date)
 
 if(time_period == 1){
   diff_time <- as.numeric(difftime(as.Date(end_date), as.Date(start_date), units = "days" )) %/% 365
 } else if (time_period == 2) {
   diff_time <- as.numeric(difftime(as.Date(end_date), as.Date(start_date), units = "days" )) %/% 30
 } else {
   print("Your time_period is neither 1 (yearly calculation) nor 2 (monthly calculation). Please introduce a correct integer.")
   stop()
 }
 
 print("Time periods computed.")
 
 # this is not perfect, doesn't take into account lap years
 monthly_days <- c(31,28,31,30,31,30,31,31,30,31,30,31)
 
 dates <- c(start_date)
 for(i in 1:diff_time) {
   if(time_period == 1) {
     dates <- append(dates,as.character(as.Date(start_date)+i*lubridate::years(1)))
     years <- lubridate::year(dates)
   } else if (time_period == 2) {
     y <- (i-1) %/% 12
     e <- (i-1) %% 12 +1
     dates <- append(dates,as.character(as.Date(start_date)+lubridate::years(y)+lubridate::days(sum(monthly_days[1:e]))))
   } else {
     print("Your time_period is neither 1 (yearly calculation) nor 2 (monthly calculation). Please introduce a correct integer.")
     stop()
   }
 }
 
 print("Date vectors created.")
 
 if(time_period == 1) {
   numerator <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
   colnames(numerator) <- years
   rownames(numerator) <- cohortsGenerated$cohortId
   
   for(y in years) {
     # filter for a given year  
     counts_working <- cohort %>% filter(lubridate::year(cohort_start_date) == y) %>% group_by(cohort_definition_id) %>% count()
     for (c in cohortsGenerated$cohortId) {
       # add the counts of each cohort to its place in the summary table
       number <- counts_working %>% filter(cohort_definition_id == c) %>% select(n) %>% collect()
       check_empty <- number %>% tally() %>% pull()
       if(check_empty == 0) {
         numerator[as.character(c),as.character(y)] <- 0
       } else {
         numerator[as.character(c),as.character(y)] <- as.numeric(number[[1]]) # could not find a better way to extract the number from the tibble
       }     
       }
   }
 } else if(time_period == 2) {
   numerator <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(dates))
   months <- 1:length(dates)
   colnames(numerator) <- months
   rownames(numerator) <- cohortsGenerated$cohortId
   
   for(m in months) {
     # filter for a given year or month
     ye <- start_year + (m-1) %/% 12
     mo <- (m-1) %% 12 + 1
     
     counts_working <- cohort %>% filter(lubridate::year(cohort_start_date) == ye) %>% filter(lubridate::month(cohort_start_date) == mo) %>% group_by(cohort_definition_id) %>% count()
     for (c in cohortsGenerated$cohortId) {
       # add the counts of each cohort to its place in the summary table
       number <- counts_working %>% filter(cohort_definition_id == c) %>% select(n) %>% collect()
       check_empty <- number %>% tally() %>% pull()
       if(check_empty == 0) {
         numerator[as.character(c),m] <- 0
       } else {
         numerator[as.character(c),m] <- as.numeric(number[[1]]) # could not find a better way to extract the number from the tibble
       }
     }
   }
 } else {
   print("Your time_period is neither 1 (yearly calculation) nor 2 (monthly calculation). Please introduce a correct integer.")
   stop()
 }

 print("Numerator (number of events) calculated.")
 
# Compute the denominator for both incidence and prevalence (source population)
 pop <- tbl(db, sql("SELECT * FROM public.observation_period"))
 
 if(time_period == 1) {
   denominator <- matrix(data=NA,nrow=2,ncol=length(years))
   colnames(denominator) <- years
   rownames(denominator) <- c("Incidence","Prevalence")
   
   i <- 1
   for(y in years) {
     d <- dates[i]
     d_end <- as.Date(dates[i+1]) - lubridate::days(1)
     d_end <- as.character(d_end)
     working_pop <- pop %>% filter(observation_period_end_date >= d) %>% mutate(t_end_date = ifelse(observation_period_end_date > d,as.Date(d_end),observation_period_end_date )) %>% filter(observation_period_start_date <= d) %>% mutate(t_start_date = ifelse(observation_period_start_date < d,as.Date(d),observation_period_start_date )) %>% collect() %>% mutate(working_days = as.numeric(difftime(t_end_date + lubridate::days(1), t_start_date, units="days")))
     denominator[1,as.character(y)] <- sum(working_pop$working_days)
     denominator[2,as.character(y)] <- working_pop %>% tally() %>% pull()
     i <- i+1
   }
 } else if(time_period == 2) {
   denominator <- matrix(data=NA,nrow=2,ncol=length(months))
   colnames(denominator) <- months
   rownames(denominator) <- c("Incidence","Prevalence")
   
    for(m in months) {
      ye <- start_year + (m-1) %/% 12
      mo <- (m-1) %% 12 + 1
      d <- dates[m]
      d_end <- paste0(as.integer(ye),"-",as.integer(mo),"-",monthly_days[mo])
     working_pop <- pop %>% filter(observation_period_end_date >= d) %>% mutate(t_end_date = ifelse(observation_period_end_date > d, as.Date(d_end),observation_period_end_date )) %>% filter(observation_period_start_date <= d) %>% mutate(t_start_date = ifelse(observation_period_start_date < d,as.Date(paste0(as.integer(ye),"-",as.integer(mo),"-01")),observation_period_start_date )) %>% collect() %>% mutate(working_days = as.numeric(difftime(t_end_date + lubridate::days(1), t_start_date, units="days")))
     denominator[1,m] <- sum(working_pop$working_days)
     denominator[2,m] <- working_pop %>% tally() %>% pull()
    }
 } else {
   print("Your time_period is neither 1 (yearly calculation) nor 2 (monthly calculation). Please introduce a correct integer.")
   stop()
 }

 print("Denominator (source population) collected.")

# Finally compute Incidence and Prevalence, with CIs. Incidence is per 100,000 person-years. Prevalence is per persons. 
 
if(time_period == 1) {
  Incidence <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
  colnames(Incidence) <- as.character(years)
  rownames(Incidence) <- cohortsGenerated$cohortId 
  Incidence_L <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
  colnames(Incidence_L) <- as.character(years)
  rownames(Incidence_L) <- cohortsGenerated$cohortId 
  Incidence_H <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
  colnames(Incidence_H) <- as.character(years)
  rownames(Incidence_H) <- cohortsGenerated$cohortId 
  Prevalence <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
  colnames(Prevalence) <- as.character(years)
  rownames(Prevalence) <- cohortsGenerated$cohortId 
  Prevalence_L <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
  colnames(Prevalence_L) <- as.character(years)
  rownames(Prevalence_L) <- cohortsGenerated$cohortId 
  Prevalence_H <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(years))
  colnames(Prevalence_H) <- as.character(years)
  rownames(Prevalence_H) <- cohortsGenerated$cohortId 
  
  for(y in years) {
    for(c in cohortsGenerated$cohortId) {
      Incidence[c,as.character(y)] <- numerator[c,as.character(y)]/(denominator["Incidence",as.character(y)]/365.25)*100000
      Prevalence[c,as.character(y)] <- numerator[c,as.character(y)]/(denominator["Prevalence",as.character(y)])
      
      Incidence_L[c,as.character(y)] <- 100000 * stats::qchisq(0.05 / 2, df = 2 * (numerator[c,as.character(y)] - 1))/2 / (denominator["Incidence",as.character(y)]/365.25)
      Incidence_H[c,as.character(y)] <- 100000 * stats::qchisq(1 - 0.05 / 2, df = 2 * numerator[c,as.character(y)])/2 / (denominator["Incidence",as.character(y)]/365.25)
      
      Prevalence_L[c,as.character(y)] <- Prevalence[c,as.character(y)] - stats::qnorm(0.975) *  sqrt(Prevalence[c,as.character(y)] * (1 - Prevalence[c,as.character(y)]) / denominator["Prevalence",as.character(y)])
      Prevalence_H[c,as.character(y)] <- Prevalence[c,as.character(y)] + stats::qnorm(0.975) *  sqrt(Prevalence[c,as.character(y)] * (1 - Prevalence[c,as.character(y)]) / denominator["Prevalence",as.character(y)])
    }
  }
  
} else if(time_period == 2) {
  Incidence <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(months))
  colnames(Incidence) <- as.character(months)
  rownames(Incidence) <- cohortsGenerated$cohortId 
  Incidence_L <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(months))
  colnames(Incidence_L) <- as.character(months)
  rownames(Incidence_L) <- cohortsGenerated$cohortId 
  Incidence_H <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(months))
  colnames(Incidence_H) <- as.character(months)
  rownames(Incidence_H) <- cohortsGenerated$cohortId 
  Prevalence <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(months))
  colnames(Prevalence) <- as.character(months)
  rownames(Prevalence) <- cohortsGenerated$cohortId 
  Prevalence_L <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(months))
  colnames(Prevalence_L) <- as.character(months)
  rownames(Prevalence_L) <- cohortsGenerated$cohortId 
  Prevalence_H <- matrix(data=NA,nrow=nrow(cohortsGenerated),ncol=length(months))
  colnames(Prevalence_H) <- as.character(months)
  rownames(Prevalence_H) <- cohortsGenerated$cohortId 
  
  for(m in months) {
    for(c in cohortsGenerated$cohortId) {
      Incidence[c,m] <- numerator[c,m]/(denominator["Incidence",m]/365.25)*100000
      Prevalence[c,m] <- numerator[c,m]/(denominator["Prevalence",m])
      
      Incidence_L[c,m] <- 100000 * stats::qchisq(0.05 / 2, df = 2 * (numerator[c,m] - 1))/2 / (denominator["Incidence",m]/365.25)
      Incidence_H[c,m] <- 100000 * stats::qchisq(1 - 0.05 / 2, df = 2 * numerator[c,m])/2 / (denominator["Incidence",m]/365.25)
      
      Prevalence_L[c,m] <- Prevalence[c,m] - stats::qnorm(0.975) *  sqrt(Prevalence[c,m] * (1 - Prevalence[c,m]) / denominator["Prevalence",m])
      Prevalence_H[c,m] <- Prevalence[c,m] + stats::qnorm(0.975) *  sqrt(Prevalence[c,m] * (1 - Prevalence[c,m]) / denominator["Prevalence",m])
    }
  }
  
} else {
  print("Your time_period is neither 1 (yearly calculation) nor 2 (monthly calculation). Please introduce a correct integer.")
  stop()
}

 print("Incidence and Prevalence tables done. Only the plots are left!")
 
# Join the results in a tibble with which we can produce barplots
 
 if(time_period == 1) {
   Results <- cbind(rep(years,each=length(cohortsGenerated$cohortId)),rep(cohortsGenerated$cohortId,length(years)),c(Incidence),c(Incidence_L),c(Incidence_H),c(Prevalence),c(Prevalence_L),c(Prevalence_H))
   colnames(Results) <- c("year","cohort_id","Inc","Inc_l","Inc_u","Pre","Pre_l","Pre_u")
   Results <- as_tibble(Results) %>% mutate(year = as.integer(year), cohort_id = as.factor(cohort_id), Inc=as.numeric(Inc),Inc_l=as.numeric(Inc_l),Inc_u=as.numeric(Inc_u),Pre=as.numeric(Pre),Pre_l=as.numeric(Pre_l),Pre_u=as.numeric(Pre_u))
   
   print(ggplot(Results, aes(year, Inc, colour = cohort_id)) + geom_point() +  geom_errorbar(aes(ymin = Inc_l, ymax = Inc_u)) + scale_color_manual(labels=c("COVID-19","Influenza"), values = c("blue","red")) + theme(legend.title = element_blank()) + ylab("Incidence") + xlab("Year") + facet_grid(cohort_id ~ ., scales="free_y") + scale_x_continuous(breaks=seq(start_year,start_year+diff_time,1)))
   print(ggplot(Results, aes(year, Pre, colour = cohort_id)) + geom_point() +  geom_errorbar(aes(ymin = Pre_l, ymax = Pre_u)) + scale_color_manual(labels=c("COVID-19","Influenza"), values = c("blue","red")) + theme(legend.title = element_blank()) + ylab("Prevalence") + xlab("Year") + facet_grid(cohort_id ~ ., scales="free_y") + scale_x_continuous(breaks=seq(start_year,start_year+diff_time,1)))
 
   } else if(time_period == 2) {
   Results <- cbind(rep(dates,each=length(cohortsGenerated$cohortId)),rep(cohortsGenerated$cohortId,length(dates)),c(Incidence),c(Incidence_L),c(Incidence_H),c(Prevalence),c(Prevalence_L),c(Prevalence_H))
   colnames(Results) <- c("date","cohort_id","Inc","Inc_l","Inc_u","Pre","Pre_l","Pre_u")
   Results <- as_tibble(Results) %>% mutate(Inc=as.numeric(Inc),Inc_l=as.numeric(Inc_l),Inc_u=as.numeric(Inc_u),Pre=as.numeric(Pre),Pre_l=as.numeric(Pre_l),Pre_u=as.numeric(Pre_u))
   
   n <- diff_time%/%12
   for(i in 1:n) {
     ye <- start_year + i-1
     print(ggplot(Results[(1+24*(i-1)):(24*i),], aes(date, Inc, colour = cohort_id)) + geom_point() +  geom_errorbar(aes(ymin = Inc_l, ymax = Inc_u)) + scale_color_manual(labels=c("COVID-19","Influenza"), values = c("blue","red")) + theme(legend.title = element_blank()) + ylab("Incidence") + xlab("Date") + ggtitle(ye) + facet_grid(cohort_id ~ ., scales="free_y"))
   }
   for(i in 1:n) {
     print(ggplot(Results[(1+24*(i-1)):(24*i),], aes(date, Pre, colour = cohort_id)) + geom_point() +  geom_errorbar(aes(ymin = Pre_l, ymax = Pre_u)) + scale_color_manual(labels=c("COVID-19","Influenza"), values = c("blue","red")) + theme(legend.title = element_blank()) + ylab("Prevalence") + xlab("Date") + ggtitle(ye) + facet_grid(cohort_id ~ ., scales="free_y"))
   }
   
   } else {
     print("Your time_period is neither 1 (yearly calculation) nor 2 (monthly calculation). Please introduce a correct integer.")
     stop()
 }
 
 return(Results)
 
 }
 

 
