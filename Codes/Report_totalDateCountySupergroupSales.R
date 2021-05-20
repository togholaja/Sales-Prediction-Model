#'---
#'title: "Weather Impact on Sales"
#'subtitle: "Investigate Association between Total Category Turnover and Weather Variables: Multivariate Analysis"
#'author: Tosan Ogholaja
#'date: '`r format(Sys.Date(),"%B %d,%Y")`'
#'output:
#'  html_document:
#'    toc: true
#'    toc_float: false
#'    theme: flatly
#'---



#+ echo=F
#    fig_width: 25

#+ echo=F, include=F

options(warn=-1)

#' `r knitr::opts_chunk$set(echo=FALSE) `
#' `r knitr::opts_chunk$set(include=FALSE) `



# Automatic compiling into Notebook.
# rmarkdown::render("./Codes/Report_totalDateCountySupergroupSales.R",output_file = "Report_totalDateCountySupergroupSales.html",output_dir = "./OutputFolder/")



# ----

# Aggregated sales on County-ProductGroup-date level (all stores and categories within group)

# ----

library(data.table)
library(lubridate)
library(zoo)
library(kableExtra)
library(RODBC)
library(dplyr)
library(formattable)
library(leaps)     # for best-subsets model selection
library(captioner) # for table captions and in-line references
library(sjPlot)    # for interaction plots based on regression models


options(scipen=999)
options(stringsAsFactors = FALSE)


# Open connection with Teradata dB
dbHandle <- odbcConnect(dsn = "Alpha_dsn") 


# Save current date-time
start_time <- Sys.time()

#-------------------------------------------------------

# Functions section

# teraQueryText should be embraced by double quotes ""  
teraQ <- function(conn=dbHandle, teraQueryText){
  library(data.table)
  aQuery <- paste0("select * from openquery(TERADATA, ' ", teraQueryText," ')")
  res <- sqlQuery(channel = conn, aQuery)
  if(any(grep('Microsoft', res, ignore.case = TRUE))) stop("\nSQL Message: \n",res)
  setDT(res)
  return(res)
}

#-------------------------------------------------------
getwd()
# Create/check necessary folders.

if(!dir.exists("../DataFolder")) stop("/DataFolder/ does not exist in the current working directory. Please check that the sales-weather extraction has taken place.")
if(!dir.exists("../DataFolder/DataForExcel")) dir.create("../DataFolder/DataForExcel", showWarnings = F)
if(!dir.exists("../OutputFolder")) dir.create("../OutputFolder", showWarnings = F)


# -----------------------Import sales-weather data--------------------------------#


# Add some more region info per store:
moreRegionInfo <- teraQ(teraQueryText = paste0("select ANA_Code, County from live_cim_view.store_dimension_hierarchy ")) 
setnames(moreRegionInfo, "Ana_Code","ANA_Code")

# ---

# Run process below for every supergroup of products, as found in DataFolder/weatherSalesData_perSupergroup.

salesFiles <- list.files("../DataFolder/weatherSalesData_perSupergroup",pattern = "_Group.RDS")
supergroups <- gsub(pattern = "weatherSalesData_", replacement = "", x = salesFiles)
supergroups <- gsub(pattern = "_Group.RDS", replacement = "", x = supergroups)

aggregatedDataList <- list()
for(curGroupInd in 1:length(supergroups)){
  curProductGroup <- supergroups[curGroupInd]
  cat("\nProduct supergroup under process: ",curProductGroup,"\n")
  
  # Load weather-sales data at store-day level. 
  salesWeatherData <- readRDS(paste0("../DataFolder/weatherSalesData_perSupergroup/weatherSalesData_",curProductGroup,"_Group.RDS"))
  salesWeatherData <- merge(salesWeatherData, moreRegionInfo, by="ANA_Code", all.x = TRUE)
  
  # Create variables at store-product level
  setorder(salesWeatherData, ANA_Code,ProductGroup,Transaction_Date)
  
  salesWeatherData[, mean3days_Temp:= round(rollmean(Temp,3, fill=NA, align = "right"),1), by=.(ANA_Code,ProductGroup)]
  salesWeatherData[is.na(mean3days_Temp), mean3days_Temp:=Temp]
  
  salesWeatherData[, last3days_Temp:= round(shift(rollmean(Temp, 3, fill=NA, align = "right"), n = 1, fill = NA, type = "lag"),1), by=.(ANA_Code,ProductGroup)]
  salesWeatherData[is.na(last3days_Temp), last3days_Temp:=Temp]
  
  salesWeatherData[, last7days_Temp:= round(shift(rollmean(Temp, 7, fill=NA, align = "right"), n = 1, fill = NA, type = "lag"),1), by=.(ANA_Code,ProductGroup)]
  salesWeatherData[is.na(last7days_Temp), last7days_Temp:=Temp]
  
  salesWeatherData[, weekday:=wday(Transaction_Date)]
  salesWeatherData[, weekday:=factor(weekday,levels = 1:7)]
  salesWeatherData[, monthNum:=factor(monthNum)]
  
  
  # Add holiday dummies for Xmas & Easter
  holidaysVarsNames <- c("Xmas","afterXmas","Easter")
  salesWeatherData[,(holidaysVarsNames):=0] 
  salesWeatherData[monthNum==12 & mday(Transaction_Date) %in% 20:25, Xmas:=1]
  salesWeatherData[monthNum==12 & mday(Transaction_Date) %in% 26:31, afterXmas:=1]
  
  yearsRange <- 2013:2023
  easterDates <- as.Date(timeDate::Easter(year=yearsRange, shift=rep(-6:0,length(yearsRange) )))
  salesWeatherData[Transaction_Date %in% easterDates, Easter:=1]
  
  
  # -----------------------Aggregate sales on county-productGroup-date level--------------------------------#
  
  
  # Kill Retail_Line_Code and ANA_Code. 
  aggregatedDataList[[curGroupInd]] <- salesWeatherData[, .(Turnover=sum(Turnover, na.rm = TRUE),
                                                            Temp=mean(Temp,na.rm = TRUE),
                                                            Min_Temp=mean(Min_Temp,na.rm = TRUE),
                                                            Max_Temp=mean(Max_Temp,na.rm = TRUE),
                                                            Rain=mean(Rain,na.rm = TRUE),
                                                            Min_Rain=mean(Min_Rain,na.rm = TRUE),
                                                            Max_Rain=mean(Max_Rain,na.rm = TRUE),
                                                            Cloud=mean(Cloud,na.rm = TRUE),
                                                            Min_Cloud=mean(Min_Cloud,na.rm = TRUE),
                                                            Max_Cloud=mean(Max_Cloud,na.rm = TRUE),
                                                            Wind_Speed=mean(Wind_Speed,na.rm = TRUE),
                                                            Min_Wind_Speed=mean(Min_Wind_Speed,na.rm = TRUE),
                                                            Max_Wind_Speed=mean(Max_Wind_Speed,na.rm = TRUE),
                                                            Feels_Like=mean(Feels_Like,na.rm = TRUE),
                                                            Min_Feels_Like=mean(Min_Feels_Like,na.rm = TRUE),
                                                            Max_Feels_Like=mean(Max_Feels_Like,na.rm = TRUE),
                                                            monthNum=unique(monthNum),
                                                            baseTemp=mean(baseTemp,na.rm = TRUE),
                                                            mean3days_Temp=mean(mean3days_Temp,na.rm = TRUE),
                                                            last3days_Temp=mean(last3days_Temp,na.rm = TRUE),
                                                            last7days_Temp=mean(last7days_Temp,na.rm = TRUE),
                                                            weekday=unique(weekday),
                                                            Easter=unique(Easter),
                                                            Xmas=unique(Xmas),
                                                            afterXmas=unique(afterXmas)
  ), 
  by=.(County, ProductGroup, Transaction_Date)]
  
  #rm(salesWeatherData); gc()
  
} # end of for-loop

str(aggregateSalesWeatherData3)
# Bind datasets for all product groups.
aggregateSalesWeatherData3 <- rbindlist(aggregatedDataList, use.names = TRUE, fill = TRUE)
#rm(aggregatedDataList); gc()

avgDailySalesPerCountyCategory <- aggregateSalesWeatherData3[, .(Turnover=sum(Turnover)), 
                                                             by=.(Transaction_Date, County, ProductGroup)][
                                                               , .(avgDailyTurnover=round(mean(Turnover),2)), 
                                                               by=.(County, ProductGroup)]
aggregateSalesWeatherData3 <- merge(aggregateSalesWeatherData3, 
                                    avgDailySalesPerCountyCategory, 
                                    by=c("County", "ProductGroup"))

#rm(avgDailySalesPerCountyCategory, moreRegionInfo); gc()

aggregateSalesWeatherData3 <- aggregateSalesWeatherData3[!is.na(County)]


# -----------------------Create additional variables--------------------------------#


# Create alternative Y variables, i.e. normalised turnover.
aggregateSalesWeatherData3[, Turnover_over_avgDailyTurnover := round(Turnover/avgDailyTurnover,3)]


aggregateSalesWeatherData3[, tempDeviationFromBase := Temp-baseTemp]

aggregateSalesWeatherData3[, tempDeviationFromlast3daysTemp := Temp-last3days_Temp]

aggregateSalesWeatherData3[, tempDeviationFromlast7daysTemp := Temp-last7days_Temp]

aggregateSalesWeatherData3[, TempSq := Temp^2]


saveRDS(aggregateSalesWeatherData3, file = "../DataFolder/DataForExcel/finalDataset.RDS")


# -----------------------Intro--------------------------------#


#'
#' # Intro
#' 
#' The current investigation is based on a data sample with the following characteristics:
#' 
#' * Transaction_Date range: 2017-07-01 up to 2019-07-01.
#' * Categories (or product groups): `r paste0(supergroups, collapse=", ")`.
#' * All category-county-dates across UK having positive sales (`r aggregateSalesWeatherData3[, uniqueN(County)]` counties).
#' 
#' <br>
#'     

#'     
#' ### Level of sales aggregation   
#'  
#' * The sales value or turnover (in GBP) has been aggregated at category-county-date level for the particular product groups/categories selected.
#' 
#' * The analysis has been performed separately for each product group. 
#'  


#' <a href="#top">Back to top</a>
#'
#'
#' <br>

#'
#' # Regression model fitting
#' 
#' In order to fit the regression models that follow (one model per product group), 
#' we have considered the original weather variables available in Teradata
#' plus some additional weather-related variables we have created, 
#' as well as some interaction terms on pairs of the aforementioned variables.
#' 
#' Each model includes:
#' 
#' * (turnover / avg daily turnover per category-county) as the dependent variable (Y), 
#' in order to normalise turnover so that the weather coefficients of the models are comparable, 
#' 
#' and may include some or all of the following covariates, depending on the results of the variable selection process:
#' 
#' * one or more weather variables and/or interaction terms, 
#' * weekday (1:7) (forced in all models),
#' * Xmas, afterXmas and Easter variables (in 0/1 form).
#' 
#'




# -----------------------Define the set of weather vars--------------------------------#

# Names of weather-related vars
firstWeatherColumn <- 5 
lastWeatherColumn <- 19 
weatherVarNames <- names(aggregateSalesWeatherData3)[firstWeatherColumn:lastWeatherColumn]

basicVars <- TRUE
if(basicVars==FALSE){
  weatherVarNames <- c(weatherVarNames, "TempSq", "baseTemp","mean3days_Temp", grep("Deviation", names(aggregateSalesWeatherData3), value = TRUE)) 
} else {
  weatherVarNames <- c(weatherVarNames, "TempSq", "tempDeviationFromBase", #"tempDeviationFromBasePercent",
                       "tempDeviationFromlast3daysTemp", "tempDeviationFromlast7daysTemp") 
}

rawWeatherVarNames <- c(weatherVarNames[c(1,4,7,10)], # 13
                        "TempSq", "tempDeviationFromBase", 
                        "tempDeviationFromlast3daysTemp", "tempDeviationFromlast7daysTemp") 

useRawWeatherVars <- TRUE
if(useRawWeatherVars) {
  weatherVarNamesFull <- weatherVarNames
  weatherVarNames <- rawWeatherVarNames
}


#'
#' 
#+ include=T

tableNums <- captioner(prefix="Table ")


kable(weatherVarNamesFull, caption = tableNums("fullsetXvars", caption="Full set of available explanatory variables" )) %>%
  kable_styling(bootstrap_options = "striped", full_width = F, position="left") %>%
  row_spec(row = 0, bold=TRUE, background = "lightblue")



#' 
#' <br>
#'

#'
#' __Note__: Due to the very high correlation between some groups of the above explanatory variables, 
#' the min/max variables have been removed. 
#' 
#' Therefore, for instance, only Temp has been retained to participate in the analysis below, 
#' while Max_Temp and Min_Temp have been removed, so as to avoid multicollinearity issues in the regression models.
#' 
#' Also, the Feels_Like group of variables is highly correlated with Temp (>90%), so it has also been removed. 
#' 
#' Finally, tempDeviationFromlast3daysTemp is highly correlated with tempDeviationFromlast7daysTemp (>85%); 
#' thus, only the latter was retained since it is more related to Temp and to normalised sales.  
#'   
getwd()
#' <a href="#top">Back to top</a>
#'
#'
#' <br>

# ----------------------------Model selection via LEAPS by subsection-------------------------------------#

avgDailyTurnoverDT <- aggregateSalesWeatherData3[, .(mean_avgDailyTurnover=mean(avgDailyTurnover)), by=.(ProductGroup)]

# ----


coefTablesList <- list()
bestLeapsModelRows <- list()
finalLMs_forInteractionPlots <- list()
coefTablesList_AllCovariates <- list()

for(curGroupInd in 1:length(supergroups)){
  
  curProductGroup <- supergroups[curGroupInd]
  
  # Define full model
  tempData3 <- na.omit(aggregateSalesWeatherData3[ProductGroup==curProductGroup,
                                                  c("Turnover_over_avgDailyTurnover",  "weekday", weatherVarNames, holidaysVarsNames), with=FALSE])
  # Remove some highly correlated variables if present:
  # if(cor(tempData3$Temp,tempData3$TempSq)>0.75) 
  # tempData3 <- tempData3[, !"TempSq"]
  # if(cor(tempData3$tempDeviationFromlast7daysTemp,tempData3$tempDeviationFromlast3daysTemp)>0.75) 
  tempData3 <- tempData3[, !"tempDeviationFromlast3daysTemp"]
  
  #tempData3 = as.data.table(tempData3)
  
  tempData3$TempCat <-  ifelse(tempData3$Temp <= 5, 0,
                               ifelse(tempData3$Temp >5 & tempData3$Temp <=10, 3,
                                      ifelse(tempData3$Temp >10 & tempData3$Temp <=20,5,10)))
  
  saveRDS(tempData3, paste0("../DataFolder/DataForExcel/tempData3.RDS"))
  
  maxModelsNum <- (ncol(tempData3))*3 
  forceInColumnInds <- c(1:6)
  models <- regsubsets(Turnover_over_avgDailyTurnover~. + Temp:Cloud + Temp:Rain + Temp:Wind_Speed
                       + Rain:Cloud + Cloud:Wind_Speed + Rain:Wind_Speed
                       + Temp:tempDeviationFromBase + Temp:tempDeviationFromlast7daysTemp,
                       data=tempData3, nvmax = maxModelsNum, force.in=forceInColumnInds)
  # leaps <- regsubsets(Turnover_over_avgDailyTurnover~., nbest = 1, method = "exhaustive", data = tempData3) # added by TO
  
  
  # summary(leaps)
  # install.packages("car", repos="http://cloud.r-project.org")
  #library(car)
  # subsets(leaps, statistic = "bic")
 
  modelSum <- summary(models)
  row.names(modelSum$outmat) <- substr(row.names(modelSum$outmat),1,2)
  numVars <- as.numeric(row.names(modelSum$outmat))
  leapsOutMatrix <- data.table(ProductGroup=curProductGroup, 
                               R2=round(modelSum$rsq*100,2), 
                               AdjR2=round(modelSum$adjr2*100,2), BIC=modelSum$bic,
                               NumberOfVariables=numVars,modelSum$outmat)
 # modelSum$adjr2 <- 1-(1-R2)*(n-1)/(n-p-1)
  
#  (modelSum$adjr2 * (n-p-1))/(n - 1)
  # Best BIC model
  bestModelIndex <- which.min(modelSum$bic)
  modelRowToKeep <- leapsOutMatrix[bestModelIndex]
  bestLeapsModelRows[[curGroupInd]] <- modelRowToKeep
  
  selectedVars <- names(modelSum$which[bestModelIndex,][modelSum$which[bestModelIndex,]==TRUE])[-1]
  if(any(grepl("weekday", selectedVars))){
    selectedVars <- selectedVars[-grep("weekday", selectedVars)]
    selectedVars <- c("weekday", selectedVars)  
  }
  if(any(grepl("monthNum", selectedVars))){
    selectedVars <- selectedVars[-grep("monthNum", selectedVars)]
    selectedVars <- c("monthNum", selectedVars)  
  }
  
  interactionVarsString <- paste0(selectedVars,collapse = " + ")
  eval(parse(text=paste0("finalLM <- lm(Turnover_over_avgDailyTurnover~  ", interactionVarsString,", data=tempData3)")))
  
  finalLMs_forInteractionPlots[[curGroupInd]] <- finalLM
  
  AvgSales <- avgDailyTurnoverDT[ProductGroup==curProductGroup,mean_avgDailyTurnover]
  FinalModelResultsTable <- data.table(Variable= names(finalLM$coefficients), Coef=finalLM$coefficients, 
                                       AvgCountyDailySales_forCategory=AvgSales,
                                       ExpectedSalesChange=round(AvgSales * finalLM$coefficients,2),
                                       P_value=round(summary(finalLM)$coef[,4],10))
  
  coefTablesList_AllCovariates[[curGroupInd]] <- FinalModelResultsTable
  
  Report_monthWeekdayHoliday <- FALSE
  if(Report_monthWeekdayHoliday==FALSE) {
    FinalModelResultsTable <- FinalModelResultsTable[!(Variable %like% "month") & !(Variable %like% "weekday") & !(Variable %in% holidaysVarsNames)]
  }
  
  # Add interpretation of coefs.
  FinalModelResultsTable[Variable=="(Intercept)", Interpretation:=paste0("The expected county turnover on a January Sunday, with all numeric variables equal to 0, is ",round(ExpectedSalesChange,2)," GBP.")]
  
  FinalModelResultsTable[Variable=="monthNum2", Interpretation:=paste0("The expected county turnover (GBP) on a February day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum3", Interpretation:=paste0("The expected county turnover (GBP) on a March day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum4", Interpretation:=paste0("The expected county turnover (GBP) on an April day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum5", Interpretation:=paste0("The expected county turnover (GBP) on a May day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum6", Interpretation:=paste0("The expected county turnover (GBP) on a June day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum7", Interpretation:=paste0("The expected county turnover (GBP) on a July day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum8", Interpretation:=paste0("The expected county turnover (GBP) on an August day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum9", Interpretation:=paste0("The expected county turnover (GBP) on a September day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum10", Interpretation:=paste0("The expected county turnover (GBP) on an October day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum11", Interpretation:=paste0("The expected county turnover (GBP) on a November day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  FinalModelResultsTable[Variable=="monthNum12", Interpretation:=paste0("The expected county turnover (GBP) on a December day is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to January (ceteris paribus).")]
  
  FinalModelResultsTable[Variable=="weekday2", Interpretation:=paste0("The expected county turnover (GBP) on a Monday is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to a Sunday (ceteris paribus).")]
  FinalModelResultsTable[Variable=="weekday3", Interpretation:=paste0("The expected county turnover (GBP) on a Tuesday is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to a Sunday (ceteris paribus).")]
  FinalModelResultsTable[Variable=="weekday4", Interpretation:=paste0("The expected county turnover (GBP) on a Wednesday is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to a Sunday (ceteris paribus).")]
  FinalModelResultsTable[Variable=="weekday5", Interpretation:=paste0("The expected county turnover (GBP) on a Thursday is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to a Sunday (ceteris paribus).")]
  FinalModelResultsTable[Variable=="weekday6", Interpretation:=paste0("The expected county turnover (GBP) on a Friday is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to a Sunday (ceteris paribus).")]
  FinalModelResultsTable[Variable=="weekday7", Interpretation:=paste0("The expected county turnover (GBP) on a Saturday is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," compared to a Sunday (ceteris paribus).")]
  
  FinalModelResultsTable[Variable=="Temp", Interpretation:=paste0("The expected county turnover (GBP) for a 1 degree increase in Temp is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="Rain", Interpretation:=paste0("The expected county turnover (GBP) for a 1 level increase in Rain (range:0-6) is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="Cloud", Interpretation:=paste0("The expected county turnover (GBP) for a 1% increase in Cloud is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="Wind_Speed", Interpretation:=paste0("The expected county turnover (GBP) for a 1m/hr increase in Wind_Speed is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="Feels_Like", Interpretation:=paste0("The expected county turnover (GBP) for a 1 degree increase in Feels_Like is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="TempSq", Interpretation:=paste0("The expected county turnover (GBP) for a 1 degree increase in TempSq is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="tempDeviationFromBase", Interpretation:=paste0("The expected county turnover (GBP) for a 1 unit increase in tempDeviationFromBase is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="tempDeviationFromlast3daysTemp", Interpretation:=paste0("The expected county turnover (GBP) for a 1 unit increase in tempDeviationFromlast3daysTemp is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  FinalModelResultsTable[Variable=="tempDeviationFromlast7daysTemp", Interpretation:=paste0("The expected county turnover (GBP) for a 1 unit increase in tempDeviationFromlast7daysTemp is ",round(ExpectedSalesChange,2)," ",ifelse(sign(ExpectedSalesChange)==1, "higher", "lower")," (ceteris paribus).")]
  
  FinalModelResultsTable[Variable %like% ":", Interpretation:="(See respective interaction plot at appropriate section below)"]
  
  coefTablesList[[curGroupInd]] <- FinalModelResultsTable
  
  # Save final LMs for excel file building.
  saveRDS(finalLMs_forInteractionPlots[[curGroupInd]], paste0("../DataFolder/DataForExcel/FinalLM_",curProductGroup,".RDS"))
}



# ---------------------------- Output per category ------------------------------------#

#' 
#' ## The best LEAPS model per category
#'

#'
#' LEAPS is an exhaustive search algorithm for best subsets, i.e. a procedure to select the best variables to fit a model.
#' 

#' `r tableNums("dotsMatrix", display="cite")` below illustrates the LEAPS output of the best model by category 
#' (according to the BIC criterion, see definition below). 
#' 
#' Note that the variables corresponding to weekday and holidays have been removed from the table below for easier inspection; 
#' however, they are included as covariates in the related models.
#' 
#'
#' <br>
#' 
#' **Definitions** for the table that follows: 
#' 
#' * __AdjR2__ is the adjusted R-square value (%) of the respective model.
#' * __BIC__ is the Bayesian Information Criterion for model selection among a finite set of models. It is based on notion of the likelihood function, and the smaller its value the better the model.
#' * __NumberOfVariables__ shows the number of explanatory variables included in the respective model. Note that we have forced the 6 weekday-related dummies to be always included in the models. 
#'

dotsMatrix <- rbindlist(bestLeapsModelRows, use.names = TRUE, fill=TRUE)

#+ include=T
kable(dotsMatrix[, !names(dotsMatrix) %like% "weekday", with=FALSE][, !holidaysVarsNames, with=FALSE], 
      caption = tableNums("dotsMatrix", caption="Output of the best LEAPS model per product group" )) %>%   
  kable_styling(bootstrap_options = "striped", full_width = F, position="left") %>%
  row_spec(row = 0, bold=TRUE, background = "lightblue") %>%
  column_spec(column = 4,border_right = T) %>%
  column_spec(column = 1,border_right = T)


#' <a href="#top">Back to top</a>
#'
#'
#' <br>

# ---------------------------------- Summary Tables of Models ----------------------------------#

paste0(c(tableNums("routputBICModel1", display="num"), tableNums("routputBICModel2", display="num"),tableNums("routputBICModel3", display="num"),tableNums("routputBICModel4", display="num"),tableNums("routputBICModel5", display="num"),tableNums("routputBICModel6", display="num")),collapse=",")

#'
#' ## Model Summary Per Category
#'
#' In this section, summarizing tables 
#' with coefficients are presented for each of the models of `r tableNums("dotsMatrix", display="cite")`.
#' 
#' Note that, as before, the variables corresponding to weekday and holidays have been removed from the tables below for easier inspection of the weather variables effect; 
#' however, they are included as covariates in the related models.
#' 

#' 
#' **Definitions** for the tables that follow: 
#' 
#' * __AvgCountyDailySales_forCategory__ is the average daily turnover across counties for a particular category. 
#' 
#' * __Coef__ is the percent change (%) in avg daily turnover (at the level of analysis, e.g. here it is at County-Category-Date) 
#' given a 1 unit increase of the respective explanatory variable of the model, ceteris paribus. 
#' For a categorical covariate, 
#' Coef is the percent change (%) in avg daily turnover at the current level of the covariate compared to the reference level of the covariate 
#' (e.g. Sunday is the reference level of weekday).
#' 
#' * __ExpectedSalesChange__ is the expected change in turnover for a unit increase in case of a quantitative explanatory variable, 
#' or the expected turnover change in the current level of a factor compared to its reference level 
#' (e.g. Sunday is the reference level of weekday).
#' It is calculated by multiplying the respective regression coefficient x AvgDailyTurnover for the level of analysis used 
#' (e.g. here AvgCountyDailySales_forCategory). 
#'
#'
#' 
#'--------------------------------------
#'
#' __Example of interpretation including interactions:__
#' 
#' Let us consider the Ambient Grocery category with the related figures of `r tableNums("routputBICModel6", display="cite")`. 
#' 
#' The figures that follow in this paragraph are not up to date, but are used for illustrative purposes.
#' 
#' Assume that the model includes, among others, Temp, tempDeviationFromlast7daysTemp and Temp:tempDeviationFromlast7daysTemp.
#' 
#' The ExpectedSalesChange is +52.29 GBP for 1 degree increase in Temp.
#' 
#' Also, the ExpectedSalesChange is -55.66 GBP for a 1 degree increase in tempDeviationFromlast7daysTemp, 
#' i.e. for a 1 degree increase in the deviation between Temp and the avg Temp of the last 7 days.
#' 
#' Suppose that Temp goes from 10 to 11 degrees (1 degree increase) and 
#' suppose that the deviation between temp and last7days average Temp goes from 2 to 3 degrees (1 degree increase). 
#' 
#' Thus, the interaction, which is a multiplication of the two involved terms (Temp & tempDeviationFromlast7daysTemp), 
#' goes from 10x2=20 to 11x3=33 (13 units increase). 
#' Note from `r tableNums("routputBICModel6", display="cite")` that the respective ExpectedSalesChange 
#' for a 1 unit increase in the interaction term is +7.74 GBP.
#' 
#' Finally, the total expected sales change is computed as the sum of the increment times the ExpectedSalesChange for each involved variable
#' and equals 1x52.31 + 1x(-51.89) +  13x7.57 = +97.25 GBP.
#' 
#' One can think of the interaction term playing the role of _acceleration_ for the ExpectedSalesChange.
#'
#'
#' 
#'--------------------------------------
#'


#' 
#' <br>
#' 



#+ include=T, results="asis", eval=TRUE, tidy=FALSE
for(i in 1:length(supergroups)){
  cat("  \n### Category: ",supergroups[i] ,"\n\n")
  cat(htmltools::HTML("<br>"))
  kable(coefTablesList[[i]][order(P_value,decreasing = F)],
        caption = tableNums(paste0("routputBICModel",i), caption="Summary of the best-BIC fitted model. Variables sorted by increasing p-value, i.e. decreasing significance." ))  %>%   
    kable_styling(bootstrap_options = "striped", full_width = T, position="left") %>%
    row_spec(row = 0, bold=TRUE, background = "lightblue")  %>%
    column_spec(column = 6, width_max = "35cm", width_min = "20cm") %>%
    print
  cat(htmltools::HTML('<a href="#top">Back to top</a>'))
  cat("  \n\n")
  cat(htmltools::HTML("<br><br>"))
  cat("  \n\n")
}



# ---------------------------------- Interaction Plots ----------------------------------#


#'
#' ## Interaction Plots per Category
#'
#' Below follow the plots of the marginal effects of the various interaction terms 
#' included in each model of `r tableNums("dotsMatrix", display="cite")`.
#' 
#' Thus, for each category, we get a different set of plots corresponding to the interaction terms included in the respective model.
#' 
#' __In simple words__, an interaction plot shows the expected normalized turnover for the different values of the respective interaction terms, 
#' given that the rest of the model covariates are equal to their average values if numerical,
#' or to their reference level if categorical (e.g. Sunday for weekday).
#' 
#' __For example__, in the Ambient Grocery category, the interaction plot for Temp:Wind_Speed shows that 
#' as temperature increases, the normalized turnover will increase given no wind (Wind_Speed=1m/hr),
#' but will decrease given very strong wind (Wind_Speed=47m/hr).
#' 




#+ include=T, results="asis", eval=TRUE, tidy=FALSE
for(i in 1:length(supergroups)){
  cat("  \n### Category: ",supergroups[i] ,"\n\n")
  cat(htmltools::HTML("<br>"))
  print(plot_model(finalLMs_forInteractionPlots[[i]], type="int"))
  cat("  \n\n")
  cat(htmltools::HTML('<a href="#top">Back to top</a>'))
  cat("  \n\n")
  cat(htmltools::HTML("<br>"))
  cat("  \n\n")
}



#' 
#'--------------------------------------
#'
#' <br><br>

# ----------------------------------END---------------------------------------------#

#-------------------------------------------------------


# Save the processing time in a txt file.
end_time <- Sys.time()

if(!dir.exists("Processing_times")) dir.create('../Processing_times',showWarnings = F)
cat(paste0('\nProcess for Modelling and Report Creation took: ',format(end_time - start_time),"\n"),file = '../Processing_times/runTime.txt',append = TRUE)

#-------------------------------------------------------

