#########################################################
#
# Weather Impact on Sales
#
# Weather data for stores
#
#########################################################

library(data.table)
library(lubridate)
library(RODBC)
library(RcppRoll)
library(zoo)

options(scipen=999)
options(stringsAsFactors = FALSE)

# Save current date-time
start_time <- Sys.time()


# Open connection with Teradata dB
dbHandle <- odbcConnect(dsn = "Alpha_dsn")
#-------------------------------------------------------

# Download weather data only once for all the stores of the data.

# Load overall stores vector
overallANA_Codes <- paste0(readRDS("./DataFolder/salesData_perSupergroup/storesVec.RDS"))

startDateToDownloadWeather <- startDateToDownload 
# A. Fetch weather data for selected stores
a1 <- list()
init <- 1 ; fin <- 500
for(i in 1:6){
  aQuery <- paste0("select * from openquery(teradata,
                   'select * from daily_store_weather_data where ana_code in (",paste(overallANA_Codes[init:fin],collapse = ","),")
                   and Actual_Date >= '\'",paste(startDateToDownloadWeather),"'\' (Date, format '\'dd/mm/yyyy'\')  
                   and Actual_Date <= '\'",paste(endDateToDownload),"'\' (Date, format '\'dd/mm/yyyy'\')  ');")
  
  a1[[i]] <- sqlQuery(channel = dbHandle,aQuery)
  setDT(a1[[i]])
  
  init <- fin+1
  if(i==5) {fin <- length(overallANA_Codes)}  else fin <- fin+500
}

weatherData <- rbindlist(a1, use.names = TRUE, fill=T)
#rm(a1); gc() 

setnames(weatherData,"Actual_Date","Transaction_Date")

setorder(weatherData,ANA_Code,Transaction_Date)

# Remove duplicated rows, if any.
weatherData <- weatherData[!duplicated(weatherData[,.(ANA_Code,Transaction_Date)])]

# Fill-in missing Rain values, translating to 0 (No Rain).
weatherData[is.na(Rain), Rain:=0]
weatherData[is.na(Max_Rain), Max_Rain:=0]
weatherData[is.na(Min_Rain), Min_Rain:=0]

#----------------------------------------------

# B. Fetch stores' region code.
a2 <- list()
init <- 1 ; fin <- 500
for(i in 1:6){
  aQuery <- paste0("select * from openquery(teradata,
                   'select ana_code,region_code from store_dimension_hierarchy where ana_code in (",paste(overallANA_Codes[init:fin],collapse = ","),")');")
  a2[[i]] <- sqlQuery(channel = dbHandle,aQuery)
  setDT(a2[[i]])
  init <- fin+1
  if(i==5) {fin <- length(overallANA_Codes)}  else fin <- fin+500
}


storesRegion <- rbindlist(a2, use.names=TRUE, fill=T) 
#rm(a2); gc()
close(dbHandle)

setnames(storesRegion,'Ana_Code','ANA_Code')

storesRegion <- storesRegion[!duplicated(storesRegion[,.(ANA_Code,Region_Code)])]

#----------------------------------------------

# Merge weather data with region.
mergedDataWeatherRegion <- merge(weatherData, storesRegion, by=c('ANA_Code'),all.x = T,all.y = T)
#class(mockDates)

#rm(weatherData); gc()
#mergedDataWeatherRegion[mergedDataWeatherRegion$Transaction_Date == '2018-01-21' & mergedDataWeatherRegion$ANA_Code == 5000128096603]

# Stores with missing Temp data:
storesWithoutWeatherData <- mergedDataWeatherRegion[is.na(Temp), .(ANA_Code,Region_Code)]
uniqueRegionDate <- unique(mergedDataWeatherRegion[,.(Region_Code,Transaction_Date)])[!is.na(Transaction_Date)]
mockDates <- merge(storesWithoutWeatherData, uniqueRegionDate, by=c("Region_Code"))

# Remove stores without weather data
mergedDataWeatherRegion <- mergedDataWeatherRegion[!is.na(Temp),]

mergedDataWeatherRegion[,meanTempRegionDate:=round(mean(Temp,na.rm=T),0),by=.(Region_Code,Transaction_Date)]
mockDates <- merge(mockDates,
                   unique(mergedDataWeatherRegion[,.(Transaction_Date,Region_Code,meanTempRegionDate)]),
                   by=c('Region_Code','Transaction_Date'))

mergedDataWeatherRegion <- merge(mergedDataWeatherRegion,mockDates,all.x = T,all.y = T,by=c('Region_Code','Transaction_Date','ANA_Code','meanTempRegionDate'))
suppressWarnings(mergedDataWeatherRegion[is.na(Temp) & !is.na(meanTempRegionDate), Temp:=meanTempRegionDate])

#-------------------------------------------------------

# Run process below for every supergroup of products, as found in DataFolder/salesData_perSupergroup.

salesFiles <- list.files("./DataFolder/salesData_perSupergroup",pattern = "_Group.RDS")
supergroups <- gsub(pattern = "dailyStoreProductSales_", replacement = "", x = salesFiles)
supergroups <- gsub(pattern = "_Group.RDS", replacement = "", x = supergroups)

for(curGroupInd in 1:length(supergroups)){
  curProductGroup <- supergroups[curGroupInd]
  cat("\nProduct supergroup under process: ",curProductGroup,"\n")
  
  # Load sales data. 
  storeProductSales_daily <- readRDS(paste0("./DataFolder/salesData_perSupergroup/dailyStoreProductSales_",curProductGroup,"_Group.RDS"))
  
  # Compute quantities that will later help in computing base temperature.
  mergedDataWeatherRegion_temp <- merge(mergedDataWeatherRegion, storeProductSales_daily[,.(ANA_Code, Transaction_Date)], 
                                        all.x = TRUE, all.y = TRUE,by=c("ANA_Code", "Transaction_Date") )
  mergedDataWeatherRegion_temp[,weekNum:=week(Transaction_Date)]
  mergedDataWeatherRegion_temp[,monthNum:=month(Transaction_Date)]
  mergedDataWeatherRegion_temp[,meanWeekRegionTemp:=round(mean(Temp,na.rm=T),0), by=.(weekNum,Region_Code)]
  mergedDataWeatherRegion_temp[,meanWeekTemp:=round(mean(Temp,na.rm=T),0), by=.(weekNum)]
  mergedDataWeatherRegion_temp[,meanMonthRegionTemp:=round(mean(Temp,na.rm=T),0), by=.(monthNum,Region_Code)]
  mergedDataWeatherRegion_temp[,meanMonthTemp:=round(mean(Temp,na.rm=T),0), by=.(monthNum)]
  
  
  # Merge weather data with sales data.
  storeProductSales_withWeather <- merge(storeProductSales_daily, mergedDataWeatherRegion_temp, 
                                         by=c("ANA_Code","Transaction_Date"),all.x = T)
  #rm(storeProductSales_daily, mergedDataWeatherRegion_temp); gc()
  
  # Compute base temperature
  storeProductSales_withWeather[,Temp:=as.numeric(Temp)]
  storeProductSales_withWeather[is.na(Temp) & !is.na(meanWeekRegionTemp), ':='(Temp=meanWeekRegionTemp,
                                                                               baseTemp=meanWeekRegionTemp)]
  storeProductSales_withWeather[is.na(Temp) & !is.na(meanWeekTemp), ':='(Temp=meanWeekTemp,
                                                                         baseTemp=meanWeekTemp)]
  storeProductSales_withWeather[is.na(Temp) & !is.na(meanMonthRegionTemp), ':='(Temp=meanMonthRegionTemp,
                                                                                baseTemp=meanMonthRegionTemp)]
  storeProductSales_withWeather[is.na(Temp) & !is.na(meanMonthTemp), ':='(Temp=meanMonthTemp,
                                                                          baseTemp=meanMonthTemp)]
  storeProductSales_withWeather[is.na(baseTemp), baseTemp:=meanWeekRegionTemp]
  
  # Remove columns no longer needed.
  storeProductSales_withWeather[, (c("weekNum","meanWeekRegionTemp","meanWeekTemp","meanMonthRegionTemp","meanMonthTemp")):=NULL]
  
  saveRDS(storeProductSales_withWeather,
          paste0("./DataFolder/weatherSalesData_perSupergroup/weatherSalesData_",curProductGroup,"_Group.RDS"))
  #rm(storeProductSales_withWeather); gc()
}

#-------------------------------------------------------


# Save the processing time in a txt file.
end_time <- Sys.time()

if(!dir.exists("Processing_times")) dir.create('./Processing_times',showWarnings = F)
cat(paste0('\nProcess for Weather Data Extraction from Teradata took: ',format(end_time - start_time),"\n"),file = './Processing_times/runTime.txt',append = TRUE)

print('End of Weather Data')

#-------------------------------------------------------

