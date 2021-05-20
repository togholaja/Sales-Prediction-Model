#########################################################
#
# Weather Impact on Sales
#
# Parameters setting

#install.packages('data.table', repos = "http://cloud.r-project.org")
#install.packages('RODBC', repos = "http://cloud.r-project.org")
#install.packages('formattable', repos = "http://cloud.r-project.org")
#install.packages('xts', repos = "http://cloud.r-project.org")
#install.packages('leaps', repos = "http://cloud.r-project.org")
#install.packages('caret', repos = "http://cloud.r-project.org")
#install.packages('zoo', repos = "http://cloud.r-project.org")
#install.packages('captioner', repos = "http://cloud.r-project.org")
#install.packages('sjPlot' , repos = "http://cloud.r-project.org")
#install.packages('RcppRoll', repos = "http://cloud.r-project.org")
#install.packages('kableExtra', repos = "http://cloud.r-project.org")



library(RODBC)
library(data.table)
library(caret)
library(zoo)
library(formattable)
library(leaps)
library(sjPlot)
library(xts)
library(lubridate)

#
#########################################################
wd <- "O:/weather2/weather/Final_Deliverable/BS1_ProductGroupsTurnover" 
setwd(wd)
getwd()



#----------------------

# Open connection with Teradata dB.
dbHandle <- odbcConnect(dsn = "Alpha_dsn")

# How many years before the endDateToDownload should weather data start being loaded (affects baseTemp calculation):
yearsBeforeEndDateToDownload <- 2


last3months <- 150 # in days

# Start and End date to download data from Teradata; both dates in 'dd/mm/yyyy' format.
startDateToDownload <- '02/06/2019'
endDateToDownload <- '31/12/2019' #format(Sys.Date() -last3months,format="%d/%m/%Y")



#----------------------

