#########################################################
#
# Weather Impact
#
# Main running script
#
#########################################################


library(data.table)

# Set working directory
wd <- "Weather/Final_Deliverable/BS1_ProductGroupsTurnover" 
setwd(wd)

#-------------------------------------------------------

cat('Sales data fetching starts after parameter loading.\n')

source('./Codes/parametersSetting.R')
source('./Codes/core_auxiliary_scripts/1_FetchData.R')

cat('Sales data fetching has successfully completed.\n')

#-----------------

cat('Weather data fetching starts.\n')

rm(list=ls()) # clean environment

source('./Codes/parametersSetting.R')
source('./Codes/core_auxiliary_scripts/2_ExtractWeatherDataForStores.R')

cat('Weather data fetching has successfully completed.\n The weather data have been merged with the sales data. \n')

#-----------------

cat('Modelling and html report building starts.\n')

rm(list=ls()) # clean environment

source('./Codes/parametersSetting.R')
rmarkdown::render("./Codes/Report_totalDateCountySupergroupSales.R",output_file = "Report_totalDateCountySupergroupSales.html",output_dir = "./OutputFolder/")

cat('Modelling has successfully completed.\n An html report has been created in the Output folder. \n')

#-----------------


cat('Generate excel workbook starts.\n')

rm(list=ls()) # clean environment

source('./Codes/parametersSetting.R')
source('Supergroups_BuildBackwardLookingExcel.R')

cat('Excel workbook update has successfully completed.')

#-----------------
