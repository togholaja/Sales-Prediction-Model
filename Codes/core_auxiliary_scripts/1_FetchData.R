#########################################################
#
# Weather Impact on Sales
#
# Data extraction of store x (product) x date sales
#
#########################################################

# Load libraries
library(data.table)
library(lubridate)
library(RODBC)

options(scipen=999)
options(stringsAsFactors = FALSE)

# Save current date-time
start_time <- Sys.time()

# Open connection with Teradata dB
dbHandle <- odbcConnect(dsn = "Alpha_dsn")

#-------------------------------------------------------

# Create necessary folders

if(!dir.exists("./DataFolder")) dir.create("./DataFolder", showWarnings = F)
if(!dir.exists("./DataFolder/salesData_perSupergroup")) dir.create("./DataFolder/salesData_perSupergroup", showWarnings = F)
if(!dir.exists("./DataFolder/weatherSalesData_perSupergroup")) dir.create("./DataFolder/weatherSalesData_perSupergroup", showWarnings = F)

#-------------------------------------------------------

# Download sales data
getwd()
#---------------------------------------

# Import the supergroups of products, based on TotalCodes sets.

supergroupsDT <- fread("./DataFolder/Product_supergroups_hierarchy.csv")[Model_Grouping %in% c("Water", "Salad")]
supergroupsDT[, Model_Grouping:=gsub("/","_",Model_Grouping)]
supergroupsDT[, Model_Grouping:=gsub(" - ","_",Model_Grouping)]

supergroupsDT <- supergroupsDT[Sub_Section_Code>0 & Section_Code>0 & Department_Code>0]
supergroupsDT <- supergroupsDT[!grep("DO NOT USE",SubSect_Description)]
supergroupsDT <- supergroupsDT[!grep("DO NOT USE",Sect_Description)]

supergroupsDT[, TotalCode := Sub_Section_Code + Section_Code*100 + Department_Code*10000]
supergroups <- supergroupsDT[, unique(Model_Grouping)]


storesVec <- c()

for(curGroupInd in 1:length(supergroups)){
  curProductGroup <- supergroups[curGroupInd]
  cat("\nProduct supergroup under process: ",curProductGroup,"\n")
  
  TotalCodesSet <- supergroupsDT[Model_Grouping==curProductGroup, unique(TotalCode)]
  # Split the query in two, to avoid exceding Teradata user spool space. Otherwise, delete blanksin aQquery below.
  if(length(TotalCodesSet)>50) {
    threshold <- length(TotalCodesSet)%/%2
    sets <- list(TotalCodesSet[1:threshold], TotalCodesSet[(threshold+1):length(TotalCodesSet)])
  } else {
    sets <- list(TotalCodesSet)
  }
  runs <- 1:length(sets)
  
  a0 <- list()
  for(i in runs){
    aQuery <- paste0("select * from openquery(teradata,
                     'select a.ana_code, a.transaction_date, sum(a.net_sales_value_inc_vat + a.app_staff_discount_value) as Turnover 
                     from (select ana_code,transaction_date,consumer_unit_id,ean_code,net_sales_value_inc_vat,app_staff_discount_value from live_cim_view.daily_store_product_sales 
                     where Transaction_Date >= '\'",paste(startDateToDownload),"'\' (Date, format '\'dd/mm/yyyy'\') and Transaction_Date <= '\'",paste(endDateToDownload),"'\' (Date, format '\'dd/mm/yyyy'\')  ) a
                     inner join (select distinct consumer_unit_id,ean_code from live_cim_view.product_dimension_hierarchy  
                     where Sub_Section_Code + Section_Code*100 + Department_Code*10000 in (",paste(sets[[i]],collapse = ","),") and turnover_ind = 1) b 
                     on a.consumer_unit_id=b.consumer_unit_id and a.ean_code=b.ean_code inner join (select distinct ana_code from live_cim_view.store_dimension_hierarchy where region_code not in ('\'FR'\','\'NB'\','\'HS'\','\'HE'\')
                     and region_description not like '\'Detroit'\') c on a.ana_code=c.ana_code group by a.ana_code,a.transaction_date');")
    print(system.time({ a0[[i]] <- sqlQuery(channel = dbHandle,aQuery) }))
    setDT(a0[[i]])
  }
  salesMerged <- rbindlist(a0, use.names = TRUE, fill=TRUE)
  salesMerged <- data.table(ProductGroup=curProductGroup,salesMerged)
  if(length(runs)>1) {
    salesMerged <- salesMerged[, .(Turnover=sum(Turnover, na.rm=TRUE)),
                               by=.(ProductGroup, ANA_Code,Transaction_Date)]
  }
  #rm(a0,sets, runs); gc() # sets, runs
  
  
  # Check for duplicates (productGroup x store x date combos)
  anyDuplicatesDT <- salesMerged[duplicated(salesMerged[,.(ProductGroup, ANA_Code,Transaction_Date)])]
  if(nrow(anyDuplicatesDT)>0){
    cat('\nThere are duplicates at the store x productGroup x date sales data.\n')
    cat('\nThe first 10 duplicated records are the following:\n')
    head(anyDuplicatesDT[,.(ProductGroup,ANA_Code,Transaction_Date)],10)
    stop('\nR is exiting.',call. = FALSE)
  } else {cat('\nNo duplicates found.\n')}
  #rm(anyDuplicatesDT); gc()
  
  setorder(salesMerged,ANA_Code,Transaction_Date)
  
  saveRDS(salesMerged,file = paste0("./DataFolder/salesData_perSupergroup/dailyStoreProductSales_",curProductGroup,"_Group.RDS"))
  #tail(salesMerged)
  storesVec <- union(storesVec, salesMerged[,unique(ANA_Code)])
  #rm(salesMerged); gc()
  
}
saveRDS(storesVec, "./DataFolder/salesData_perSupergroup/storesVec.RDS")



#write.table(storesVec, file = "storesVec.txt", sep = " ",
 #           row.names = FALSE)

#write.csv(storesVec, file = "storesVec.csv")

#-------------------------------------------------------

close(dbHandle)

#-------------------------------------------------------


# Save the processing time in a txt file.
end_time <- Sys.time()

if(!dir.exists("Processing_times")) dir.create('./Processing_times',showWarnings = F)
cat(paste0('\nProcess for Sales Data Extraction from Teradata took: ',format(end_time - start_time),"\n"),file = './Processing_times/runTime.txt',append = FALSE)

print('End of FethData')
#-------------------------------------------------------


