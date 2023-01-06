# A response to Bill's data request:

# Would you have copy of the list of fish that the Stoud Water Research investigators found in Pinehurst Branch
# when they sampled on June 18th and 19th 2019.

#-----Criteria
# 1) Fish data only
#-- Species
#-- Count (n)
#-- length and weight (if available)
# 2) Select only "Pinehurst Branch"
# 3) Select only 20190618 and 20190619 data


library(data.table)
library(tidyverse)
library(prettyR)
library(dplyr)

library(RODBC)
db <- ("C:/Users/cwainright/OneDrive - DOI/Documents - NPS-NCRN-Biological Stream Sampling/General/Annual-Data-Packages/2022/NCRN_MBSS/NCRN_MBSS_be_2022.mdb")# https://doimspp.sharepoint.com/:u:/r/sites/NCRNBiologicalStreamSampling/Shared%20Documents/General/Annual-Data-Packages/2022/NCRN_MBSS/NCRN_MBSS_be_2022.mdb?csf=1&web=1&e=jjeJIg
con <- RODBC::odbcConnectAccess2007(db) # open db connection
db_objs <- RODBC::sqlTables(con) # test db connection
tbl_names <- db_objs %>%
    subset(TABLE_TYPE == "TABLE") %>%
    subset(TABLE_NAME %in% c("tbl_Fish_Data",
                             "tbl_Locations",
                             "tbl_Fish_Events",
                             "tbl_Events")) %>%
    select(TABLE_NAME)

# make list of queries so we can extract a few rows from each table
qry_list <- vector(mode="list", length=nrow(tbl_names))
names(qry_list) <- tbl_names$TABLE_NAME

# what query do you want to run on each table?
for (i in 1:length(qry_list)){
    qry_list[[i]] <- paste("SELECT * FROM", names(qry_list)[i])
}

# function that runs queries against an access db:
getQueryResults <- function(qryList, connection){ # accepts two arguments:
    # 1.`qryList` (a list-object of query char strings)
    # 2. `connection` an active odbc connection
    tryCatch(
        expr = {
            results_list <- vector(mode="list", length=length(qryList)) # instantiate empty list to hold query results
            names(results_list) <- names(qryList) # name elements in `results_list` to match `qryList` element names
            for(i in 1:length(qryList)){ # loop runs once per query
                results_list[[i]] <- RODBC::sqlQuery(connection, qryList[[i]]) # query the db and save result to `results_list`
            }
            assign("results_list", results_list, envir = globalenv()) # save query results as environment object `results_list`
            message( # console messaging
                for(j in 1:length(qryList)){ # print a message for each query
                    if(class(results_list[[j]]) != "data.frame"){ # if the query result isn't a data frame
                        e <- results_list[[j]][1] # grab the ODBC error message from [[j]][1]
                        cat(paste("'", names(results_list)[j], "'", " query failed. Error message: ", "'", e, "'", "\n", sep = "")) # print to console that an error occurred
                    } else {
                        cat(paste("Query for ", "'", names(qryList)[j], "'", " executed successfully", "\n", sep = "")) # print to console that query ran successfully
                    }
                }
            )
        },
        error = {
            function(error_msg){
                message("An error occurred when running the query:")
                message(error_msg)
            }
        },
        finally = {
            message("All queries have been executed") # message indicating the function job completed
        }
    )
}

getQueryResults(qryList = qry_list, connection = con)

RODBC::odbcCloseAll() # close db connection

# join tables

df <- results_list$tbl_Fish_Data %>%
    select(Fish_Species, Fish_Event_ID, Total_Pass_1, Total_Pass_2)
df <- dplyr::left_join(df, results_list$tbl_Fish_Events %>% select(Event_ID, Fish_Event_ID), by = "Fish_Event_ID")
df <- dplyr::left_join(df, results_list$tbl_Events %>% select(Event_ID, Start_Date, Location_ID), by = "Event_ID")
df <- dplyr::left_join(df, results_list$tbl_Locations %>% select(Location_ID, Loc_Name), by = "Location_ID")
df <- df %>% select(Fish_Species, Total_Pass_1, Total_Pass_2, Start_Date, Loc_Name)
df <- df %>% subset(format(as.Date(Start_Date),"%Y")==2019) %>%
    subset(Loc_Name == "Pinehurst Branch")

data.table::fwrite(df, "data/20230106_data_request.csv")
