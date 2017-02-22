rm(list = ls())
gc(reset = T)

# Loading functions and source code ---------------------------------------

# suppressPackageStartupMessages({
#   source_fnames <- dir("../src/", pattern = "^[a-z].*", full.names = T)
#   invisible( sapply(source_fnames, function(fname) source(fname)) )
# })




# placeholders for functions ----------------------------------------------

preprocess_data_source <- function(...) {
  return(NULL)
}

compute_share_base_v2 <- function(data, ...) {
  return(data)
}

determine_search_path_attributes <- function(...) {
  return(NULL)
}

get_market_data_per_object <- function(...) {
  return(NULL)
}

get_competitors_per_object <- function(...) {
  return(NULL)
}

search_objects <- function(...) {
  return(NULL)
}

# Reading data and metadata -----------------------------------------------

data_source_file_name    <- "../data/data_file.csv"
meta_data_file_name      <- "../data/metadata_file.csv"

tbls                    <- readr::read_csv(data_source_file_name)
meta_data               <- read_csv(meta_data_file_name)

# This adds columns based on lagged and/or windows functions
tbls %<>% preprocess_data_source()
one_value_columns <- tbls %>% determine_one_value_attributes(all_attributes)


# Business logic parameters -------------------------------------

# All these will need to come through the setup page and this code needs to be slightly modified
share_base_attributes   <- c("CATEGORY")
time_periods <- c("P1M")       # these can be P2M, P3M P6M etc. and determine which columns to use for computing (these are columns computed in the preprocessing using windowed functions)
referenece_periods <- c("PP")  # these are either PP or YA - and determine which columns to use for computing (these are lagged columns computed in the preprocessing)

share_base_values       <- list(CATEGORY = "LAUNDRY")

object_types <- c("BRAND")
object_values <- list("SOME_BRAND")
business_objects <- setNames(object_values, object_types)


# some algorithm specific initialization parameters ------------------------------------------

algo_type = "exhaustive"
method    = "0"

# this is a folder for storring individual algorithm runs - in some cases we simply need to reuse the same result so no purpose in recomputing
# TODO: Migrate it to a database
ex_search_results_output_folder <- "ex_search_results_output_folder"

# The parameters for the simple algebra before triggering the core algorithm

minimal_treshold <- 0.5
competitive_min_explanation_tresh <- 0.33
competitive_explanation_total_tresh <- 0.8

# initialization code -----------------------------------------------------

# The markets and marketlevels dataframe on which we operate. Typically has the Markets Market Levels
df_target <- unique(tbls[, c("Market", "Market_Level")])

# df_target %<>% filter(Market %in% top_20_markets)
# This adds the dates to df_target - here for small dependency - might not be needed at all
dates <- unique(tbls$Date)

sales_var_root_name       <- c("VALUE_SALES_MLC")
vol_var_root_name        <- c("VOLUME_SALES_MSU")


# Lists for storing the results -------------------------------------------

# These lists go into the output file at the end

# this two lists are currently populated for output
top_tables                <- list()
list_markets              <- list()




# Computation section -----------------------------------------------------


# First loop to parallelize is over time periods - This means we compute over different columns
for(time_period in time_periods) {
  
  # TODO: make this parametrization cleaner
  if(!time_period %in% c("P1M", "P1W")) {
    sales_var <- sales_var_root_name %>% paste(time_period, sep = "_")
    vol_var <- vol_var_root_name %>% paste(time_period, sep = "_")
  }
  
  # Second Loop to parallelize is different reference period - This means we compute over different columns
  for(ref_period in referenece_periods) {
    
    # set the reference period columns
    sales_var_pp    <- sales_var  %>% paste(ref_period, sep = "_")
    vol_var_pp      <- vol_var    %>% paste(ref_period, sep = "_")
    diff_decriptor  <- ref_period
    suffix_diff_ref_period <- paste("D", ref_period, sep = "")
    
    # Third loop is share base type - This means we compute over different chunks of the data (different row sections)
    for(i in 1:length(share_base_values)) {
      share_base_type <- names(share_base_values[i])
      
      # Fourth loop is share base value - This means we compute over different chunks of the data (different row sections)
      for(share_base_val in share_base_values[[i]]) {
        cat(share_base_type, ":" , share_base_val, "\n")
        
        # TODO: Decide whether compute_share_base_v2 returns a smaller chunk or uses an indicator column in the entire dataset (I think first option is better)
        # NOTE: 
        tbls %<>% compute_share_base_v2()
        
        val_share_base_ref_period <- paste("VAL_SHARE_BASE_MLC", ref_period, sep = "_")
        cur_df_target <- df_target
        
        # Fifth loop is over different columns (or product attributes) - for now two columns - MANUFACTURER and BRAND
        for(bot_idx in 1:length(business_objects)) {
          
          object_type <- names(business_objects)[bot_idx]
          # Sixt loop is over the different values of the the different columns example (Process only Ariel in BRAND or process only P&G in MANUFACTURER)
          for(bov_idx in 1:length(business_objects[[bot_idx]])) {

            obj_val <- business_objects[[bot_idx]][[bov_idx]]
            
            search_path_root_attribute <- object_type
            search_path_attributes <- determine_search_path_attributes(search_path_root_attribute)
            
            # Seventh loop is over different Market Levels 
            for(mlevel in unique(cur_df_target$Market_Level)) {
              
              cur_df_target_market_level <- cur_df_target %>% filter(Market_Level == mlevel)
              object_market_data <- cur_df_target_market_level %>% dlply(.var = "Market", function(y) { 
                
                object_market_data <- get_market_data_per_object(data)
                
                return(object_market_data)
              })
              
              # Note top_tables is an output that doesn't require the split by markets but is also not computationally intensive
              top_tables[[time_period]][[ref_period]][[share_base_type]][[share_base_val]][[object_type]][[obj_val]][[mlevel]] <- object_market_data
              
              # Eight loop is over different Markets in a Market level
              for(market in unique(cur_df_target_market_level$Market)) {
                
                object_and_competitors <- get_competitors_per_object()
                #
                if(is.null(object_and_competitors)) next

                # THIS RUNS THE CORE ALGORITHMS FOR a target object and its competitors - the output is then post-processed and several output lists are created
                objects_and_competitors_search <- object_and_competitors %>% search_objects()
                
                post_processed_output1 <- post_process1(objects_and_competitors_search)
                post_processed_output2 <- post_process2(objects_and_competitors_search)
                # TODO: parametrize Value Share By Market
                post_processed_output1_list[[time_period]][[ref_period]][[share_base_type]][[share_base_val]][[object_type]][[obj_val]][[mlevel]][[market]]  <- post_processed_output1
                post_processed_output2_list[[time_period]][[ref_period]][[share_base_type]][[share_base_val]][[object_type]][[obj_val]][[mlevel]][[market]]  <- post_processed_output2
                
                
              }
            }
          }
        }
      }
    }
  }
}

# The output object part should be further developed based on what is needed for the Shiny apps
# Only as a placeholder for now
save(top_tables, 
     list_markets,
     file = "temp_output_file.RData")

