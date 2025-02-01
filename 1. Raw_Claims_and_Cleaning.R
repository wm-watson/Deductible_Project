# 1. Raw Claims ----
## A. Companies by Year by Deduc Type ----
library(tidyverse)
library(beepr)
library(readr)
# Read and examine data

# Filepath
filepath <- "C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/final.csv"

# Read the header and the first 1 million lines (including the header)
lines <- read_lines(filepath, n_max = 1e6 + 1)  # +1 to include the header

# Write the selected lines to a new file
write_lines(lines, "selected_first_1m_rows.csv")

samp <- read_csv("selected_first_1m_rows.csv")


### Get Unique company, party, etc by deductible----

# Create a global variable to store results
final_results <- NULL
chunk_counter <- 0
total_rows_processed <- 0

callback <- function(x, pos) {
  # Update counters
  chunk_counter <<- chunk_counter + 1
  total_rows_processed <<- total_rows_processed + nrow(x)
  
  # Print progress
  print(paste("Processing chunk", chunk_counter, 
              "- Rows:", nrow(x),
              "- Years:", paste(unique(x$Plan_year), collapse = ", "),
              "- Total rows processed:", total_rows_processed))
  
  # Process chunk
  unique_rows <- x %>% 
    select(COMPANY_KEY, SUBGROUPKEY, PARTYKEY, DEDUCTIBLE_CATEGORY, Plan_year) %>%
    distinct()
  
  # Combine results
  if(is.null(final_results)) {
    final_results <<- unique_rows
  } else {
    final_results <<- bind_rows(final_results, unique_rows) %>% distinct()
  }
  
  TRUE
}

# Process the file in chunks
read_csv_chunked(filepath,
                 callback = callback,
                 chunk_size = 1e6)

# Final summary
summary_by_year <- final_results %>%
  group_by(Plan_year) %>%
  summarise(n = n()) %>%
  arrange(Plan_year)

print("Final summary by year:")
print(summary_by_year)

# Unique companys/deductible type by Year
## Write table
write_csv(final_results, "C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/unique_comps.csv", )

## B. Raw Claim by Year----
unique_comps <- read_csv("C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/unique_comps.csv",
                         col_names = TRUE)

### Pull claims from unique comp keys----
# Pull in claims, calculate the unique Number of Subscr ids, and filter
