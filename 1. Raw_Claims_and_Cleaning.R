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

start.time <- Sys.time()
# Initialize variables for tracking
filtered_chunks <- NULL
chunk_counter <- 0
total_rows_processed <- 0
embedded_count <- 0
aggregate_count <- 0

# Callback function to process chunks
process_chunk <- function(x, pos) {
  chunk_counter <<- chunk_counter + 1
  total_rows_processed <<- total_rows_processed + nrow(x)
  
  # Filter the chunk
  filtered_chunk <- x %>%
    filter(deductible_types %in% c("Embedded Only", "Aggregate Only"))
  
  # Update counts
  embedded_count <<- embedded_count + sum(filtered_chunk$deductible_types == "Embedded Only")
  aggregate_count <<- aggregate_count + sum(filtered_chunk$deductible_types == "Aggregate Only")
  
  # Store filtered chunk
  if(is.null(filtered_chunks)) {
    filtered_chunks <<- filtered_chunk
  } else {
    filtered_chunks <<- bind_rows(filtered_chunks, filtered_chunk)
  }
  
  # Print progress
  print(paste("Processed chunk", chunk_counter, 
              "- Rows in chunk:", nrow(x),
              "- Total rows processed:", total_rows_processed))
  print(paste("Current counts - Embedded:", embedded_count, 
              "Aggregate:", aggregate_count))
  
  TRUE
}

# Process the file in chunks
read_csv_chunked(filepath,
                 callback = process_chunk,
                 chunk_size = 1e6)

# Print final summary
print("\nFinal counts:")
print(paste("Total Embedded Only:", embedded_count))
print(paste("Total Aggregate Only:", aggregate_count))

end.time <- Sys.time()
end.time-start.time
beep(8)

write_csv(filtered_chunks, "C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/Dissert_data/embed_deduc.csv")
beep(8)

### Continuation----
start.time <- Sys.time()

# Initialize variables with previous counts
chunk_counter <- 70  # Starting from chunk 70
total_rows_processed <- 70000000  # 70 million rows processed
embedded_count <- 62263916  # Previous embedded count
aggregate_count <- 536842  # Previous aggregate count
filtered_chunks <- NULL

# Callback function to process chunks
process_chunk <- function(x, pos) {
  chunk_counter <<- chunk_counter + 1
  total_rows_processed <<- total_rows_processed + nrow(x)
  
  # Filter the chunk
  filtered_chunk <- x %>%
    filter(deductible_types %in% c("Embedded Only", "Aggregate Only"))
  
  # Update counts
  embedded_count <<- embedded_count + sum(filtered_chunk$deductible_types == "Embedded Only")
  aggregate_count <<- aggregate_count + sum(filtered_chunk$deductible_types == "Aggregate Only")
  
  # Save filtered chunk to disk immediately to prevent memory issues
  if(nrow(filtered_chunk) > 0) {
    write_csv(filtered_chunk, 
              "filtered_results_continuation.csv", 
              append = TRUE)
  }
  
  # Print progress
  print(paste("Processed chunk", chunk_counter, 
              "- Rows in chunk:", nrow(x),
              "- Total rows processed:", total_rows_processed))
  print(paste("Current counts - Embedded:", embedded_count, 
              "Aggregate:", aggregate_count))
  
  TRUE
}

# Process the remaining rows
read_csv_chunked(filepath,
                 callback = process_chunk,
                 chunk_size = 1e6,
                 skip = total_rows_processed)  # Skip previously processed rows

# Print final summary
print("\nFinal counts:")
print(paste("Total Embedded Only:", embedded_count))
print(paste("Total Aggregate Only:", aggregate_count))

end.time <- Sys.time()
print(end.time - start.time)
beep(8)

#### Combine with previous results----

previous_results <- read_csv("previous_filtered_results.csv")
continuation_results <- read_csv("filtered_results_continuation.csv")
final_results <- bind_rows(previous_results, continuation_results)
write_csv(final_results, "complete_filtered_results.csv")

# 2. Clean Data----
filepath <- "C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/Dissert_data/"

#data
embed_deduc_raw <- read_csv("embed_deduc.csv")

## A. Charlson Comorbidities----
library(comorbidity)

# Calculate CCI scores
# Group data by both MVDID and Plan_year
cci_scores <- embed_deduc_raw %>%
  group_by(MVDID, Plan_year) %>%
  group_split() %>%
  lapply(function(person_year) {
    cci <- comorbidity(
      x = person_year,
      id = "MVDID",
      code = "CODEVALUE",
      map = "icd10_charlson",
      assign0 = TRUE
    ) %>%
      mutate(Plan_year = unique(person_year$Plan_year))
    return(cci)
  }) %>%
  bind_rows()

# Join back to original data
embed_deduc_cci <- embed_deduc_raw %>%
  left_join(cci_scores, by = c("MVDID" = "id", "Plan_year"))
rm(embed_deduc_raw)
rm(cci_scores)

## B. Deductible Switches----
### Firm----
company_patterns <- filtered_data %>%
  distinct(COMPANY_CD, Plan_year, deductible_types) %>%
  group_by(COMPANY_CD) %>%
  summarize(
    years_present = n_distinct(Plan_year),
    min_year = min(Plan_year),
    max_year = max(Plan_year),
    # Create pattern with years
    deductible_history = paste(paste(Plan_year, deductible_types), collapse = " -> "),
    # Identify if stayed same or switched
    status = case_when(
      years_present == 1 ~ "Single year only",
      n_distinct(deductible_types) == 1 ~ paste("Stayed", first(deductible_types)),
      TRUE ~ "Switched"
    )
  ) %>%
  # For switchers, identify the specific transition
  mutate(
    switch_details = case_when(
      status == "Switched" ~ {
        types <- str_extract_all(deductible_history, "Aggregate Only|Embedded Only")[[1]]
        years <- str_extract_all(deductible_history, "\\d{4}")[[1]]
        paste("Switched from", types[1], "in", years[1],
              "to", types[length(types)], "in", years[length(years)])
      },
      TRUE ~ status
    )
  )

# Summary statistics
summary_stats <- company_patterns %>%
  group_by(status) %>%
  summarize(
    count = n(),
    pct = round(100 * n() / nrow(company_patterns), 1)
  )

# Detailed switch patterns
switch_patterns <- company_patterns %>%
  filter(status == "Switched") %>%
  group_by(switch_details) %>%
  summarize(
    count = n(),
    pct = round(100 * n() / sum(status == "Switched"), 1)
  )

print("Overall Summary:")
print(summary_stats)

print("\nDetailed Switch Patterns:")
print(switch_patterns)

### Individual----
subscriber_patterns <- filtered_data %>%
  distinct(SUBSCRIBER_ID, Plan_year, deductible_types) %>%
  group_by(SUBSCRIBER_ID) %>%
  summarize(
    years_present = n_distinct(Plan_year),
    min_year = min(Plan_year),
    max_year = max(Plan_year),
    deductible_history = paste(paste(Plan_year, deductible_types), collapse = " -> "),
    # Identify type for stayers
    consistent_type = if(n_distinct(deductible_types) == 1) first(deductible_types) else NA,
    status = case_when(
      years_present == 1 ~ paste("Single year:", first(deductible_types)),
      n_distinct(deductible_types) == 1 ~ paste("Stayed on", first(deductible_types), 
                                                "for", years_present, "years", 
                                                "(", min_year, "to", max_year, ")"),
      TRUE ~ "Switched"
    )
  ) %>%
  # For switchers, identify the specific transition
  mutate(
    switch_details = case_when(
      status == "Switched" ~ {
        types <- str_extract_all(deductible_history, "Aggregate Only|Embedded Only")[[1]]
        years <- str_extract_all(deductible_history, "\\d{4}")[[1]]
        paste("Switched from", types[1], "in", years[1],
              "to", types[length(types)], "in", years[length(years)])
      },
      TRUE ~ status
    )
  )

# Summary statistics with detailed stay patterns
summary_stats_subscriber <- subscriber_patterns %>%
  group_by(status) %>%
  summarize(
    count = n(),
    pct = round(100 * n() / nrow(subscriber_patterns), 1)
  )

# Separate summaries for different categories
single_year_summary <- subscriber_patterns %>%
  filter(str_detect(status, "Single year")) %>%
  group_by(status) %>%
  summarize(
    count = n(),
    pct = round(100 * n() / nrow(subscriber_patterns), 1)
  )

stayers_summary <- subscriber_patterns %>%
  filter(str_detect(status, "Stayed")) %>%
  group_by(status) %>%
  summarize(
    count = n(),
    pct = round(100 * n() / nrow(subscriber_patterns), 1)
  )

switchers_summary <- subscriber_patterns %>%
  filter(str_detect(status, "Switched")) %>%
  group_by(switch_details) %>%
  summarize(
    count = n(),
    pct = round(100 * n() / nrow(subscriber_patterns), 1)
  )

print("Single Year Subscribers:")
print(single_year_summary)

print("\nSubscribers Who Stayed on Same Type:")
print(stayers_summary)

print("\nSubscribers Who Switched:")
print(switchers_summary)
