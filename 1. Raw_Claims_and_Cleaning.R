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

# Callback function to process chunks
process_chunk <- function(x, pos) {
  chunk_counter <<- chunk_counter + 1
  total_rows_processed <<- total_rows_processed + nrow(x)
  
  # Filter the chunk with correct column name
  filtered_chunk <- x %>%
    filter(`deductible_types` %in% c("Embedded Only", "Aggregate Only"))
  
  # Update counts
  embedded_count <<- embedded_count + sum(filtered_chunk$deductible_types == "Embedded Only")
  aggregate_count <<- aggregate_count + sum(filtered_chunk$deductible_types == "Aggregate Only")
  
  # Append filtered chunk directly to the existing embed_deduc file
  if(nrow(filtered_chunk) > 0) {
    write_csv(filtered_chunk, 
              "embed_deduc.csv", 
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

# Process the remaining rows with explicit column names
read_csv_chunked(filepath,
                 callback = process_chunk,
                 chunk_size = 1e6,
                 skip = total_rows_processed,  # Skip previously processed rows
                 col_names = col_names)  # Use the exact column names from the file

# Print final summary
print("\nFinal counts:")
print(paste("Total Embedded Only:", embedded_count))
print(paste("Total Aggregate Only:", aggregate_count))

end.time <- Sys.time()
end.time - start.time
beep(8)


<<<<<<< HEAD
## 2. Identify Cohorts----
### A. Individual----
#data
# Filepath
filepath <- "R:/GraduateStudents/WatsonWilliamP/Deductible_Project/Deductible_Project/Data/"
start.time <- Sys.time()

# Define column names and types
col_names <- names(read_csv(paste0(filepath, "final.csv"), n_max = 1))
col_types <- cols(
  MVDID = col_character(),
  Plan_year = col_double(),
  .default = col_guess()
)

# Create temporary file for MVDID-year combinations
write_csv(data.frame(MVDID = character(), Plan_year = numeric()), 
          paste0(filepath, "temp_mvdid_years.csv"))

# Process chunks to get MVDID-year combinations
process_chunk <- function(x, pos) {
  chunk_mvdid_years <- x %>%
    mutate(Plan_year = as.numeric(Plan_year)) %>%
    select(MVDID, Plan_year) %>%
    distinct()
  
  write_csv(chunk_mvdid_years, 
            paste0(filepath, "temp_mvdid_years.csv"), 
            append = TRUE)
  
  print(paste("Processed chunk", pos))
  TRUE
}

# Read and process file in chunks
read_csv_chunked(paste0(filepath, "final.csv"),
                 callback = process_chunk,
                 chunk_size = 1e6,
                 col_names = col_names,
                 col_types = col_types)

# Process temporary file to identify MVDIDs present for at least 2 years
mvdid_counts <- read_csv(paste0(filepath, "temp_mvdid_years.csv")) %>%
  filter(!is.na(MVDID)) %>%
  mutate(Plan_year = as.numeric(Plan_year)) %>%
  distinct() %>%
  group_by(MVDID) %>%
  summarise(
    year_count = n_distinct(Plan_year),
    years = list(sort(unique(Plan_year)))
  ) %>%
  filter(year_count >= 2) %>%
  mutate(
    has_consecutive = map_lgl(years, function(x) any(diff(x) == 1))
  ) %>%
  filter(has_consecutive)

# Create empty data frame with correct column names
empty_df <- as.data.frame(matrix(ncol = length(col_names), nrow = 0))
names(empty_df) <- col_names

# Create output file with headers
write_csv(empty_df, paste0(filepath, "individual.csv"))

# Process chunks again to save filtered data
process_filtered_chunk <- function(x, pos) {
  filtered_chunk <- x %>%
    inner_join(mvdid_counts, by = "MVDID")
  
  write_csv(filtered_chunk, 
            paste0(filepath, "individual.csv"), 
            append = TRUE)
  
  print(paste("Processed filtered chunk", pos))
  TRUE
}

# Read and process file in chunks again for filtering
read_csv_chunked(paste0(filepath, "final.csv"),
                 callback = process_filtered_chunk,
                 chunk_size = 1e6,
                 col_names = col_names,
                 col_types = col_types)

# Clean up temporary file
unlink(paste0(filepath, "temp_mvdid_years.csv"))
=======
# 2. Clean Data----
filepath <- "C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/Dissert_data/"

#data
embed_deduc_raw <- read_csv("embed_deduc.csv")

## A. Identify Cohorts----
### Individual----
start.time <- Sys.time()

# Define the correct column names
col_names <- c("X1", "COMPANY_KEY", "SUBGROUPKEY", "MEMBERID", "MVDID", "SUBSCRIBERID", 
               "ZIPCODE", "PARTYKEY", "PLACEOFSERVICE", "PROCEDURECODE", "MOD1", 
               "SERVICEFROMDATE", "REVENUECODE", "BILLEDAMOUNT", "NONCOVEREDAMOUNT", 
               "ALLOWEDAMOUNT", "PAIDAMOUNT", "COBAMOUNT", "COINSURANCEAMOUNT", 
               "COPAYAMOUNT", "DEDUCTIBLEAMOUNT", "PARTYKEY.1", "MEMBERKEY", 
               "PLANGROUP", "LOB", "CLAIMNUMBER", "CODEVALUE", "CODETYPE", 
               "PATIENTDOB", "PATIENTGENDER", "NETWORKINDICATOR", "Plan_year", 
               "plan_year_start", "age_at_plan_year_start", "family_size", 
               "DEDUCTIBLE_CATEGORY", "unique_deductible_types", "deductible_types")

# Create empty temporary file with headers
write_csv(data.frame(MVDID = character(), Plan_year = numeric()), 
          "temp_mvdid_years.csv")

# Callback function to process chunks
process_chunk <- function(x, pos) {
  # Extract unique MVDID-year combinations
  chunk_mvdid_years <- x %>%
    select(MVDID, Plan_year) %>%
    distinct()
  
  # Append to storage, without column names
  write_csv(chunk_mvdid_years, 
            "temp_mvdid_years.csv", 
            append = TRUE)
  
  # Print progress
  print(paste("Processed chunk at position:", pos,
              "- Unique combinations in this chunk:", nrow(chunk_mvdid_years)))
  
  TRUE
}

# Process the file to get all MVDID-year combinations
read_csv_chunked("embed_deduc.csv",
                 callback = process_chunk,
                 chunk_size = 1e6,
                 col_names = col_names,
                 skip = 1)

# Process the temporary file to get MVDIDs present for 2+ years
mvdid_counts <- read_csv("temp_mvdid_years.csv") %>%
  filter(!is.na(MVDID)) %>%  # Remove any NA values
  distinct() %>%  # Remove any duplicates
  group_by(MVDID) %>%
  summarise(year_count = n_distinct(Plan_year)) %>%
  filter(year_count >= 2)

# Save the list of qualifying MVDIDs
write_csv(mvdid_counts, "mvdid_multiple_years.csv")

# Now filter the original data for these MVDIDs
process_qualified_chunk <- function(x, pos) {
  filtered_chunk <- x %>%
    inner_join(mvdid_counts, by = "MVDID")
  
  # Save filtered chunk
  write_csv(filtered_chunk, 
            "embed_deduc_multiple_years.csv", 
            append = TRUE)
  
  # Print progress
  print(paste("Processed qualified chunk at position:", pos,
              "- Rows in filtered chunk:", nrow(filtered_chunk)))
  
  TRUE
}

# Process original file again to save only records for qualifying MVDIDs
read_csv_chunked("embed_deduc.csv",
                 callback = process_qualified_chunk,
                 chunk_size = 1e6,
                 col_names = col_names,
                 skip = 1)

# Clean up temporary file
unlink("temp_mvdid_years.csv")
>>>>>>> 6620ce7c6b9c9e04b47441b8b42e3c83b8c3ddec

end.time <- Sys.time()
print(paste("Total processing time:", end.time - start.time))

# Print summary
qualified_mvids <- nrow(mvdid_counts)
<<<<<<< HEAD
print(paste("Number of MVDIDs present in 2+ consecutive years:", qualified_mvids))
beep(8)


### B. Deductible Switches----
filepath <- "R:/GraduateStudents/WatsonWilliamP/Deductible_Project/Deductible_Project/Data/"

library(readr)
library(data.table)

start.time <- Sys.time()

# Initialize global counters
initial_claims <- 0
initial_mvids <- character(0)
initial_subs <- character(0)

# Create temporary file for subscriber patterns
temp_patterns <- data.frame(
  MVDID = character(),
  SUBSCRIBERID = character(),
  Plan_year = numeric(),
  deductible_types = character(),
  claim_count = numeric()
)
write_csv(temp_patterns, paste0(filepath, "temp_patterns.csv"))

# Define column types
col_types <- cols(
  MVDID = col_character(),
  SUBSCRIBERID = col_character(),
  Plan_year = col_double(),
  deductible_types = col_character(),
  PAIDAMOUNT = col_double(),
  .default = col_guess()
)

# Process chunks to get unique subscriber patterns
process_chunk <- function(x, pos) {
  initial_claims <<- initial_claims + nrow(x)
  initial_mvids <<- union(initial_mvids, unique(x$MVDID))
  initial_subs <<- union(initial_subs, unique(x$SUBSCRIBERID))
  
  chunk_patterns <- x %>%
    mutate(Plan_year = as.numeric(Plan_year)) %>%
    filter(deductible_types %in% c("Aggregate Only", "Embedded Only")) %>%
    group_by(MVDID, SUBSCRIBERID, Plan_year, deductible_types) %>%
    summarize(
      claim_count = n(),
      .groups = 'drop'
    )
  
  write_csv(chunk_patterns, paste0(filepath, "temp_patterns.csv"), append = TRUE)
  
  print(paste("Processed chunk", pos))
  TRUE
}

read_csv_chunked(paste0(filepath, "individual.csv"),
                 callback = process_chunk,
                 chunk_size = 1e6,
                 col_types = col_types)

# Process patterns
subscriber_patterns <- read_csv(paste0(filepath, "temp_patterns.csv")) %>%
  mutate(Plan_year = as.numeric(Plan_year)) %>%
  group_by(MVDID, SUBSCRIBERID, Plan_year) %>%
  summarize(
    claim_count = sum(claim_count),
    deductible_types = first(deductible_types),
    .groups = 'drop'
  ) %>%
  # First verify each MVDID has claims in consecutive years
  group_by(MVDID) %>%
  arrange(Plan_year) %>%
  mutate(
    next_year = lead(Plan_year),
    has_next_year = !is.na(next_year) & next_year == Plan_year + 1
  ) %>%
  filter(has_next_year | lag(has_next_year)) %>%
  # Then look for switches
  group_by(SUBSCRIBERID) %>%
  arrange(Plan_year) %>%
  mutate(
    next_year_type = lead(deductible_types),
    next_year = lead(Plan_year),
    is_switch_pair = (
      # Either Aggregate to Embedded
      (deductible_types == "Aggregate Only" & 
         next_year_type == "Embedded Only" &
         next_year == Plan_year + 1) |
        # Or Embedded to Aggregate
        (deductible_types == "Embedded Only" & 
           next_year_type == "Aggregate Only" &
           next_year == Plan_year + 1)
    )
  ) %>%
  filter(is_switch_pair | lead(is_switch_pair)) %>%
  summarize(
    MVDIDs = list(unique(MVDID)),
    n_mvdids = n_distinct(MVDID),
    total_claims = sum(claim_count),
    switch_year = min(Plan_year[is_switch_pair == TRUE]) + 1,
    pre_switch_year = min(Plan_year[is_switch_pair == TRUE]),
    switch_from = first(deductible_types[is_switch_pair == TRUE]),
    switch_to = first(next_year_type[is_switch_pair == TRUE]),
    .groups = 'drop'
  ) %>%
  mutate(
    status = "Switched",
    switch_details = paste("Switched from", switch_from, "in", pre_switch_year,
                           "to", switch_to, "in", switch_year)
  )

# Generate summary statistics
print("\nSwitch Direction Summary:")
switch_summary <- subscriber_patterns %>%
  group_by(switch_from, switch_to) %>%
  summarise(
    n_subscribers = n(),
    n_mvids = sum(n_mvdids),
    total_claims = sum(total_claims),
    .groups = 'drop'
  )
print(switch_summary)
end.time <- Sys.time()
end.time-start.time
beep(8)

### C. Claims----
library(lubridate)
library(tidyverse)
library(beepr)

# Define column types
col_types <- cols(
  MVDID = col_character(),
  SUBSCRIBERID = col_character(),
  Plan_year = col_double(),
  PAIDAMOUNT = col_double(),
  .default = col_guess()
)

process_chunk <- function(chunk, switcher_mvids, switch_info) {
  # Process actual claims data
  processed <- chunk %>%
    mutate(Plan_year = as.numeric(Plan_year)) %>%
    filter(MVDID %in% switcher_mvids) %>%
    left_join(switch_info, by = c("MVDID", "SUBSCRIBERID")) %>%
    mutate(
      period = case_when(
        Plan_year == pre_switch_year ~ "Pre-Switch",
        Plan_year == switch_year ~ "Switch Year",
        TRUE ~ "Other"
      ),
      period_type = case_when(
        period == "Pre-Switch" ~ "pre_switch_year",
        period == "Switch Year" ~ "switch_year"
      ),
      switch_type = paste(switch_from, "to", switch_to)
    ) %>%
    select(
      MVDID, SUBSCRIBERID, period_type, Plan_year, period,
      switch_details, pre_switch_year, switch_year, 
      switch_from, switch_to, PAIDAMOUNT, switch_type,
      everything()
    ) %>%
    filter(period != "Other")
  
  return(processed)
}

get_switcher_claims_chunked <- function(filepath, switcher_mvids, subscriber_patterns, chunk_size = 1e6) {
  switch_info <- subscriber_patterns %>%
    select(SUBSCRIBERID, switch_details, pre_switch_year, switch_year, 
           switch_from, switch_to, MVDIDs) %>%
    unnest(cols = c(MVDIDs)) %>%
    rename(MVDID = MVDIDs)
  
  claims_analyzed <- tibble()
  
  callback <- function(chunk, pos) {
    processed_chunk <- process_chunk(chunk, switcher_mvids, switch_info)
    
    if(nrow(processed_chunk) > 0) {
      claims_analyzed <<- bind_rows(claims_analyzed, processed_chunk)
    }
    print(paste("Processed chunk", pos))
    TRUE
  }
  
  read_csv_chunked(
    file = paste0(filepath, "individual.csv"),
    callback = callback,
    chunk_size = chunk_size,
    col_types = col_types
  )
  
  return(claims_analyzed)
}

# Execute the claims processing
start.time <- Sys.time()

switcher_mvids <- subscriber_patterns %>%
  unnest(cols = c(MVDIDs)) %>%
  pull(MVDIDs) %>%
  unique()

print("Number of switcher MVDIDs:")
print(length(switcher_mvids))

# Get processed claims data
claims_data <- get_switcher_claims_chunked(filepath, switcher_mvids, subscriber_patterns)

print(paste("Execution time:", Sys.time() - start.time))
beep(8)

### D. Elixhauser Comorbidities----
library(comorbidity)

library(comorbidity)

# 1. Calculate Elixhauser scores by MVDID and Plan_year
elix <- claims_data %>% 
  filter(!is.na(CODEVALUE)) %>%
  select(MVDID, CODEVALUE, Plan_year) %>%
  distinct() %>%
  split(.$Plan_year) %>%
  lapply(function(year_data) {
    comorbidity(
      x = year_data %>% select(MVDID, CODEVALUE),
      id = "MVDID",
      code = "CODEVALUE",
      map = "elixhauser_icd10_quan",
      assign0 = FALSE
    ) %>%
      mutate(Plan_year = unique(year_data$Plan_year))
  }) %>%
  bind_rows()

# 2. Calculate total comorbidities by year
elix_scores <- elix %>%
  mutate(
    tot_comorbidities = rowSums(select(., -MVDID, -Plan_year))
  ) %>%
  select(MVDID, Plan_year, tot_comorbidities)

# 3. Add scores to claims data and create bins
breaks <- c(0, 1, 2, 3, Inf)
labels <- c("0", "1", "2", "3+")

claims_with_elix <- claims_data %>%
  left_join(elix_scores, by = c("MVDID", "Plan_year")) %>%
  mutate(
    tot_comorbidities = coalesce(tot_comorbidities, 0),
    comorbid_bins = cut(tot_comorbidities, 
                        breaks = breaks, 
                        labels = labels, 
                        include.lowest = TRUE,
                        right = FALSE)
  )

# 4. Summary statistics
yearly_summary <- claims_with_elix %>%
  group_by(Plan_year) %>%
  summarise(
    min_comorbidities = min(tot_comorbidities),
    max_comorbidities = max(tot_comorbidities),
    avg_comorbidities = mean(tot_comorbidities),
    n_members = n_distinct(MVDID),
    .groups = "drop"
  )

bin_summary <- claims_with_elix %>%
  group_by(Plan_year, comorbid_bins) %>%
  summarise(
    n_members = n_distinct(MVDID),
    pct_members = n_distinct(MVDID) / n_distinct(claims_with_elix$MVDID) * 100,
    .groups = "drop"
  )

# 5. Period comparisons
period_summary <- claims_with_elix %>%
  group_by(period) %>%
  summarise(
    avg_comorbidities = mean(tot_comorbidities),
    n_members = n_distinct(MVDID),
    .groups = "drop"
  )

print("Yearly Comorbidity Summary:")
print(yearly_summary)
print("\nComorbidity Bin Distribution:")
print(bin_summary)
print("\nPeriod Summary:")
print(period_summary)

### E. Age Bins----
breaks <- c(0, 4, 12, 17, 34, 49, 64)
labels <- c("0-4", "5-12", "13-17", "18-34", "35-49", "50-64")

claims_with_elix_age <- claims_with_elix %>%
  mutate(age_group = cut(age_at_plan_year_start, 
                         breaks = breaks, 
                         labels = labels, 
                         include.lowest = TRUE, 
                         right = FALSE))

### F. Family Size----
#Calculate family size and create bins
claims_elix_age_fam <- claims_with_elix_age %>%
  group_by(SUBSCRIBERID, Plan_year) %>%
  mutate(new_fam_size = n_distinct(MVDID)) %>%
  ungroup() %>%
  mutate(family_size_bins = cut(family_size,
                                breaks = c(0, 2, 3, 4, 5, Inf),
                                labels = c("2", "3", "4", "5", "6+"),
                                include.lowest = TRUE,
                                right = FALSE))


save(claims_elix_age_fam, file = paste0(filepath, "claims_elix_age_fam.RData"))
load(paste0(filepath, "claims_elix_age_fam.RData"))

### G. Benefit Information----


## B. Firm Cohort----
### Firms----


### Deductible Switch----


### Claims----
=======
print(paste("Number of MVDIDs present in 2+ years:", qualified_mvids))
beep(8)

### Firm----

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

>>>>>>> 6620ce7c6b9c9e04b47441b8b42e3c83b8c3ddec

## C. Charlson Comorbidities----
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

<<<<<<< HEAD


=======
>>>>>>> 6620ce7c6b9c9e04b47441b8b42e3c83b8c3ddec
