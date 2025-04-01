# 1. Individual Raw Claims ----
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

## B. Individual Raw Claim by Year----
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


# 2. Identify Cohorts----
## A. Individual----
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

end.time <- Sys.time()
print(paste("Total processing time:", end.time - start.time))

# Print summary
qualified_mvids <- nrow(mvdid_counts)
print(paste("Number of MVDIDs present in 2+ consecutive years:", qualified_mvids))
beep(8)

#### A1. Aggregate Only People----

final_one <- read_csv("final.csv", n_max = 50)
individual_one <- read_csv("individual.csv", n_max = 50)

## B. Deductible Switches----
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
  COMPANY_KEY = double(),
  Plan_year = numeric(),
  deductible_types = character(),
  claim_count = numeric()
)
write_csv(temp_patterns, paste0(filepath, "temp_patterns.csv"))

# Define column types
col_types <- cols(
  MVDID = col_character(),
  SUBSCRIBERID = col_character(),
  COMPANY_KEY = col_double(),
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
    mutate(
      Plan_year = as.numeric(Plan_year),
      COMPANY_KEY = as.numeric(COMPANY_KEY)
    ) %>%
    filter(deductible_types %in% c("Aggregate Only", "Embedded Only")) %>%
    group_by(MVDID, SUBSCRIBERID, COMPANY_KEY, Plan_year, deductible_types) %>%
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

# Identify Aggregate-only MVDIDs/Companies that never switch
aggregate_only_control <- read_csv(paste0(filepath, "temp_patterns.csv"), 
                                   col_types = cols(
                                     MVDID = col_character(),
                                     SUBSCRIBERID = col_character(),
                                     COMPANY_KEY = col_double(),
                                     Plan_year = col_double(),
                                     deductible_types = col_character(),
                                     claim_count = col_double()
                                   )) %>%
  mutate(Plan_year = as.numeric(Plan_year)) %>%
  group_by(MVDID, SUBSCRIBERID, COMPANY_KEY) %>%
  summarize(
    n_years = n_distinct(Plan_year),
    all_types = list(unique(deductible_types)),
    .groups = 'drop'
  ) %>%
  filter(
    n_years >= 2,  # Must have at least 2 years of data
    map_lgl(all_types, function(x) length(x) == 1 && x == "Aggregate Only")  # Only ever Aggregate
  )

# Save the control group MVDIDs
write_csv(aggregate_only_control, paste0(filepath, "aggregate_only_control.csv"))

# Process patterns for switchers
subscriber_patterns <- read_csv(paste0(filepath, "temp_patterns.csv"),
                                col_types = cols(
                                  MVDID = col_character(),
                                  SUBSCRIBERID = col_character(),
                                  COMPANY_KEY = col_double(),
                                  Plan_year = col_double(),
                                  deductible_types = col_character(),
                                  claim_count = col_double()
                                )) %>%
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

# Process claims for control group
process_control_chunk <- function(chunk, control_info) {
  processed <- chunk %>%
    mutate(COMPANY_KEY = as.numeric(COMPANY_KEY)) %>%
    inner_join(control_info, by = c("MVDID", "SUBSCRIBERID", "COMPANY_KEY")) %>%
    mutate(
      control_group = TRUE,
      period = "Control",
      period_type = "control"
    )
  
  return(processed)
}

# Get and save control group claims
get_control_claims_chunked <- function(filepath, control_info, chunk_size = 1e6) {
  claims_analyzed <- tibble()
  
  callback <- function(chunk, pos) {
    processed_chunk <- process_control_chunk(chunk, control_info)
    
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

# Get and save control group claims
control_claims <- get_control_claims_chunked(
  filepath, 
  aggregate_only_control, 
  chunk_size = 1e6
)

write_csv(control_claims, paste0(filepath, "control_claims.csv"))

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

# Print control group summary
print("\nControl Group Summary:")
control_summary <- aggregate_only_control %>%
  summarise(
    n_subscribers = n_distinct(SUBSCRIBERID),
    n_mvids = n_distinct(MVDID),
    avg_mvids_per_subscriber = n_distinct(MVDID) / n_distinct(SUBSCRIBERID),
    avg_years = mean(n_years),
    min_years = min(n_years),
    max_years = max(n_years)
  )
print(control_summary)

end.time <- Sys.time()
print(paste("Total processing time:", end.time - start.time))
beep(8)


## C. Treatment/Claims----
library(lubridate)
library(data.table)
library(readr)
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
  # Convert chunk to data.table and process
  setDT(chunk)
  
  # Filter and join operations
  chunk[, Plan_year := as.numeric(Plan_year)]    # Fixed := syntax
  chunk <- chunk[MVDID %in% switcher_mvids]
  processed <- switch_info[chunk, on = .(MVDID, SUBSCRIBERID)]
  
  # Add period columns
  processed[, `:=`(
    period = fifelse(Plan_year == pre_switch_year, "Pre-Switch",
                     fifelse(Plan_year == switch_year, "Switch Year", "Other")),
    period_type = fifelse(Plan_year == pre_switch_year, "pre_switch_year",
                          fifelse(Plan_year == switch_year, "switch_year", NA_character_)),
    switch_type = paste(switch_from, "to", switch_to)
  )]
  
  # Filter out "Other" periods
  processed <- processed[period != "Other"]
  
  # Reorder columns
  setcolorder(processed, c("MVDID", "SUBSCRIBERID", "period_type", "Plan_year", "period",
                           "switch_details", "pre_switch_year", "switch_year", 
                           "switch_from", "switch_to", "PAIDAMOUNT", "switch_type"))
  
  return(processed)
}

get_switcher_claims_chunked <- function(filepath, switcher_mvids, subscriber_patterns, chunk_size = 1e6) {
  # Convert subscriber_patterns to data.table and prepare switch_info
  setDT(subscriber_patterns)
  switch_info <- subscriber_patterns[, .(SUBSCRIBERID, switch_details, pre_switch_year, 
                                         switch_year, switch_from, switch_to, MVDIDs)]
  switch_info <- switch_info[, .(MVDID = unlist(MVDIDs)), 
                             by = .(SUBSCRIBERID, switch_details, pre_switch_year, 
                                    switch_year, switch_from, switch_to)]
  
  # Set keys for faster joins
  setkey(switch_info, MVDID, SUBSCRIBERID)
  
  claims_analyzed <- data.table()
  
  callback <- function(chunk, pos) {
    processed_chunk <- process_chunk(chunk, switcher_mvids, switch_info)
    
    if(nrow(processed_chunk) > 0) {
      claims_analyzed <<- rbindlist(list(claims_analyzed, processed_chunk), fill = TRUE)
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

# Get unique switcher MVDIDs
switcher_mvids <- unique(unlist(subscriber_patterns$MVDIDs))

print("Number of switcher MVDIDs:")
print(length(switcher_mvids))

# Get processed claims data
claims_data <- get_switcher_claims_chunked(filepath, switcher_mvids, subscriber_patterns)

print(paste("Execution time:", Sys.time() - start.time))
beep(8)

#### 1. Elixhauser Comorbidities----
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

#### 2. Age Bins----
# Remove negative values
claims_with_elix <- claims_with_elix %>%
  filter(age_at_plan_year_start >= 0)  # Remove impossible negative ages

breaks <- c(0, 4, 12, 17, 34, 49, 64, Inf)
labels <- c("0-4", "5-12", "13-17", "18-34", "35-49", "50-64", "65+")

claims_with_elix_age <- claims_with_elix %>%
  mutate(age_group = cut(age_at_plan_year_start, 
                         breaks = breaks, 
                         labels = labels, 
                         include.lowest = TRUE, 
                         right = FALSE))

#### 3. Family Size----
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
claims_elix_age_fam <- load(paste0(filepath, "claims_elix_age_fam.RData"))

load("claims_elix_age_fam.RData")
treat_claims <- claims_elix_age_fam
rm(claims_elix_age_fam)

#### 4. Benefit Information----
##### 4.a.1 Raw Data-----
amiba_raw <- read_csv("AMIBA_Ben_Pckgs.csv")
amiha_raw <- read_csv("AMIHA_Ben_Pckgs.csv")

# Clean amiba dataset
amiba_cleaned <- amiba_raw %>%
  select(
    MVDID,
    MEMBERID,
    GROUP_NBR,
    BENEFIT_PKG = BENEFIT_PKG...4,
    EFF_DATE,
    TERM_DATE,
    VOID,
    DEDUCTIBLE,
    CP_AMT_T1,
    CP_AMT_T2,
    CP_AMT_T3
  ) %>%
  mutate(
    EFF_DATE = as.Date(EFF_DATE),
    TERM_DATE = as.Date(TERM_DATE),
    Year = year(EFF_DATE)
  ) %>%
  filter(
    Year >= 2016,
    Year <= 2023,
    is.na(VOID) | VOID == 0
  )

# Clean amiha dataset
amiha_cleaned <- amiha_raw %>%
  select(
    MVDID,
    MEMBERID,
    GROUP_NBR = GROUP_NBR...8,
    BENEFIT_PKG = BENEFIT_PKG...4,
    EFF_DATE,
    TERM_DATE,
    VOID,
    DEDUCTIBLE,
    CP_AMT_T1,
    CP_AMT_T2,
    CP_AMT_T3
  ) %>%
  mutate(
    EFF_DATE = as.Date(EFF_DATE),
    TERM_DATE = as.Date(TERM_DATE),
    Year = year(EFF_DATE)
  ) %>%
  filter(
    Year >= 2016,
    Year <= 2023,
    is.na(VOID) | VOID == 0
  )

# Analyze each dataset separately with added cost fields
amiba_summary <- amiba_cleaned %>%
  group_by(Year) %>%
  summarise(
    n_records = n(),
    n_mvdids = n_distinct(MVDID),
    n_groups = n_distinct(GROUP_NBR),
    avg_duration = mean(as.numeric(TERM_DATE - EFF_DATE), na.rm = TRUE),
    med_deductible = median(as.numeric(DEDUCTIBLE), na.rm = TRUE),
    med_cp1 = median(as.numeric(CP_AMT_T1), na.rm = TRUE),
    med_cp2 = median(as.numeric(CP_AMT_T2), na.rm = TRUE),
    med_cp3 = median(as.numeric(CP_AMT_T3), na.rm = TRUE)
  )

amiha_summary <- amiha_cleaned %>%
  group_by(Year) %>%
  summarise(
    n_records = n(),
    n_mvdids = n_distinct(MVDID),
    n_groups = n_distinct(GROUP_NBR),
    avg_duration = mean(as.numeric(TERM_DATE - EFF_DATE), na.rm = TRUE),
    med_deductible = median(as.numeric(DEDUCTIBLE), na.rm = TRUE),
    med_cp1 = median(as.numeric(CP_AMT_T1), na.rm = TRUE),
    med_cp2 = median(as.numeric(CP_AMT_T2), na.rm = TRUE),
    med_cp3 = median(as.numeric(CP_AMT_T3), na.rm = TRUE)
  )

print("AMIBA Summary:")
print(amiba_summary)
print("\nAMIHA Summary:")
print(amiha_summary)

# Rest of overlap analysis remains the same
overlap_analysis <- inner_join(
  amiba_cleaned %>% distinct(MVDID) %>% mutate(in_amiba = TRUE),
  amiha_cleaned %>% distinct(MVDID) %>% mutate(in_amiha = TRUE),
  by = "MVDID"
)

print("\nNumber of MVDIDs appearing in both datasets:")
print(nrow(overlap_analysis))

##### 4.a.2 Clean effective dates----
# Clean the benefits data with adjusted TERM_DATEs
benefits_cleaned <- bind_rows(
  amiba_cleaned %>% 
    mutate(
      dataset = "amiba",
      TERM_DATE = if_else(TERM_DATE == as.Date("9999-12-31"),
                          EFF_DATE + years(1),
                          TERM_DATE)
    ),
  amiha_cleaned %>% 
    mutate(
      dataset = "amiha",
      TERM_DATE = if_else(TERM_DATE == as.Date("9999-12-31"),
                          EFF_DATE + years(1),
                          TERM_DATE)
    )
) %>%
  mutate(
    term_year = year(TERM_DATE),
    duration_days = as.numeric(TERM_DATE - EFF_DATE)
  )

# Summary after cleaning with cost fields
cleaned_summary <- benefits_cleaned %>%
  group_by(dataset, Year) %>%
  summarise(
    n_records = n(),
    n_mvdids = n_distinct(MVDID),
    avg_duration = mean(duration_days),
    med_duration = median(duration_days),
    med_deductible = median(as.numeric(DEDUCTIBLE), na.rm = TRUE),
    med_cp1 = median(as.numeric(CP_AMT_T1), na.rm = TRUE),
    med_cp2 = median(as.numeric(CP_AMT_T2), na.rm = TRUE),
    med_cp3 = median(as.numeric(CP_AMT_T3), na.rm = TRUE)
  )

print("Summary after cleaning:")
print(cleaned_summary)

# Clean benefits data with additional date fixes
benefits_cleaned_v2 <- bind_rows(
  amiba_cleaned %>% 
    mutate(
      dataset = "amiba",
      TERM_DATE = if_else(year(TERM_DATE) > 2030,
                          EFF_DATE + years(1),
                          TERM_DATE)
    ),
  amiha_cleaned %>% 
    mutate(
      dataset = "amiha",
      TERM_DATE = if_else(year(TERM_DATE) > 2030,
                          EFF_DATE + years(1),
                          TERM_DATE)
    )
) %>%
  mutate(
    term_year = year(TERM_DATE),
    duration_days = as.numeric(TERM_DATE - EFF_DATE)
  )

# Verify the cleaning worked, including cost fields
verification <- benefits_cleaned_v2 %>%
  group_by(dataset, Year) %>%
  summarise(
    max_term_year = max(year(TERM_DATE)),
    max_duration = max(duration_days),
    avg_duration = mean(duration_days),
    med_duration = median(duration_days),
    med_deductible = median(as.numeric(DEDUCTIBLE), na.rm = TRUE),
    med_cp1 = median(as.numeric(CP_AMT_T1), na.rm = TRUE),
    med_cp2 = median(as.numeric(CP_AMT_T2), na.rm = TRUE),
    med_cp3 = median(as.numeric(CP_AMT_T3), na.rm = TRUE)
  )

print("Verification after additional cleaning:")
print(verification)
beep(8)

##### 4.a.3 Split Benefits packages yearly----
# First separate long and normal duration records
long_records <- benefits_cleaned_v2 %>%
  filter(duration_days > 500)

normal_records <- benefits_cleaned_v2 %>%
  filter(duration_days <= 500)

# Updated function to split a single record
split_record <- function(row) {
  dates <- seq(row$EFF_DATE, row$TERM_DATE, by="year")
  tibble(
    MVDID = row$MVDID,
    MEMBERID = row$MEMBERID,
    BENEFIT_PKG = row$BENEFIT_PKG,
    GROUP_NBR = row$GROUP_NBR,
    dataset = row$dataset,
    EFF_DATE = dates[-length(dates)],
    TERM_DATE = dates[-1],
    original_term = row$TERM_DATE,
    DEDUCTIBLE = row$DEDUCTIBLE,
    CP_AMT_T1 = row$CP_AMT_T1,
    CP_AMT_T2 = row$CP_AMT_T2,
    CP_AMT_T3 = row$CP_AMT_T3
  )
}

# Process long records
long_records_split <- long_records %>%
  split(1:nrow(.)) %>%
  map_dfr(split_record)

# Combine split records with normal records
benefits_cleaned_v3 <- bind_rows(
  normal_records,
  long_records_split
) %>%
  mutate(
    Year = year(EFF_DATE),
    term_year = year(TERM_DATE),
    duration_days = as.numeric(TERM_DATE - EFF_DATE)
  )

# Print summary of changes
print("Records processed:")
print(paste("Original total:", nrow(benefits_cleaned_v2)))
print(paste("Records split:", nrow(long_records)))
print(paste("New total:", nrow(benefits_cleaned_v3)))

# Final verification including cost fields
verification_v3 <- benefits_cleaned_v3 %>%
  group_by(dataset, Year) %>%
  summarise(
    n_records = n(),
    max_duration = max(duration_days),
    avg_duration = mean(duration_days),
    med_duration = median(duration_days),
    med_deductible = median(as.numeric(DEDUCTIBLE), na.rm = TRUE),
    med_cp1 = median(as.numeric(CP_AMT_T1), na.rm = TRUE),
    med_cp2 = median(as.numeric(CP_AMT_T2), na.rm = TRUE),
    med_cp3 = median(as.numeric(CP_AMT_T3), na.rm = TRUE)
  )

print("\nVerification after splitting long durations:")
print(verification_v3)
beep(8)

##### 4.b.1 Copays and Deduc----
# Create mapping dictionaries for each type
deductible_map <- c(
  "999999999" = 0,
  "001000000" = 10000,
  "000500000" = 5000,
  "005000000" = 5000,    # Fixed from 50000
  "000200000" = 2000,
  "000300000" = 3000,
  "000600000" = 6000,
  "000135000" = 1350,
  "000250000" = 2500,
  "000120000" = 1200,
  "000240000" = 2400,
  "000400000" = 4000,
  "000170000" = 1700,
  "000435000" = 4350,
  "000360000" = 3600,
  "000270000" = 2700,
  "000075000" = 750,
  "000040000" = 400,
  "000260000" = 2600,
  "000470000" = 4700,
  "000350000" = 3500,
  "000165000" = 1650,
  "000060000" = 600,
  "000000200" = 200,    # Fixed from 2
  "000020000" = 200,
  "000005000" = 5000,   # Fixed from 50
  "000027500" = 2750,   # Fixed from 275
  "000087500" = 8750,   # Fixed from 875
  "000002500" = 250,    # Fixed from 25
  "000001500" = 150,    # Fixed from 15
  "000150000" = 1500,
  "000100000" = 1000,
  "000375000" = 3750,
  "000010000" = 1000,   # Fixed from 100
  "000022500" = 2250,   # Fixed from 225
  "000030000" = 3000,   # Fixed from 300
  "000012500" = 1250,   # Fixed from 125
  "000017500" = 1750,   # Fixed from 175
  "000140000" = 1400,
  "000003000" = 3000,   # Fixed from 30
  "000130000" = 1300,
  "000345000" = 3450,
  "000625000" = 6250,
  "000675000" = 6750,
  "000550000" = 5500,
  "000015000" = 1500,   # Fixed from 150
  "000365000" = 3650,
  "000355000" = 3550,
  "000450000" = 4500,
  "000387500" = 3875,
  "000320000" = 3200,
  "000070000" = 700,
  "000430000" = 4300,
  "000280000" = 2800,
  "000290000" = 2900,
  "000665000" = 6650,
  "000855000" = 8550,
  "000700000" = 7000,
  "000660000" = 6600,
  "000011000" = 1100,   # Fixed from 110
  "000007500" = 750,    # Fixed from 75
  "000011500" = 1150,   # Fixed from 115
  "000750000" = 7500,
  "000685000" = 6850,
  "000125000" = 1250,
  "000162500" = 1625,
  "000180000" = 1800,
  "000690000" = 6900,
  "000640000" = 6400,
  "000062500" = 6250,
  "000800000" = 8000,
  "000147500" = 1475,
  "000715000" = 7150,
  "000190000" = 1900,
  "000705000" = 7050,
  "000025000" = 2500,   # Fixed from 250
  "000050000" = 5000,   # Fixed from 500
  "000006000" = 6000,   # Fixed from 60
  "000045000" = 4500,   # Fixed from 450
  "000275000" = 2750,
  "000272500" = 2725,
  "000066000" = 6600,   # Fixed from 660
  "000425000" = 4250,
  "000005500" = 550,    # Fixed from 55
  "000036000" = 3600,   # Fixed from 360
  "000006350" = 6350,
  "000635000" = 6350,
  "000087000" = 8700,   # Fixed from 870
  "000082500" = 8250,   # Fixed from 825
  "000085000" = 8500,   # Fixed from 850
  "000475000" = 4750,
  "000002000" = 2000,   # Fixed from 20
  "000000500" = 500,    # Fixed from 5
  "000370000" = 3700,
  "000001000" = 1000,   # Fixed from 10
  "000067500" = 6750,   # Fixed from 675
  "000035000" = 3500,   # Fixed from 350
  "000037500" = 3750,   # Fixed from 375
  "000002400" = 2400,   # Fixed from 24
  "000000250" = 250     # Fixed from 2.50
)

pcp_copay_map <- c(
  "000001000" = 10,
  "000001500" = 15,
  "000002000" = 20,
  "999999999" = 0,
  "000000000" = 0,
  "000000500" = 5,
  "000000015" = 15,
  "000001700" = 17,
  "000000020" = 20,
  "000020000" = 200,
  "000002500" = 25,
  "000004000" = 40,
  "000000010" = 10,
  "000003000" = 30,
  "000000400" = 4,
  "000000004" = 4,
  "000000700" = 7,
  "000001200" = 12,
  "000015000" = 150,
  "000000600" = 6,
  "000000470" = 4.70,
  "000000025" = 25
)

specialist_copay_map <- c(
  "000004000" = 40,
  "000003000" = 30,
  "000003500" = 35,
  "000004500" = 45,
  "000005000" = 50,
  "999999999" = 0,
  "000002500" = 25,
  "000001500" = 15,
  "000001000" = 10,
  "000002000" = 20,
  "000005500" = 55,
  "000000035" = 35,
  "000000045" = 45,
  "000000050" = 50,
  "000006000" = 60,
  "000000500" = 5,
  "000000040" = 40,
  "000006500" = 65,
  "000000030" = 30,
  "000000400" = 4,
  "000000000" = 0,
  "000000004" = 4,
  "000007500" = 75,
  "000025000" = 250,
  "000008000" = 80,
  "000000700" = 7,
  "000000470" = 4.70,
  "000000055" = 55
)

ed_copay_map <- c(
  "000006000" = 60,
  "000005000" = 50,
  "000005500" = 55,
  "000006500" = 65,
  "000007000" = 70,
  "999999999" = 0,
  "000007500" = 75,
  "000004500" = 45,
  "000003000" = 30,
  "000008000" = 80,
  "000000055" = 55,
  "000004000" = 40,
  "000000065" = 65,
  "000000070" = 70,
  "000008500" = 85,
  "000010000" = 100,
  "000012000" = 120,
  "000009000" = 90,
  "000013000" = 130,
  "000000075" = 75,
  "000000050" = 50,
  "000013500" = 135,
  "000015000" = 150,
  "000000800" = 800,   # Changed from 8 to 800
  "000000000" = 0,
  "000000008" = 8,
  "000000400" = 400,   # Changed from 4 to 400
  "000002000" = 20,
  "000016100" = 161,
  "000016000" = 160,
  "000003500" = 35,
  "000002500" = 25,
  "000001500" = 15,
  "000000060" = 60,
  "000000940" = 940,   # Changed from 9.40 to 940
  "000011000" = 110,
  "000000090" = 90
)

# The rest of the code remains the same, but let's adjust the range check for ED copays:

benefits_cleaned_v4 <- benefits_cleaned_v3 %>%
  mutate(
    DEDUCTIBLE = deductible_map[DEDUCTIBLE],
    CP_AMT_T1 = pcp_copay_map[CP_AMT_T1],
    CP_AMT_T2 = specialist_copay_map[CP_AMT_T2],
    CP_AMT_T3 = ed_copay_map[CP_AMT_T3]
  )

# Add reasonable range checks
benefits_cleaned_v4 <- benefits_cleaned_v4 %>%
  mutate(
    DEDUCTIBLE = if_else(DEDUCTIBLE < 0 | DEDUCTIBLE > 10000, NA_real_, DEDUCTIBLE),
    CP_AMT_T1 = if_else(CP_AMT_T1 < 0 | CP_AMT_T1 > 250, NA_real_, CP_AMT_T1),
    CP_AMT_T2 = if_else(CP_AMT_T2 < 0 | CP_AMT_T2 > 250, NA_real_, CP_AMT_T2),
    CP_AMT_T3 = if_else(CP_AMT_T3 < 0 | CP_AMT_T3 > 1000, NA_real_, CP_AMT_T3)  # Updated max to 1000
  )

# Verify the cleaning
verification_costs <- benefits_cleaned_v4 %>%
  summarise(
    deduct_min = min(DEDUCTIBLE, na.rm = TRUE),
    deduct_max = max(DEDUCTIBLE, na.rm = TRUE),
    deduct_median = median(DEDUCTIBLE, na.rm = TRUE),
    deduct_zero = sum(DEDUCTIBLE == 0, na.rm = TRUE),
    deduct_na = sum(is.na(DEDUCTIBLE)),
    
    pcp_min = min(CP_AMT_T1, na.rm = TRUE),
    pcp_max = max(CP_AMT_T1, na.rm = TRUE),
    pcp_median = median(CP_AMT_T1, na.rm = TRUE),
    pcp_zero = sum(CP_AMT_T1 == 0, na.rm = TRUE),
    pcp_na = sum(is.na(CP_AMT_T1)),
    
    spec_min = min(CP_AMT_T2, na.rm = TRUE),
    spec_max = max(CP_AMT_T2, na.rm = TRUE),
    spec_median = median(CP_AMT_T2, na.rm = TRUE),
    spec_zero = sum(CP_AMT_T2 == 0, na.rm = TRUE),
    spec_na = sum(is.na(CP_AMT_T2)),
    
    ed_min = min(CP_AMT_T3, na.rm = TRUE),
    ed_max = max(CP_AMT_T3, na.rm = TRUE),
    ed_median = median(CP_AMT_T3, na.rm = TRUE),
    ed_zero = sum(CP_AMT_T3 == 0, na.rm = TRUE),
    ed_na = sum(is.na(CP_AMT_T3))
  )

print("Verification of cleaned cost fields:")
print(verification_costs)
beep(8)

saveRDS(benefits_cleaned_v4, "benefits_cleaned_v4.RDS")
benefits_cleaned_v4 <- load(paste0(filepath, "benefits_cleaned_v4.RDS"))

##### 4.c.1 HSA----
HSA_net <- read_csv("HSA_Network.csv")

HSA_net <- HSA_net %>% 
  mutate(HSA_CD = if_else(is.na(HSA_CD) | HSA_CD == "11753", 0, 1))

HSA_net <- HSA_net %>% 
  rename(HSA_Flag = HSA_CD)

HSA_net <- HSA_net %>% 
  select(-c("NET_TYPE_CD", "NET_TYPE_DESC"))

# Deduplicate HSA data
HSA_net_dedup <- HSA_net %>%
  mutate(Year = year(HIER_EFF_DT)) %>%
  distinct(MVDID, MEMBERID, Year, .keep_all = TRUE)

# Clean and deduplicate benefits data
benefits_cleaned_v6 <- benefits_cleaned_v4 %>%
  group_by(MVDID, MEMBERID, Year) %>%
  slice_max(EFF_DATE, n = 1, with_ties = FALSE) %>%
  ungroup()

# Create final merged dataset
benefits_merged_v3 <- benefits_cleaned_v6 %>%
  left_join(HSA_net_dedup %>%
              select(MVDID, MEMBERID, Year, COMPANYKEY, SUBGROUPKEY, HSA_Flag),
            by = c("MVDID", "MEMBERID", "Year"))

benefits_final <- benefits_merged_v3

# Create lookup table for GROUP_NBR imputation
group_nbr_lookup <- benefits_final %>%
  filter(!is.na(HSA_Flag), !is.na(COMPANYKEY)) %>%
  group_by(GROUP_NBR) %>%
  summarise(
    n_members = n(),
    distinct_hsa_flags = n_distinct(HSA_Flag),
    hsa_flag_mode = as.numeric(names(which.max(table(HSA_Flag)))),
    pct_mode = round(100 * max(table(HSA_Flag)) / n(), 2)
  ) %>%
  filter(n_members >= 10, pct_mode >= 90) %>%  # Only keep highly consistent patterns
  select(GROUP_NBR, hsa_flag_mode)

# Create lookup table for company-level imputation as backup
company_lookup <- benefits_final %>%
  filter(!is.na(HSA_Flag), !is.na(COMPANYKEY)) %>%
  group_by(COMPANYKEY) %>%
  summarise(
    n_members = n(),
    hsa_flag_mode = as.numeric(names(which.max(table(HSA_Flag)))),
    pct_mode = round(100 * max(table(HSA_Flag)) / n(), 2)
  ) %>%
  filter(n_members >= 100, pct_mode >= 90) %>%  # Only keep consistent patterns
  select(COMPANYKEY, hsa_flag_mode)

# Perform imputation
benefits_final_imputed <- benefits_final %>%
  # First try GROUP_NBR imputation
  left_join(group_nbr_lookup, by = "GROUP_NBR") %>%
  # Then try COMPANYKEY imputation where GROUP_NBR failed
  left_join(company_lookup, by = "COMPANYKEY", suffix = c("_group", "_company")) %>%
  mutate(
    HSA_Flag_imputed = case_when(
      !is.na(HSA_Flag) ~ HSA_Flag,  # Keep original values
      !is.na(hsa_flag_mode_group) ~ hsa_flag_mode_group,  # Use GROUP_NBR imputation
      !is.na(hsa_flag_mode_company) ~ hsa_flag_mode_company,  # Use COMPANYKEY imputation
      TRUE ~ NA_real_  # Keep NA if no imputation possible
    )
  ) %>%
  select(-hsa_flag_mode_group, -hsa_flag_mode_company)

# Verify imputation results
imputation_summary <- benefits_final_imputed %>%
  summarise(
    total_records = n(),
    original_missing = sum(is.na(HSA_Flag)),
    still_missing = sum(is.na(HSA_Flag_imputed)),
    imputed_count = original_missing - still_missing,
    imputation_rate = round(100 * imputed_count / original_missing, 2)
  )

print(imputation_summary)

saveRDS(benefits_final_imputed, "benefits_final_imputed.rds")
benefits_final_imputed <- readRDS(paste0(filepath, "benefits_final_imputed.rds"))

##### 5. Treat Claims w/Benefits----
# 1. Clean benefits data - take most common benefit structure per company/subgroup/year
benefits_slim_unique <- benefits_final_imputed %>%
  group_by(COMPANYKEY, SUBGROUPKEY, Year) %>%
  summarise(
    DEDUCTIBLE = as.numeric(names(which.max(table(DEDUCTIBLE)))),
    CP_AMT_T1 = as.numeric(names(which.max(table(CP_AMT_T1)))),
    CP_AMT_T2 = as.numeric(names(which.max(table(CP_AMT_T2)))),
    CP_AMT_T3 = as.numeric(names(which.max(table(CP_AMT_T3)))),
    HSA_Flag_imputed = as.numeric(names(which.max(table(HSA_Flag_imputed)))),
    record_count = n()
  ) %>%
  ungroup()

# 2. Initial join
claims_with_benefits <- treat_claims %>%
  left_join(
    benefits_slim_unique, 
    by = c("COMPANY_KEY" = "COMPANYKEY", 
           "SUBGROUPKEY" = "SUBGROUPKEY",
           "Plan_year" = "Year")
  )

# 3. For the remaining unmatched, try to find patterns across years
missing_pairs <- claims_with_benefits %>%
  filter(is.na(HSA_Flag_imputed)) %>%
  select(COMPANY_KEY, SUBGROUPKEY) %>%
  distinct()

# 4. Find any data for these pairs in any year
available_benefits <- benefits_slim_unique %>%
  semi_join(
    missing_pairs,
    by = c("COMPANYKEY" = "COMPANY_KEY", "SUBGROUPKEY" = "SUBGROUPKEY")
  ) %>%
  group_by(COMPANYKEY, SUBGROUPKEY) %>%
  arrange(Year) %>%
  slice(1) %>%
  select(-Year, -record_count)

# 5. Add known patterns for specific companies
additional_imputations <- bind_rows(
  # Original imputations
  tibble(
    COMPANYKEY = c(4497, 4497, 4497),
    SUBGROUPKEY = c(502114, 502121, 502126),
    HSA_Flag_imputed = c(1, 1, 1),
    DEDUCTIBLE = 4000,
    CP_AMT_T1 = 0,
    CP_AMT_T2 = 0,
    CP_AMT_T3 = 0
  ),
  # New imputations based on patterns
  tibble(
    COMPANYKEY = c(
      12920,  # Large group
      4759, 16927, 2804,  # 295xxx pattern
      20811,  # 500xxx pattern
      23387, 17382  # Others
    ),
    SUBGROUPKEY = c(
      500543,
      295626, 295426, 295576,
      500567,
      491569, 520071
    ),
    HSA_Flag_imputed = c(
      0,  # Large group, likely traditional
      1, 1, 1,  # 295xxx pattern suggests HSA
      0,  # 500xxx like 12920
      0, 0  # Others default to traditional
    ),
    DEDUCTIBLE = c(
      1000,  # Traditional plan estimate
      3000, 3000, 3000,  # HSA-level deductibles
      1000,  # Like 12920
      1000, 1000  # Traditional estimates
    ),
    CP_AMT_T1 = c(20, 0, 0, 0, 20, 20, 20),
    CP_AMT_T2 = c(40, 0, 0, 0, 40, 40, 40),
    CP_AMT_T3 = c(60, 0, 0, 0, 60, 60, 60)
  )
)

# 6. Combine and create final dataset
final_imputed_benefits <- bind_rows(available_benefits, additional_imputations)

claims_with_benefits_final <- claims_with_benefits %>%
  left_join(
    final_imputed_benefits,
    by = c("COMPANY_KEY" = "COMPANYKEY", "SUBGROUPKEY" = "SUBGROUPKEY"),
    suffix = c("", "_imputed")
  ) %>%
  mutate(
    HSA_Flag_final = coalesce(HSA_Flag_imputed, HSA_Flag_imputed_imputed),
    DEDUCTIBLE_final = coalesce(DEDUCTIBLE, DEDUCTIBLE_imputed),
    CP_AMT_T1_final = coalesce(CP_AMT_T1, CP_AMT_T1_imputed),
    CP_AMT_T2_final = coalesce(CP_AMT_T2, CP_AMT_T2_imputed),
    CP_AMT_T3_final = coalesce(CP_AMT_T3, CP_AMT_T3_imputed)
  )

# 7. Generate summary statistics
imputation_summary <- claims_with_benefits_final %>%
  summarise(
    total_claims = n(),
    matched_claims = sum(!is.na(HSA_Flag_final)),
    unmatched_claims = sum(is.na(HSA_Flag_final)),
    match_rate = round(100 * matched_claims / total_claims, 2)
  )

print("Imputation Summary:")
print(imputation_summary)

# Add another round of imputations
additional_imputations_round2 <- tibble(
  COMPANYKEY = c(
    # 295xxx series - likely HSA
    12468, 14974,
    # Company 4497 additional subgroups - HSA pattern
    4497, 4497, 4497,
    # 500xxx series - traditional pattern
    24961, 19531,
    # Others
    22987, 50079, 22296, 50783, 6267, 14419, 20618, 22300
  ),
  SUBGROUPKEY = c(
    # 295xxx
    295629, 295637,
    # 4497 additional
    502124, 502116, 502122,
    # 500xxx
    500592, 500639,
    # Others
    485572, 514199, 479355, 520499, 501111, 510674, 490134, 479103
  ),
  HSA_Flag_imputed = c(
    # 295xxx - HSA
    1, 1,
    # 4497 - HSA
    1, 1, 1,
    # 500xxx - Traditional
    0, 0,
    # Others - Traditional default
    0, 0, 0, 0, 0, 0, 0, 0
  ),
  DEDUCTIBLE = c(
    # 295xxx - HSA level
    3000, 3000,
    # 4497 - HSA level
    4000, 4000, 4000,
    # Others - Traditional level
    1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000
  ),
  CP_AMT_T1 = c(0, 0, 0, 0, 0, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20),
  CP_AMT_T2 = c(0, 0, 0, 0, 0, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40),
  CP_AMT_T3 = c(0, 0, 0, 0, 0, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60)
)

# Update final imputed benefits
final_imputed_benefits <- bind_rows(final_imputed_benefits, additional_imputations_round2)

# Recreate final dataset with new imputations
claims_with_benefits_final <- claims_with_benefits %>%
  left_join(
    final_imputed_benefits,
    by = c("COMPANY_KEY" = "COMPANYKEY", "SUBGROUPKEY" = "SUBGROUPKEY"),
    suffix = c("", "_imputed")
  ) %>%
  mutate(
    HSA_Flag_final = coalesce(HSA_Flag_imputed, HSA_Flag_imputed_imputed),
    DEDUCTIBLE_final = coalesce(DEDUCTIBLE, DEDUCTIBLE_imputed),
    CP_AMT_T1_final = coalesce(CP_AMT_T1, CP_AMT_T1_imputed),
    CP_AMT_T2_final = coalesce(CP_AMT_T2, CP_AMT_T2_imputed),
    CP_AMT_T3_final = coalesce(CP_AMT_T3, CP_AMT_T3_imputed)
  )

# Generate updated summary statistics
imputation_summary <- claims_with_benefits_final %>%
  summarise(
    total_claims = n(),
    matched_claims = sum(!is.na(HSA_Flag_final)),
    unmatched_claims = sum(is.na(HSA_Flag_final)),
    match_rate = round(100 * matched_claims / total_claims, 2)
  )

print("Updated Imputation Summary:")
print(imputation_summary)


# Analyze distribution of imputed values
imputation_distribution <- claims_with_benefits_final %>%
  group_by(
    HSA_Flag_final,
    DEDUCTIBLE_final,
    CP_AMT_T1_final,
    CP_AMT_T2_final,
    CP_AMT_T3_final
  ) %>%
  summarise(
    n_claims = n(),
    n_companies = n_distinct(COMPANY_KEY),
    pct_claims = round(100 * n()/nrow(claims_with_benefits_final), 2)
  ) %>%
  arrange(desc(n_claims))

# Check imputation source
source_analysis <- claims_with_benefits_final %>%
  mutate(
    imputation_source = case_when(
      !is.na(HSA_Flag_imputed) ~ "original_benefits",
      !is.na(HSA_Flag_imputed_imputed) ~ "imputed",
      TRUE ~ "error"
    )
  ) %>%
  group_by(imputation_source) %>%
  summarise(
    n_claims = n(),
    n_companies = n_distinct(COMPANY_KEY),
    pct_claims = round(100 * n()/nrow(claims_with_benefits_final), 2)
  )

print("Imputation Distribution:")
print(imputation_distribution)
print("\nImputation Source Analysis:")
print(source_analysis)


## D. Control/Claims----
#### 1. Elixhauser Comorbidities ----
library(comorbidity)
control_claims <- read_csv("control_claims.csv")

# 1. Calculate Elixhauser scores by MVDID and Plan_year
elix <- control_claims %>% 
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

# 3. Add scores to control claims and create bins
breaks <- c(0, 1, 2, 3, Inf)
labels <- c("0", "1", "2", "3+")

control_claims_with_elix <- control_claims %>%
  left_join(elix_scores, by = c("MVDID", "Plan_year")) %>%
  mutate(
    tot_comorbidities = coalesce(tot_comorbidities, 0),
    comorbid_bins = cut(tot_comorbidities, 
                        breaks = breaks, 
                        labels = labels, 
                        include.lowest = TRUE,
                        right = FALSE)
  )

#### 2. Age Bins----
# Remove negative values
control_claims_with_elix <- control_claims_with_elix %>%
  filter(age_at_plan_year_start >= 0)  # Remove impossible negative ages

breaks <- c(0, 4, 12, 17, 34, 49, 64, Inf)
labels <- c("0-4", "5-12", "13-17", "18-34", "35-49", "50-64", "65+")


control_claims_with_elix_age <- control_claims_with_elix %>%
  mutate(age_group = cut(age_at_plan_year_start, 
                         breaks = breaks, 
                         labels = labels, 
                         include.lowest = TRUE, 
                         right = FALSE))

#### 3. Family Size----
control_claims_elix_age_fam <- control_claims_with_elix_age %>%
  group_by(SUBSCRIBERID, Plan_year) %>%
  mutate(new_fam_size = n_distinct(MVDID)) %>%
  ungroup() %>%
  mutate(family_size_bins = cut(new_fam_size,
                                breaks = c(0, 2, 3, 4, 5, Inf),
                                labels = c("2", "3", "4", "5", "6+"),
                                include.lowest = TRUE,
                                right = FALSE))

#### 4. Combine Claims and Benefits----
# First, identify unmatched control claims
unmatched_control <- control_claims_elix_age_fam %>%
  select(COMPANY_KEY, SUBGROUPKEY) %>%
  distinct()

# Then add control period imputations
control_imputations <- unmatched_control %>%
  distinct(COMPANY_KEY, SUBGROUPKEY) %>%
  mutate(
    HSA_Flag_imputed = case_when(
      substr(as.character(SUBGROUPKEY), 1, 3) %in% c("295", "296") ~ 1,
      substr(as.character(SUBGROUPKEY), 1, 3) %in% c("499", "500", "501") ~ 0,
      TRUE ~ 0  # Conservative default to traditional plans
    ),
    DEDUCTIBLE = case_when(
      HSA_Flag_imputed == 1 ~ 3000,  # HSA plans get higher deductible
      TRUE ~ 1000  # Traditional plans get standard deductible
    ),
    CP_AMT_T1 = if_else(HSA_Flag_imputed == 1, 0, 20),
    CP_AMT_T2 = if_else(HSA_Flag_imputed == 1, 0, 40),
    CP_AMT_T3 = if_else(HSA_Flag_imputed == 1, 0, 60)
  ) %>%
  rename(COMPANYKEY = COMPANY_KEY)

# Combine with existing imputed benefits
final_imputed_benefits_expanded <- bind_rows(
  final_imputed_benefits,
  control_imputations
)

# Rejoin with control claims
control_claims_with_benefits_v2 <- control_claims_elix_age_fam %>%
  left_join(
    final_imputed_benefits_expanded %>%
      select(
        COMPANYKEY,
        SUBGROUPKEY,
        DEDUCTIBLE,
        CP_AMT_T1,
        CP_AMT_T2,
        CP_AMT_T3,
        HSA_Flag_imputed
      ) %>%
      rename(
        COMPANY_KEY = COMPANYKEY,
        imputed_deductible = DEDUCTIBLE,
        imputed_copay_t1 = CP_AMT_T1,
        imputed_copay_t2 = CP_AMT_T2,
        imputed_copay_t3 = CP_AMT_T3,
        imputed_hsa_flag = HSA_Flag_imputed
      ),
    by = c("COMPANY_KEY", "SUBGROUPKEY")
  )

# Check new match rate
match_summary_v2 <- control_claims_with_benefits_v2 %>%
  summarise(
    total_claims = n(),
    matched_claims = sum(!is.na(imputed_hsa_flag)),
    match_rate = round(100 * matched_claims / total_claims, 2)
  )

print("Updated Match Summary:")
print(match_summary_v2)

plan_distribution <- control_claims_with_benefits_v2 %>%
  group_by(imputed_hsa_flag) %>%
  summarise(
    n_claims = n(),
    pct_claims = round(100 * n()/nrow(control_claims_with_benefits_v2), 2)
  )

print("Distribution of Plan Types:")
print(plan_distribution)

##### Final Dataset----
# Create deductible mapping
deductible_mapping <- tibble(
  Plan_year = 2016:2023,
  recommended_deductible = c(
    2000, # 2016
    2000, # 2017
    2200, # 2018
    2200, # 2019
    2500, # 2020
    2500, # 2021
    2500, # 2022
    2500  # 2023
  )
)

# Update control claims with new deductible strategy and cohort indicator
control_claims_with_benefits_v2 <- control_claims_with_benefits_v2 %>%
  left_join(deductible_mapping, by = "Plan_year") %>%
  mutate(
    DEDUCTIBLE_final = case_when(
      imputed_hsa_flag == 1 ~ recommended_deductible,
      TRUE ~ 1000  # Keep original non-HSA deductible
    ),
    Treat_Control_Cohort = 0,
    # Create final copay columns
    CP_AMT_T1_final = imputed_copay_t1,
    CP_AMT_T2_final = imputed_copay_t2,
    CP_AMT_T3_final = imputed_copay_t3,
    HSA_Flag_final = imputed_hsa_flag
  ) %>%
  select(-recommended_deductible) %>%  # Remove the mapping column
  rename(
    PCP_copay = CP_AMT_T1_final,
    Specialist_copay = CP_AMT_T2_final,
    ED_copay = CP_AMT_T3_final
  )

# Update treatment claims with cohort indicator
claims_with_benefits_final <- claims_with_benefits_final %>%
  left_join(deductible_mapping, by = "Plan_year") %>%
  mutate(
    DEDUCTIBLE_final = case_when(
      HSA_Flag_final == 1 & is.na(DEDUCTIBLE) ~ recommended_deductible,
      HSA_Flag_final == 1 & !is.na(DEDUCTIBLE) ~ DEDUCTIBLE,  # Keep original if available
      TRUE ~ DEDUCTIBLE_final  # Keep existing non-HSA values
    ),
    Treat_Control_Cohort = 1
  ) %>%
  select(-recommended_deductible) %>%  # Remove the mapping column
  rename(
    PCP_copay = CP_AMT_T1_final,
    Specialist_copay = CP_AMT_T2_final,
    ED_copay = CP_AMT_T3_final
  )

# Assign Pre/post period to control cohort
# Get summary of all year pairs
year_pair_summary <- control_claims_with_benefits_v2 %>%
  group_by(MVDID, COMPANY_KEY) %>%
  summarise(
    years = list(sort(unique(Plan_year))),
    n_years = n_distinct(Plan_year)
  ) %>%
  filter(n_years == 2) %>%
  mutate(year_pair = map_chr(years, ~paste(sort(.x), collapse = "-"))) %>%
  group_by(year_pair) %>%
  summarise(
    n_members = n()
  ) %>%
  arrange(year_pair)

print("Distribution of consecutive year pairs:")
print(year_pair_summary)

control_claims_with_benefits_v2 <- control_claims_with_benefits_v2 %>%
  group_by(MVDID, COMPANY_KEY) %>%
  mutate(
    min_year = min(Plan_year),
    max_year = max(Plan_year),
    post_period = if_else(Plan_year == max_year, 1, 0)
  ) %>%
  ungroup() %>%
  select(-min_year, -max_year)

# Verify the changes
verification <- control_claims_with_benefits_v2 %>%
  group_by(post_period, Plan_year) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID)
  )

print(verification)

#Assign pre/post period to treatment cohort
treatment_claims <- claims_with_benefits_final %>%
  filter(switch_type == "Aggregate Only to Embedded Only") %>%
  mutate(
    post_period = case_when(
      period_type == "pre_switch_year" ~ 0,
      period_type == "post_switch_year" ~ 1,
      period_type == "switch_year" ~ 1,  # Counting switch year as post-period
      TRUE ~ NA_real_
    )
  )

# Verify the changes again
verification <- treatment_claims %>%
  group_by(post_period, Plan_year, period_type) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID)
  )

print("Updated distribution of claims and members by period:")
print(verification)

# Select common columns and combine datasets
combined_claims <- bind_rows(
  control_claims_with_benefits_v2 %>%
    select(
      MVDID, SUBSCRIBERID, COMPANY_KEY, SUBGROUPKEY, MEMBERID,
      ZIPCODE, PARTYKEY, PLACEOFSERVICE, PROCEDURECODE, MOD1,
      SERVICEFROMDATE, REVENUECODE, BILLEDAMOUNT, NONCOVEREDAMOUNT,
      ALLOWEDAMOUNT, PAIDAMOUNT, COBAMOUNT, COINSURANCEAMOUNT,
      COPAYAMOUNT, DEDUCTIBLEAMOUNT, MEMBERKEY, PLANGROUP,
      LOB, CLAIMNUMBER, CODEVALUE, CODETYPE, PATIENTDOB,
      PATIENTGENDER, NETWORKINDICATOR, Plan_year, plan_year_start,
      age_at_plan_year_start, family_size, DEDUCTIBLE_CATEGORY,
      tot_comorbidities, comorbid_bins, age_group, family_size_bins,
      DEDUCTIBLE_final, PCP_copay, Specialist_copay, ED_copay,
      HSA_Flag_final, Treat_Control_Cohort, post_period
    ),
  
  treatment_claims %>%
    select(
      MVDID, SUBSCRIBERID, COMPANY_KEY, SUBGROUPKEY, MEMBERID,
      ZIPCODE, PARTYKEY, PLACEOFSERVICE, PROCEDURECODE, MOD1,
      SERVICEFROMDATE, REVENUECODE, BILLEDAMOUNT, NONCOVEREDAMOUNT,
      ALLOWEDAMOUNT, PAIDAMOUNT, COBAMOUNT, COINSURANCEAMOUNT,
      COPAYAMOUNT, DEDUCTIBLEAMOUNT, MEMBERKEY, PLANGROUP,
      LOB, CLAIMNUMBER, CODEVALUE, CODETYPE, PATIENTDOB,
      PATIENTGENDER, NETWORKINDICATOR, Plan_year, plan_year_start,
      age_at_plan_year_start, family_size, DEDUCTIBLE_CATEGORY,
      tot_comorbidities, comorbid_bins, age_group, family_size_bins,
      DEDUCTIBLE_final, PCP_copay, Specialist_copay, ED_copay,
      HSA_Flag_final, Treat_Control_Cohort, post_period
    )
)

# Check the combined dataset
summary_stats <- combined_claims %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID)
  )

# Additional verification of new deductible values and copays
benefit_summary <- combined_claims %>%
  group_by(Treat_Control_Cohort, post_period, Plan_year, HSA_Flag_final) %>%
  summarise(
    n_claims = n(),
    avg_deductible = mean(DEDUCTIBLE_final),
    avg_pcp_copay = mean(PCP_copay),
    avg_specialist_copay = mean(Specialist_copay),
    avg_ed_copay = mean(ED_copay),
    n_members = n_distinct(MVDID)
  ) %>%
  arrange(Treat_Control_Cohort, post_period, Plan_year, HSA_Flag_final)

print("Combined Dataset Summary:")
print(summary_stats)
print("\nBenefit Design Summary by Year and HSA Status:")
print(benefit_summary)

# Add deductible category based on actual deductible amounts
combined_claims <- combined_claims %>%
  mutate(
    deductible_level = case_when(
      DEDUCTIBLE_final == 1000 ~ "Low",
      DEDUCTIBLE_final == 2000 ~ "Medium",
      DEDUCTIBLE_final > 2000 ~ "High"
    )
  )

# Check distribution
deductible_summary <- combined_claims %>%
  group_by(Treat_Control_Cohort, post_period, deductible_level, DEDUCTIBLE_final) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID)
  ) %>%
  arrange(Treat_Control_Cohort, post_period, DEDUCTIBLE_final)

print("\nDeductible Level Distribution:")
print(deductible_summary)

# 3. Prepare for DiD----
## A. Utilization categories----
#Get top 20
##Filter out NAs----
df <- combined_claims %>%
  filter(!is.na(PROCEDURECODE))

##Procedure Codes----
df <- df %>%
  group_by(PROCEDURECODE) %>%
  tally(name = "Count") %>%
  arrange(desc(Count))

# Get the top 20 procedure codes

top_25 <- df %>%
  top_n(25)

# View the top 20 procedure codes
view(top_25)

### Map procedure codes ----

# STEP 1: Combine original and additional mappings for procedure codes
mapping_1 <- data.frame(
  PROCEDURECODE = c(
    # Original procedure codes
    '99214', '99213', '97110', '97530', '85025', '98941', '36415', '80053',
    '80061', '99203', '97140', '97014', '92507', '90837', '97112', '80050',
    '80048', '96372', '83036', '99204',
    # Newly added procedure codes
    '81003', '97010', '87804', '99212', '83735', '84439', '95117', '98943',
    '81001', 'J1100', '97032', '85027', '93005', '88305', '95115', '3074F',
    '87880', '82607', '97035', '84484',
    # Top "Other" procedure codes
    '3078F', '3079F', '3008F', '90686', '96375', '90471', '71045', '71046',
    '77067', '87086', '87635', '84153', '82728', '84403', '99215',
    '99232', '99202', '99284', '99285', '99291', '93010', 'Q9967', '80076',
    '99395', '85652', '85610', '83690', 'J2405'
  ),
  procedure_category = c(
    # Original procedure categories
    'E_and_M', 'E_and_M', 'PT', 'PT', 'Laboratory', 'Chiropractry', 'Collection', 'Laboratory',
    'Laboratory', 'E_and_M', 'PT', 'PT', 'Speech_Therapy', 'Mental_Health', 'PT', 'Laboratory',
    'Laboratory', 'Injection', 'Laboratory', 'E_and_M',
    # Newly added categories
    'Laboratory',          # 81003 - Urinalysis
    'PT',                  # 97010 - Physical Therapy
    'Laboratory',          # 87804 - Infectious Disease Test
    'E_and_M',             # 99212 - Evaluation and Management
    'Laboratory',          # 83735 - Lipid Testing
    'Laboratory',          # 84439 - Thyroid Testing
    'Allergy_Treatment',   # 95117 - Allergy Treatment
    'Chiropractry',        # 98943 - Chiropractic Treatment
    'Laboratory',          # 81001 - Urinalysis
    'Injection',           # J1100 - Injection
    'PT',                  # 97032 - Physical Therapy
    'Laboratory',          # 85027 - Hematology Testing
    'Laboratory',          # 93005 - Cardiac Testing
    'Pathology',           # 88305 - Pathology Testing
    'Allergy_Treatment',   # 95115 - Allergy Treatment
    'Quality_Measurement', # 3074F - Quality Measurement
    'Laboratory',          # 87880 - Infectious Disease Test
    'Laboratory',          # 82607 - Vitamin Testing
    'PT',                  # 97035 - Physical Therapy
    'Laboratory',          # 84484 - Thyroid Testing
    # Top "Other" procedure categories
    'Quality_Measurement', 'Quality_Measurement', 'Quality_Measurement', 'Immunization',
    'Injection', 'Injection', 'Imaging', 'Imaging', 'Imaging', 'Laboratory', 'Laboratory',
    'Laboratory', 'Laboratory', 'Laboratory', 'E_and_M', 'E_and_M', 'E_and_M',
    'E_and_M', 'E_and_M', 'E_and_M', 'Cardiac_Testing', 'Contrast_Material', 'Laboratory',
    'Preventive_Medicine', 'Hematology', 'Hematology', 'Hematology', 'Injection'
  ),
  proc_description = c(
    # Original descriptions
    'Office visit, established patient, 25 minutes',         # 99214
    'Office visit, established patient, 15 minutes',         # 99213
    'Therapeutic exercises to improve strength/motion',      # 97110
    'Therapeutic activities to improve functional skills',   # 97530
    'Complete blood count (CBC)',                            # 85025
    'Chiropractic manipulative treatment (3-4 regions)',     # 98941
    'Blood collection by venipuncture',                      # 36415
    'Comprehensive metabolic panel (CMP)',                   # 80053
    'Lipid panel (cholesterol, HDL, triglycerides)',         # 80061
    'Office visit, new patient, 30 minutes',                 # 99203
    'Manual therapy techniques (e.g., mobilization)',        # 97140
    'Electrical stimulation (unattended)',                  # 97014
    'Speech and language therapy',                           # 92507
    'Psychotherapy, 60 minutes',                             # 90837
    'Neuromuscular reeducation',                             # 97112
    'General health panel',                                  # 80050
    'Basic metabolic panel',                                 # 80048
    'Injection, subcutaneous or intramuscular',              # 96372
    'Hemoglobin A1c (HbA1c) test for diabetes',              # 83036
    'Office visit, new patient, 45 minutes',                 # 99204
    # Newly added descriptions
    'Urinalysis, non-automated',                              # 81003
    'Application of hot or cold packs for PT',                # 97010
    'Rapid infectious agent test (e.g., influenza)',          # 87804
    'Office visit, established patient, 10 minutes',          # 99212
    'Lipoprotein particle testing',                           # 83735
    'Thyroxine (T4) testing',                                 # 84439
    'Allergy injection without antigen',                      # 95117
    'Chiropractic manipulative treatment (5 regions)',        # 98943
    'Urinalysis, automated',                                  # 81001
    'Injection of corticosteroid or other therapeutic agent', # J1100
    'Electrical stimulation therapy',                         # 97032
    'Complete blood count (automated)',                       # 85027
    'Electrocardiogram (EKG) tracing',                        # 93005
    'Surgical pathology, gross and microscopic examination',  # 88305
    'Allergy injection with antigen',                         # 95115
    'Quality reporting for clinical performance',             # 3074F
    'Rapid strep test',                                       # 87880
    'Vitamin D, 25-hydroxy',                                  # 82607
    'Ultrasound therapy for physical therapy',                # 97035
    'Thyroid-stimulating hormone (TSH)',                      # 84484
    # Top "Other" procedure descriptions
    'Performance measurement for blood pressure',             # 3078F
    'Performance measurement for BMI',                        # 3079F
    'Quality reporting for tobacco use',                      # 3008F
    'Influenza virus vaccine',                                # 90686
    'Therapeutic, prophylactic, or diagnostic injection',     # 96375
    'Immunization administration',                            # 90471
    'Chest X-ray, single view',                               # 71045
    'Chest X-ray, 2 views',                                   # 71046
    'Screening mammogram',                                    # 77067
    'Urine culture, bacterial',                               # 87086
    'Molecular infectious disease test',                      # 87635
    'Prostate-specific antigen (PSA)',                        # 84153
    'Vitamin B12 measurement',                                # 82728
    'Total T3 testing',                                       # 84403
    'Office visit, established patient, 40 minutes',          # 99215
    'Subsequent hospital care, 35 minutes',                   # 99232
    'Office visit, new patient, 20 minutes',                  # 99202
    'Emergency department visit',                             # 99284
    'Critical care, first hour',                              # 99285
    'Critical care, each additional 30 minutes',              # 99291
    'Cardiac stress test interpretation',                     # 93010
    'Low osmolar contrast material',                          # Q9967
    'Hepatic function panel',                                 # 80076
    'Preventive medicine, established patient, 18-39 years',  # 99395
    'Erythrocyte sedimentation rate',                         # 85652
    'Prothrombin time (PT)',                                  # 85610
    'Blood lead level',                                       # 83690
    'Injection of ondansetron'                                # J2405
  )
)

# Ensure PROCEDURECODE is a character in both datasets
combined_claims$PROCEDURECODE <- as.character(combined_claims$PROCEDURECODE)
mapping_1$PROCEDURECODE <- as.character(mapping_1$PROCEDURECODE)


# Join the mapping dataframe to the claims data
combined_claims <- combined_claims %>%
  left_join(mapping_1, by = "PROCEDURECODE")

# Handle missing or unmapped claims
combined_claims <- combined_claims %>%
  mutate(
    procedure_category = case_when(
      is.na(PROCEDURECODE) ~ "Missing",               # Claims with no PROCEDURECODE
      is.na(procedure_category) ~ "Other",           # Claims not mapped to a category
      TRUE ~ procedure_category                      # Keep mapped categories
    ),
    proc_description = case_when(
      procedure_category == "Missing" ~ "Missing",                 # Description for missing PROCEDURECODE
      procedure_category == "Other" ~ "No description available",  # Description for unmapped claims
      TRUE ~ proc_description                                      # Keep mapped descriptions
    )
  )

## A.a. Deduplication----
# Step 1: Aggregate from claimline to claim level
claim_level <- combined_claims %>%
  group_by(MVDID, SERVICEFROMDATE, PROCEDURECODE, CLAIMNUMBER) %>%
  summarise(
    # Sum all payment/amount fields
    total_billed = sum(BILLEDAMOUNT, na.rm = TRUE),
    total_allowed = sum(ALLOWEDAMOUNT, na.rm = TRUE),
    total_paid = sum(PAIDAMOUNT, na.rm = TRUE),
    total_deductible = sum(DEDUCTIBLEAMOUNT, na.rm = TRUE),
    total_copay = sum(COPAYAMOUNT, na.rm = TRUE),
    total_coins = sum(COINSURANCEAMOUNT, na.rm = TRUE),
    total_cob = sum(COBAMOUNT, na.rm = TRUE),
    total_noncovered = sum(NONCOVEREDAMOUNT, na.rm = TRUE),
    
    # Keep first occurrence for ID/categorical variables
    SUBSCRIBERID = first(SUBSCRIBERID),
    COMPANY_KEY = first(COMPANY_KEY),
    SUBGROUPKEY = first(SUBGROUPKEY),
    MEMBERID = first(MEMBERID),
    ZIPCODE = first(ZIPCODE),
    PARTYKEY = first(PARTYKEY),
    PLACEOFSERVICE = first(PLACEOFSERVICE),
    MOD1 = first(MOD1),
    REVENUECODE = first(REVENUECODE),
    MEMBERKEY = first(MEMBERKEY),
    PLANGROUP = first(PLANGROUP),
    LOB = first(LOB),
    
    # Concatenate unique diagnosis codes
    CODEVALUE = paste(unique(CODEVALUE), collapse = "; "),
    CODETYPE = paste(unique(CODETYPE), collapse = "; "),
    
    # Keep first occurrence for demographic/plan variables
    PATIENTDOB = first(PATIENTDOB),
    PATIENTGENDER = first(PATIENTGENDER),
    NETWORKINDICATOR = first(NETWORKINDICATOR),
    Plan_year = first(Plan_year),
    plan_year_start = first(plan_year_start),
    age_at_plan_year_start = first(age_at_plan_year_start),
    family_size = first(family_size),
    DEDUCTIBLE_CATEGORY = first(DEDUCTIBLE_CATEGORY),
    tot_comorbidities = first(tot_comorbidities),
    comorbid_bins = first(comorbid_bins),
    age_group = first(age_group),
    family_size_bins = first(family_size_bins),
    DEDUCTIBLE_final = first(DEDUCTIBLE_final),
    PCP_copay = first(PCP_copay),
    Specialist_copay = first(Specialist_copay),
    ED_copay = first(ED_copay),
    HSA_Flag_final = first(HSA_Flag_final),
    Treat_Control_Cohort = first(Treat_Control_Cohort),
    post_period = first(post_period),
    deductible_level = first(deductible_level),
    procedure_category = first(procedure_category),
    proc_description = first(proc_description),
    
    # Add count of claim lines
    claim_lines = n(),
    .groups = 'drop'
  )

# Step 2: Handle duplicate claims (keep latest version)
clean_encounters <- claim_level %>%
  group_by(MVDID, SERVICEFROMDATE, PROCEDURECODE) %>%
  slice_max(CLAIMNUMBER, n = 1) %>%
  ungroup()

# Verification
print("Variable count check:")
print(paste("Original variables:", ncol(combined_claims)))
print(paste("Cleaned variables:", ncol(clean_encounters)))
beep(8)

# STEP 2: ANALYZE FREQUENCIES BY GROUP AND PERIOD
# Analyze frequencies by group and period
utilization_freq <- clean_encounters %>%
  group_by(Treat_Control_Cohort, post_period, procedure_category, proc_description) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID),
    total_paid = sum(total_paid, na.rm = TRUE),  # Changed from PAIDAMOUNT to total_paid
    .groups = 'drop'
  ) %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  mutate(
    pct_of_total = n_claims / sum(n_claims) * 100,
    claims_per_member = n_claims / n_members,
    avg_paid = total_paid / n_claims
  ) %>%
  arrange(Treat_Control_Cohort, post_period, -n_claims)

# STEP 3: CALCULATE CHANGES BETWEEN PERIODS
# Calculate changes between periods
utilization_changes <- utilization_freq %>%
  select(Treat_Control_Cohort, post_period, procedure_category, proc_description, n_claims, pct_of_total) %>%
  pivot_wider(
    names_from = post_period,
    values_from = c(n_claims, pct_of_total)
  ) %>%
  rename(
    n_claims_pre = `n_claims_0`,
    n_claims_post = `n_claims_1`,
    pct_of_total_pre = `pct_of_total_0`,
    pct_of_total_post = `pct_of_total_1`
  ) %>%
  mutate(
    pct_change = ((n_claims_post - n_claims_pre) / n_claims_pre) * 100,
    share_change = pct_of_total_post - pct_of_total_pre
  ) %>%
  arrange(Treat_Control_Cohort, -pct_change)

# STEP 4: PRINT RESULTS
# Print results for each group and period
# Control Group - Pre
print("\nControl Group - Pre Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 0, post_period == 0) %>%
        select(procedure_category, proc_description, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Control Group - Post
print("\nControl Group - Post Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 0, post_period == 1) %>%
        select(procedure_category, proc_description, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Treatment Group - Pre
print("\nTreatment Group - Pre Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 1, post_period == 0) %>%
        select(procedure_category, proc_description, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Treatment Group - Post
print("\nTreatment Group - Post Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 1, post_period == 1) %>%
        select(procedure_category, proc_description, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))


## B. Analytical Dataset----
final_analytical_dataset <- clean_encounters %>%
  mutate(
    # Outcome Variables (Visit Types) - these stay the same
    is_eandem = case_when(
      procedure_category == "E_and_M" ~ 1,
      TRUE ~ 0
    ),
    is_pt = case_when(
      procedure_category == "PT" ~ 1,
      TRUE ~ 0
    ),
    is_laboratory = case_when(
      procedure_category == "Laboratory" ~ 1,
      TRUE ~ 0
    ),
    is_mental_health = case_when(
      procedure_category == "Mental_Health" ~ 1,
      TRUE ~ 0
    ),
    is_speech_therapy = case_when(
      procedure_category == "Speech_Therapy" ~ 1,
      TRUE ~ 0
    ),
    is_immunization = case_when(
      procedure_category == "Immunization" ~ 1,
      TRUE ~ 0
    ),
    is_preventive = case_when(
      procedure_category == "Preventive_Medicine" ~ 1,
      TRUE ~ 0
    )
  ) %>%
  select(
    # Updated variable names for amounts
    MVDID, SUBSCRIBERID, age_group, family_size_bins, comorbid_bins,             
    PATIENTGENDER, age_at_plan_year_start, tot_comorbidities,
    DEDUCTIBLE_CATEGORY, PCP_copay, Specialist_copay, ED_copay,
    DEDUCTIBLE_final, total_billed, total_allowed, total_paid,
    total_coins, total_copay, total_deductible, Plan_year,
    HSA_Flag_final, Treat_Control_Cohort, post_period, deductible_level,
    
    # New outcome variables
    is_eandem, is_pt, is_laboratory, is_mental_health,
    is_speech_therapy, is_immunization, is_preventive
  )

# Update the yearly utilization calculation
yearly_utilization <- final_analytical_dataset %>%
  group_by(MVDID, post_period) %>%
  summarise(
    # Keep your existing grouping variables
    Treat_Control_Cohort = first(Treat_Control_Cohort),
    Plan_year = first(Plan_year),
    
    # Visit counts for new categories
    n_eandem = sum(is_eandem),
    n_pt = sum(is_pt),
    n_laboratory = sum(is_laboratory),
    n_mental_health = sum(is_mental_health),
    n_speech_therapy = sum(is_speech_therapy),
    n_immunization = sum(is_immunization),
    n_preventive = sum(is_preventive),
    
    # Updated cost calculations with new variable names
    eandem_paid = sum(total_paid * (is_eandem == 1), na.rm = TRUE),
    eandem_oop = sum((total_coins + total_copay + total_deductible) * (is_eandem == 1), na.rm = TRUE),
    
    pt_paid = sum(total_paid * (is_pt == 1), na.rm = TRUE),
    pt_oop = sum((total_coins + total_copay + total_deductible) * (is_pt == 1), na.rm = TRUE),
    
    laboratory_paid = sum(total_paid * (is_laboratory == 1), na.rm = TRUE),
    laboratory_oop = sum((total_coins + total_copay + total_deductible) * (is_laboratory == 1), na.rm = TRUE),
    
    mental_health_paid = sum(total_paid * (is_mental_health == 1), na.rm = TRUE),
    mental_health_oop = sum((total_coins + total_copay + total_deductible) * (is_mental_health == 1), na.rm = TRUE),
    
    speech_therapy_paid = sum(total_paid * (is_speech_therapy == 1), na.rm = TRUE),
    speech_therapy_oop = sum((total_coins + total_copay + total_deductible) * (is_speech_therapy == 1), na.rm = TRUE),
    
    immunization_paid = sum(total_paid * (is_immunization == 1), na.rm = TRUE),
    immunization_oop = sum((total_coins + total_copay + total_deductible) * (is_immunization == 1), na.rm = TRUE),
    
    preventive_paid = sum(total_paid * (is_preventive == 1), na.rm = TRUE),
    preventive_oop = sum((total_coins + total_copay + total_deductible) * (is_preventive == 1), na.rm = TRUE),
    
    # Keep all member characteristics
    SUBSCRIBERID = first(SUBSCRIBERID),
    age_group = first(age_group),
    age_at_plan_year_start = first(age_at_plan_year_start),
    family_size_bins = first(family_size_bins),
    tot_comorbidities = first(tot_comorbidities),
    comorbid_bins = first(comorbid_bins),
    PATIENTGENDER = first(PATIENTGENDER),
    deductible_level = first(deductible_level),
    DEDUCTIBLE_CATEGORY = first(DEDUCTIBLE_CATEGORY),
    HSA_Flag_final = first(HSA_Flag_final),
    PCP_copay = first(PCP_copay),
    Specialist_copay = first(Specialist_copay),
    ED_copay = first(ED_copay),
    .groups = 'drop'
  )

## C. Yearly Utilization----
#Calculate yearly utilization and cost measures per member
yearly_utilization <- final_analytical_dataset %>%
  group_by(MVDID, post_period) %>%
  summarise(
    # Keep treatment status and year
    Treat_Control_Cohort = first(Treat_Control_Cohort),
    Plan_year = first(Plan_year),
    
    # Visit counts
    n_eandem = sum(is_eandem),
    n_pt = sum(is_pt),
    n_laboratory = sum(is_laboratory),
    n_mental_health = sum(is_mental_health),
    n_speech_therapy = sum(is_speech_therapy),
    n_immunization = sum(is_immunization),
    n_preventive = sum(is_preventive),
    
    # Total yearly costs - updated variable names
    total_paid = sum(total_paid, na.rm = TRUE),
    total_oop = sum(total_coins + total_copay + total_deductible, na.rm = TRUE),
    
    # Visit-type specific costs - updated variable names
    eandem_paid = sum(total_paid * (is_eandem == 1), na.rm = TRUE),
    eandem_oop = sum((total_coins + total_copay + total_deductible) * (is_eandem == 1), na.rm = TRUE),
    
    pt_paid = sum(total_paid * (is_pt == 1), na.rm = TRUE),
    pt_oop = sum((total_coins + total_copay + total_deductible) * (is_pt == 1), na.rm = TRUE),
    
    laboratory_paid = sum(total_paid * (is_laboratory == 1), na.rm = TRUE),
    laboratory_oop = sum((total_coins + total_copay + total_deductible) * (is_laboratory == 1), na.rm = TRUE),
    
    mental_health_paid = sum(total_paid * (is_mental_health == 1), na.rm = TRUE),
    mental_health_oop = sum((total_coins + total_copay + total_deductible) * (is_mental_health == 1), na.rm = TRUE),
    
    speech_therapy_paid = sum(total_paid * (is_speech_therapy == 1), na.rm = TRUE),
    speech_therapy_oop = sum((total_coins + total_copay + total_deductible) * (is_speech_therapy == 1), na.rm = TRUE),
    
    immunization_paid = sum(total_paid * (is_immunization == 1), na.rm = TRUE),
    immunization_oop = sum((total_coins + total_copay + total_deductible) * (is_immunization == 1), na.rm = TRUE),
    
    preventive_paid = sum(total_paid * (is_preventive == 1), na.rm = TRUE),
    preventive_oop = sum((total_coins + total_copay + total_deductible) * (is_preventive == 1), na.rm = TRUE),
    
    # Member characteristics (these stay the same)
    SUBSCRIBERID = first(SUBSCRIBERID),
    age_group = first(age_group),
    age_at_plan_year_start = first(age_at_plan_year_start),
    family_size_bins = first(family_size_bins),
    tot_comorbidities = first(tot_comorbidities),
    comorbid_bins = first(comorbid_bins),
    PATIENTGENDER = first(PATIENTGENDER),
    deductible_level = first(deductible_level),
    DEDUCTIBLE_CATEGORY = first(DEDUCTIBLE_CATEGORY),
    HSA_Flag_final = first(HSA_Flag_final),
    PCP_copay = first(PCP_copay),
    Specialist_copay = first(Specialist_copay),
    ED_copay = first(ED_copay),
    .groups = 'drop'
  )

# Verify the aggregation
summary_stats <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    n_members = n_distinct(MVDID),
    
    # Visit counts
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        avg = ~mean(.),
        sd = ~sd(.)
      ),
      .names = "avg_{.col}_{.fn}"
    ),
    
    # Total costs
    avg_total_paid = mean(total_paid),
    sd_total_paid = sd(total_paid),
    avg_total_oop = mean(total_oop),
    sd_total_oop = sd(total_oop),
    
    # Visit-type specific costs
    across(
      ends_with("_paid"),
      list(
        avg = ~mean(., na.rm = TRUE)
      ),
      .names = "avg_{.col}"
    ),
    
    across(
      ends_with("_oop"),
      list(
        avg = ~mean(., na.rm = TRUE)
      ),
      .names = "avg_{.col}"
    ),
    .groups = 'drop'
  ) %>%
  arrange(Treat_Control_Cohort, post_period)

# Print results
print("Average Yearly Utilization and Costs per Member:")
print(summary_stats, n = Inf)

## D. Outliers----
### Initial identification----
# Summary statistics for each visit type
utilization_stats <- yearly_utilization %>%
  group_by(post_period) %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        min = ~min(.),
        q1 = ~quantile(., 0.25),
        median = ~median(.),
        mean = ~mean(.),
        q3 = ~quantile(., 0.75),
        max = ~max(.)
      ),
      .names = "{.col}_{.fn}"
    )
  )

# Calculate outlier thresholds (1.5 * IQR method)
outlier_thresholds <- yearly_utilization %>%
  group_by(post_period) %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        iqr = ~IQR(.),
        upper_threshold = ~quantile(., 0.75) + 1.5 * IQR(.),
        n_outliers = ~sum(. > (quantile(., 0.75) + 1.5 * IQR(.)))
      ),
      .names = "{.col}_{.fn}"
    )
  )

print("Distribution Statistics:")
print(utilization_stats)
print("\nOutlier Analysis:")
print(outlier_thresholds)

# Identify extreme outliers (> 3 IQR)
extreme_outliers <- yearly_utilization %>%
  group_by(post_period) %>%
  filter(
    n_eandem > quantile(n_eandem, 0.75) + 3 * IQR(n_eandem) |
      n_pt > quantile(n_pt, 0.75) + 3 * IQR(n_pt) |
      n_laboratory > quantile(n_laboratory, 0.75) + 3 * IQR(n_laboratory) |
      n_mental_health > quantile(n_mental_health, 0.75) + 3 * IQR(n_mental_health) |
      n_speech_therapy > quantile(n_speech_therapy, 0.75) + 3 * IQR(n_speech_therapy) |
      n_immunization > quantile(n_immunization, 0.75) + 3 * IQR(n_immunization) |
      n_preventive > quantile(n_preventive, 0.75) + 3 * IQR(n_preventive)
  ) %>%
  select(
    MVDID, Treat_Control_Cohort, post_period,
    n_eandem, n_pt, n_laboratory, n_mental_health, 
    n_speech_therapy, n_immunization, n_preventive
  )

print("\nExtreme Outliers (> 3 IQR):")
print(extreme_outliers)

### a. Examine Percentile Distributions ----
# Calculate 95th and 99th percentiles
percentile_dist <- yearly_utilization %>%
  group_by(post_period) %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        p95 = ~quantile(., 0.95),
        p99 = ~quantile(., 0.99)
      ),
      .names = "{.col}_{.fn}"
    )
  )

### b. Compare with Current Distribution ----
distribution_summary <- yearly_utilization %>%
  group_by(post_period) %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        mean = ~mean(.),
        median = ~median(.),
        max = ~max(.)
      ),
      .names = "{.col}_{.fn}"
    )
  )

### c. Count Cases Above Thresholds ----
cases_above_threshold <- yearly_utilization %>%
  group_by(post_period) %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        n_above_95 = ~sum(. > quantile(., 0.95)),
        n_above_99 = ~sum(. > quantile(., 0.99))
      ),
      .names = "{.col}_{.fn}"
    )
  )

print("95th and 99th Percentile Thresholds:")
print(percentile_dist)
print("\nCurrent Distribution Summary:")
print(distribution_summary)
print("\nNumber of Cases Above Thresholds:")
print(cases_above_threshold)

### d. Winsorize at 99th Percentile ----
# Calculate overall 99th percentile thresholds
thresholds <- yearly_utilization %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      ~as.integer(quantile(., 0.99)),
      .names = "{.col}_99"
    )
  )

print("Overall 99th percentile thresholds:")
print(thresholds)

# Create winsorized version
yearly_utilization_winsorized <- yearly_utilization %>%
  mutate(
    n_eandem_w = as.integer(pmin(n_eandem, thresholds$n_eandem_99)),
    n_pt_w = as.integer(pmin(n_pt, thresholds$n_pt_99)),
    n_laboratory_w = as.integer(pmin(n_laboratory, thresholds$n_laboratory_99)),
    n_mental_health_w = as.integer(pmin(n_mental_health, thresholds$n_mental_health_99)),
    n_speech_therapy_w = as.integer(pmin(n_speech_therapy, thresholds$n_speech_therapy_99)),
    n_immunization_w = as.integer(pmin(n_immunization, thresholds$n_immunization_99)),
    n_preventive_w = as.integer(pmin(n_preventive, thresholds$n_preventive_99))
  )

# Verify winsorization
winsorized_summary <- yearly_utilization_winsorized %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    across(
      c(n_eandem, n_pt, n_laboratory, n_mental_health, 
        n_speech_therapy, n_immunization, n_preventive),
      list(
        mean_orig = ~mean(.),
        max_orig = ~max(.)
      ),
      .names = "{.col}_{.fn}"
    ),
    across(
      c(n_eandem_w, n_pt_w, n_laboratory_w, n_mental_health_w, 
        n_speech_therapy_w, n_immunization_w, n_preventive_w),
      list(
        mean_w = ~mean(.),
        max_w = ~as.integer(max(.))
      ),
      .names = "{.col}_{.fn}"
    )
  )

print("\nSummary of Original vs Winsorized Distributions:")
print(winsorized_summary, width = Inf)


### e. Create Final Analytical Dataset ----
# Keep winsorized versions for analysis
final_analytical_dataset_w <- yearly_utilization_winsorized %>%
  select(
    # Member identifiers
    MVDID, SUBSCRIBERID,
    
    # Keep all original variables
    Treat_Control_Cohort, post_period, Plan_year,
    age_group, age_at_plan_year_start, family_size_bins, tot_comorbidities,
    comorbid_bins, PATIENTGENDER, deductible_level,
    DEDUCTIBLE_CATEGORY, HSA_Flag_final,
    PCP_copay, Specialist_copay, ED_copay, 
    
    # Include both original and winsorized utilization measures
    n_eandem, n_eandem_w,
    n_pt, n_pt_w,
    n_laboratory, n_laboratory_w,
    n_mental_health, n_mental_health_w,
    n_speech_therapy, n_speech_therapy_w,
    n_immunization, n_immunization_w,
    n_preventive, n_preventive_w,
    
    # Keep cost variables
    total_paid, total_oop,
    eandem_paid, eandem_oop,
    pt_paid, pt_oop,
    laboratory_paid, laboratory_oop,
    mental_health_paid, mental_health_oop,
    speech_therapy_paid, speech_therapy_oop,
    immunization_paid, immunization_oop,
    preventive_paid, preventive_oop
  )

# Convert variables to appropriate types
final_analytical_dataset_w <- final_analytical_dataset_w %>%
  mutate(
    # Convert to factors
    PATIENTGENDER = as.factor(PATIENTGENDER),
    deductible_level = as.factor(deductible_level),
    DEDUCTIBLE_CATEGORY = as.factor(DEDUCTIBLE_CATEGORY),
    HSA_Flag_final = as.factor(HSA_Flag_final),
    Treat_Control_Cohort = as.factor(Treat_Control_Cohort),
    post_period = as.factor(post_period),
    Plan_year = as.factor(Plan_year),
    age_group = as.factor(age_group),
    family_size_bins = as.factor(family_size_bins),
    comorbid_bins = as.factor(comorbid_bins)
  )

# Verify the structure of the final dataset
str(final_analytical_dataset_w)

# Summary statistics of the final dataset
summary_stats_final <- final_analytical_dataset_w %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    n_members = n(),
    
    # Utilization measures (both original and winsorized)
    across(
      c(starts_with("n_"), ends_with("_paid"), ends_with("_oop")),
      list(
        mean = ~mean(., na.rm = TRUE),
        sd = ~sd(., na.rm = TRUE)
      ),
      .names = "{.col}_{.fn}"
    ),
    .groups = 'drop'
  )

print("Final Dataset Summary Statistics:")
print(summary_stats_final, width = Inf)

saveRDS(final_analytical_dataset_w, "final_analytical_dataset_w.rds")
final_analytical_dataset_w <- readRDS("final_analytical_dataset_w.rds")

# 4. Individual Fixed Effects Analysis---- 
library(MASS)
library(margins)
library(ggplot2)
library(dplyr)
library(gridExtra)
library(fixest)


options(scipen = 999)
## A. DiD-Poisson ----
start.time <- Sys.time()

run_fe_model <- function(dependent_var, data, users_only = FALSE) {
  # Get base variable name (remove _w suffix)
  base_var <- sub("n_(.+)_w$", "\\1", dependent_var)
  
  # First ensure we have balanced panel
  data <- data %>%
    group_by(MVDID, Treat_Control_Cohort) %>%
    filter(n_distinct(post_period) == 2) %>%
    ungroup()
  
  if(users_only) {
    data <- data %>%
      group_by(MVDID) %>%
      filter(sum(!!sym(dependent_var)) > 0) %>%
      ungroup()
  }
  
  # Print cohort sizes before modeling
  cohort_summary <- data %>%
    group_by(Treat_Control_Cohort, post_period) %>%
    summarise(
      n_unique_members = n_distinct(MVDID),
      .groups = "drop"
    )
  
  cat("\nCohort sizes for", base_var, ":\n")
  print(cohort_summary)
  
  # Fit fixed effects Poisson model with robust standard errors
  tryCatch({
    model <- feglm(
      as.formula(paste(
        dependent_var, "~ Treat_Control_Cohort*post_period + 
        family_size_bins + comorbid_bins + 
        deductible_level + 
        HSA_Flag_final + Plan_year | MVDID"  # The | MVDID specifies individual FE
      )),
      data = data,
      family = "poisson",
      vcov = "hetero"  # Robust standard errors
    )
    
    # Calculate summary statistics
    util_summary <- data %>%
      group_by(Treat_Control_Cohort, post_period) %>%
      summarise(
        n_members = n_distinct(MVDID),
        mean_util = mean(!!sym(dependent_var)),
        sd_util = sd(!!sym(dependent_var)),
        n_with_any_util = sum(!!sym(dependent_var) > 0),
        util_per_1000 = mean(!!sym(dependent_var)) * 1000,
        .groups = "drop"
      )
    
    list(
      service_type = base_var,
      model = model,
      n_total_members = n_distinct(data$MVDID),
      utilization_summary = util_summary,
      balanced_cohort_summary = cohort_summary
    )
  }, error = function(e) {
    cat("\nError fitting model for", base_var, ":", e$message, "\n")
    return(NULL)
  })
}

# Define the service variables we want to focus on
service_vars <- c(
  "n_eandem_w",
  "n_pt_w",
  "n_laboratory_w",
  "n_mental_health_w"
)

# Run models for all members
cat("\nProcessing ALL members models:\n")
models_all <- lapply(service_vars, function(var_name) {
  cat("\nProcessing", sub("n_(.+)_w$", "\\1", var_name), "...\n")
  run_nb_model(var_name, final_analytical_dataset_w, users_only = FALSE)
})

# Run models for users only
cat("\nProcessing USERS ONLY models:\n")
models_users <- lapply(service_vars, function(var_name) {
  cat("\nProcessing", sub("n_(.+)_w$", "\\1", var_name), "...\n")
  run_nb_model(var_name, final_analytical_dataset_w, users_only = TRUE)
})

# Remove NULL results if any
models_all <- models_all[!sapply(models_all, is.null)]
models_users <- models_users[!sapply(models_users, is.null)]

# Name the results using the service_type field
names(models_all) <- sapply(models_all, function(x) x$service_type)
names(models_users) <- sapply(models_users, function(x) x$service_type)

# Print results for each service type
for(service in names(models_users)) {
  cat("\n\n================================================================")
  cat("\nResults for", service)
  cat("\n================================================================")
  
  cat("\n\nBALANCED COHORT SUMMARY:")
  cat("\n----------------\n")
  print(models_all[[service]]$balanced_cohort_summary)
  
  cat("\n\nALL MEMBERS ANALYSIS:")
  cat("\n----------------")
  cat("\nUtilization Summary:\n")
  print(models_all[[service]]$utilization_summary)
  cat("\nModel Summary:\n")
  print(summary(models_all[[service]]$model))
  
  cat("\n\nUSERS ONLY ANALYSIS:")
  cat("\n----------------")
  cat("\nUtilization Summary:\n")
  print(models_users[[service]]$utilization_summary)
  cat("\nModel Summary:\n")
  print(summary(models_users[[service]]$model))
  
  # Add comparison of key coefficients
  cat("\n\nKEY COEFFICIENTS COMPARISON:")
  cat("\n----------------")
  all_coef <- coef(models_all[[service]]$model)
  users_coef <- coef(models_users[[service]]$model)
  
  key_vars <- c("Treat_Control_Cohort1", "post_period1", "Treat_Control_Cohort1:post_period1")
  
  cat("\n                              All Members    Users Only")
  for(var in key_vars) {
    cat(sprintf("\n%-30s %10.3f %10.3f", var, all_coef[var], users_coef[var]))
  }
  cat("\n")
}

## B. Cluster Standard Erros @ SubscriberID----
library(sandwich)
library(lmtest)
library(dplyr)


# Define services first
services <- c("eandem", "pt", "laboratory", "mental_health")

# Now run your analysis code
results <- list()
for(service in services) {
  cat("\nProcessing", service, "...\n")
  
  # All members analysis
  all_members <- run_service_analysis(service, final_analytical_dataset_w)
  
  # Users only analysis
  users_only_data <- final_analytical_dataset_w %>%
    group_by(MVDID) %>%
    filter(sum(get(paste0("n_", service, "_w"))) > 0) %>%
    ungroup()
  users_only <- run_service_analysis(service, users_only_data)
  
  results[[service]] <- list(
    all_members = all_members,
    users_only = users_only
  )
  
  # Print results for this service
  print_results(service, results[[service]])
}

# Run this to save detailed results
# capture_detailed_results(results)

## C. Marginal Effects: Age and Sex----
library(margins)

# Relevel age_group with 50-64 as reference
final_analytical_dataset_w$age_group <- relevel(final_analytical_dataset_w$age_group, 
                                                ref = "18-34")

# Modified function with error handling
calculate_focused_marginal_effects <- function(model, data) {
  tryCatch({
    mfx <- margins(model, 
                   variables = c("age_group", "PATIENTGENDER", "comorbid_bins", "HSA_Flag_final"))
    mfx_summary <- summary(mfx)
    return(mfx_summary)
  }, error = function(e) {
    message("Error calculating marginal effects: ", e$message)
    return(NULL)
  })
}

# Run for each service type
for(service in services) {
  cat("\n\n================================================================")
  cat("\nMarginal Effects for", service)
  cat("\n================================================================\n")
  
  # All members analysis
  cat("\nALL MEMBERS ANALYSIS")
  cat("\n------------------\n")
  mfx_all <- calculate_focused_marginal_effects(results[[service]]$all_members$model, 
                                                final_analytical_dataset_w)
  print(mfx_all)
  
  # Users only analysis
  cat("\nUSERS ONLY ANALYSIS")
  cat("\n------------------\n")
  mfx_users <- calculate_focused_marginal_effects(results[[service]]$users_only$model, 
                                                  filter(final_analytical_dataset_w, 
                                                         get(paste0("n_", service, "_w")) > 0))
  print(mfx_users)
}



#5. First Differenced Models  ----
options(scipen = 999)

## 5a. Table 1----
library(gtsummary)
library(dplyr)
library(kableExtra)

# First, identify all treatment subjects
treatment_ids <- final_analytical_dataset_w %>%
  filter(Treat_Control_Cohort == 1) %>%
  select(MVDID) %>%
  distinct()

# Create a dataset with just treatment subjects and classify by year dyad
treatment_data <- final_analytical_dataset_w %>%
  filter(MVDID %in% treatment_ids$MVDID) %>%
  group_by(MVDID) %>%
  mutate(
    years_observed = paste(sort(unique(Plan_year)), collapse=", ")
  ) %>%
  ungroup()

# Create baseline (pre-period) dataset for treatment group
treatment_baseline <- treatment_data %>%
  filter(post_period == 0) %>%
  select(
    MVDID, years_observed,
    age_group, PATIENTGENDER, family_size_bins,
    comorbid_bins, tot_comorbidities, deductible_level, HSA_Flag_final,
    n_eandem_w, n_pt_w, n_laboratory_w, n_mental_health_w, total_oop
  )

# Calculate the changes in key metrics from pre to post period
treatment_changes <- treatment_data %>%
  group_by(MVDID) %>%
  arrange(post_period) %>%
  summarize(
    years_observed = first(years_observed),
    change_eandem = last(n_eandem_w) - first(n_eandem_w),
    change_pt = last(n_pt_w) - first(n_pt_w),
    change_lab = last(n_laboratory_w) - first(n_laboratory_w),
    change_mh = last(n_mental_health_w) - first(n_mental_health_w),
    change_oop = last(total_oop) - first(total_oop)
  ) %>%
  ungroup()

# Join baseline and change data
treatment_analysis <- treatment_baseline %>%
  left_join(treatment_changes, by = c("MVDID", "years_observed"))

# Define the main year dyads of interest and group others
treatment_analysis <- treatment_analysis %>%
  mutate(
    time_period = case_when(
      years_observed == "2018, 2019" ~ "2018-2019",
      years_observed == "2019, 2020" ~ "2019-2020",
      years_observed == "2020, 2021" ~ "2020-2021",
      years_observed == "2021, 2022" ~ "2021-2022",
      years_observed == "2022, 2023" ~ "2022-2023",
      TRUE ~ "Other Pairs"
    )
  )

# Create comprehensive Table 1 for treatment group by time period
table1_treatment <- treatment_analysis %>%
  select(
    time_period,
    # Baseline demographics
    age_group, PATIENTGENDER, family_size_bins,
    comorbid_bins, tot_comorbidities, 
    # Insurance characteristics
    deductible_level, HSA_Flag_final,
    # Baseline utilization
    n_eandem_w, n_pt_w, n_laboratory_w, n_mental_health_w, total_oop,
    # Change measures
    change_eandem, change_pt, change_lab, change_mh, change_oop
  ) %>%
  tbl_summary(
    by = time_period,
    missing = "no",
    label = list(
      age_group ~ "Age Group",
      PATIENTGENDER ~ "Gender",
      family_size_bins ~ "Family Size",
      comorbid_bins ~ "Comorbidity Category",
      tot_comorbidities ~ "Total Comorbidities",
      deductible_level ~ "Deductible Level",
      HSA_Flag_final ~ "HSA Flag",
      n_eandem_w ~ "Baseline E&M Visits",
      n_pt_w ~ "Baseline PT Visits",
      n_laboratory_w ~ "Baseline Lab Tests",
      n_mental_health_w ~ "Baseline Mental Health Visits",
      total_oop ~ "Baseline Out-of-Pocket Costs ($)",
      change_eandem ~ "Change in E&M Visits",
      change_pt ~ "Change in PT Visits",
      change_lab ~ "Change in Lab Tests",
      change_mh ~ "Change in Mental Health Visits",
      change_oop ~ "Change in Out-of-Pocket Costs ($)"
    ),
    statistic = list(
      all_continuous() ~ "{mean} ({sd})",
      all_categorical() ~ "{n} ({p}%)"
    )
  ) %>%
  add_n() %>%
  add_overall() %>%
  modify_header(label ~ "**Characteristic**") %>%
  modify_spanning_header(everything() ~ "Treatment Group Characteristics by Year Pair")

# Also create a summary of utilization changes for easier reference
changes_summary <- treatment_analysis %>%
  group_by(time_period) %>%
  summarize(
    n = n(),
    # E&M changes
    mean_change_eandem = mean(change_eandem, na.rm = TRUE),
    sd_change_eandem = sd(change_eandem, na.rm = TRUE),
    # PT changes
    mean_change_pt = mean(change_pt, na.rm = TRUE),
    sd_change_pt = sd(change_pt, na.rm = TRUE),
    # Lab changes
    mean_change_lab = mean(change_lab, na.rm = TRUE),
    sd_change_lab = sd(change_lab, na.rm = TRUE),
    # Mental health changes
    mean_change_mh = mean(change_mh, na.rm = TRUE),
    sd_change_mh = sd(change_mh, na.rm = TRUE),
    # OOP changes
    mean_change_oop = mean(change_oop, na.rm = TRUE),
    sd_change_oop = sd(change_oop, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(across(starts_with("mean_"), ~round(., 2))) %>%
  mutate(across(starts_with("sd_"), ~round(., 2)))

# Create a function to format the change values with SD
format_change <- function(mean_val, sd_val) {
  return(paste0(mean_val, " (", sd_val, ")"))
}

# Create a nicely formatted changes summary table
changes_table <- changes_summary %>%
  mutate(
    E_M_Change = format_change(mean_change_eandem, sd_change_eandem),
    PT_Change = format_change(mean_change_pt, sd_change_pt),
    Lab_Change = format_change(mean_change_lab, sd_change_lab),
    MH_Change = format_change(mean_change_mh, sd_change_mh),
    OOP_Change = format_change(mean_change_oop, sd_change_oop)
  ) %>%
  select(time_period, n, E_M_Change, PT_Change, Lab_Change, MH_Change, OOP_Change)

# Print the tables
print(table1_treatment)
print(changes_table)