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
load(paste0(filepath, "claims_elix_age_fam.RData"))

treat_claims <- claims_elix_age_fam

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

saveRDS(benefits_final_imputed, "benefits_final_impured.rds")

#### 5. Treat Claims w/Benefits----
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


### D. Control/Claims----
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
# Add control period imputations
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
      HSA_Flag_final == 1 ~ recommended_deductible,
      TRUE ~ 1000  # Keep original non-HSA deductible
    ),
    Treat_Control_Cohort = 0
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
# STEP 1: CREATE PROCEDURE CATEGORIES
procedure_categories <- combined_claims %>%
  mutate(
    procedure_category = case_when(
      # Anesthesia
      PROCEDURECODE >= '00100' & PROCEDURECODE <= '01999' ~ 'Anesthesia',
      
      # Surgery by section
      PROCEDURECODE >= '10021' & PROCEDURECODE <= '69990' ~ 'Surgery',
      
      # Radiology
      PROCEDURECODE >= '70010' & PROCEDURECODE <= '79999' ~ 'Radiology',
      
      # Pathology/Laboratory
      PROCEDURECODE >= '80047' & PROCEDURECODE <= '89398' ~ 'Laboratory/Pathology',
      
      # Medicine (excluding E&M)
      PROCEDURECODE >= '90281' & PROCEDURECODE <= '98943' ~ 'Medicine',
      
      # E&M Services
      PROCEDURECODE >= '99201' & PROCEDURECODE <= '99499' ~ 'E&M Services',
      
      # Category III codes
      PROCEDURECODE >= '0001T' & PROCEDURECODE <= '9999T' ~ 'Category III/Temporary',
      
      # HCPCS Level II codes
      substr(PROCEDURECODE, 1, 1) == 'A' ~ 'Medical Supplies/DME',
      substr(PROCEDURECODE, 1, 1) == 'B' ~ 'Enteral/Parenteral Therapy',
      substr(PROCEDURECODE, 1, 1) == 'C' ~ 'Temporary Hospital OP Services',
      substr(PROCEDURECODE, 1, 1) == 'D' ~ 'Dental Procedures',
      substr(PROCEDURECODE, 1, 1) == 'E' ~ 'Durable Medical Equipment',
      substr(PROCEDURECODE, 1, 1) == 'G' ~ 'Temporary Procedures/Services',
      substr(PROCEDURECODE, 1, 1) == 'H' ~ 'Rehabilitative Services',
      substr(PROCEDURECODE, 1, 1) == 'J' ~ 'Drugs/Biologicals',
      substr(PROCEDURECODE, 1, 1) == 'K' ~ 'Temporary Codes',
      substr(PROCEDURECODE, 1, 1) == 'L' ~ 'Orthotic/Prosthetic',
      substr(PROCEDURECODE, 1, 1) == 'M' ~ 'Medical Services',
      substr(PROCEDURECODE, 1, 1) == 'P' ~ 'Pathology/Laboratory',
      substr(PROCEDURECODE, 1, 1) == 'Q' ~ 'Temporary Codes',
      substr(PROCEDURECODE, 1, 1) == 'R' ~ 'Diagnostic Radiology',
      substr(PROCEDURECODE, 1, 1) == 'S' ~ 'Private Payer Codes',
      substr(PROCEDURECODE, 1, 1) == 'T' ~ 'State Medicaid Codes',
      substr(PROCEDURECODE, 1, 1) == 'V' ~ 'Vision/Hearing Services',
      TRUE ~ 'Other'
    )
  )

# STEP 2: ANALYZE FREQUENCIES BY GROUP AND PERIOD
utilization_freq <- procedure_categories %>%
  group_by(Treat_Control_Cohort, post_period, procedure_category) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID),
    total_paid = sum(PAIDAMOUNT, na.rm = TRUE),
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
utilization_changes <- utilization_freq %>%
  select(Treat_Control_Cohort, post_period, procedure_category, n_claims, pct_of_total) %>%
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
# Control Group - Pre
print("\nControl Group - Pre Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 0, post_period == 0) %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Control Group - Post
print("\nControl Group - Post Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 0, post_period == 1) %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Treatment Group - Pre
print("\nTreatment Group - Pre Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 1, post_period == 0) %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Treatment Group - Post
print("\nTreatment Group - Post Period:")
print(utilization_freq %>% 
        filter(Treat_Control_Cohort == 1, post_period == 1) %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Top 25 Procedures Analysis
top_25_procedures <- procedure_categories %>%
  group_by(PROCEDURECODE, procedure_category) %>%
  summarise(
    frequency = n(),
    total_paid = sum(PAIDAMOUNT, na.rm = TRUE),
    avg_paid = mean(PAIDAMOUNT, na.rm = TRUE),
    n_members = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  arrange(desc(frequency)) %>%
  head(25)

print("\nTop 25 Procedures:")
print(top_25_procedures)

# Distribution by treatment group and period
top_25_by_treatment <- procedure_categories %>%
  filter(PROCEDURECODE %in% top_25_procedures$PROCEDURECODE) %>%
  group_by(PROCEDURECODE, procedure_category, Treat_Control_Cohort, post_period) %>%
  summarise(
    frequency = n(),
    avg_paid = mean(PAIDAMOUNT, na.rm = TRUE),
    n_members = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  arrange(PROCEDURECODE, Treat_Control_Cohort, post_period)

print("\nTop 25 Procedures by Treatment Group and Period:")
print(top_25_by_treatment)

# Average cost per member
top_25_member_costs <- procedure_categories %>%
  filter(PROCEDURECODE %in% top_25_procedures$PROCEDURECODE) %>%
  group_by(PROCEDURECODE, MVDID) %>%
  summarise(
    visits_per_member = n(),
    avg_paid_per_member = mean(PAIDAMOUNT, na.rm = TRUE),
    .groups = 'drop'
  ) %>%
  group_by(PROCEDURECODE) %>%
  summarise(
    avg_visits_per_member = mean(visits_per_member),
    median_visits_per_member = median(visits_per_member),
    avg_cost_per_member = mean(avg_paid_per_member, na.rm = TRUE),
    .groups = 'drop'
  )

print("\nTop 25 Procedures - Member Cost Analysis:")
print(top_25_member_costs)

## B. Analytical Dataset----
final_analytical_dataset <- combined_claims %>%
  mutate(
    # Outcome Variables (Visit Types)
    is_primary_care = case_when(
      PROCEDURECODE %in% c('99214', '99213', '99203', '99204', '99396') ~ 1,
      TRUE ~ 0
    ),
    is_physical_therapy = case_when(
      PROCEDURECODE %in% c('97110', '97530', '97140', '97014', '97112', '97012', '98941') ~ 1,
      TRUE ~ 0
    ),
    is_mental_health = case_when(
      PROCEDURECODE %in% c('90837', '92507') ~ 1,
      TRUE ~ 0
    ),
    is_diagnostic_lab = case_when(
      PROCEDURECODE %in% c('85025', '80053', '80061', '80050', '83036', '80048', '84443', '82306', '36415', '96372') ~ 1,
      TRUE ~ 0
    )
  ) %>%
  select(
    # Member ID
    MVDID,
    
    # DiD variables
    Treat_Control_Cohort,
    post_period,
    
    # Demographics
    age_group,                  
    family_size_bins,          
    comorbid_bins,             
    PATIENTGENDER,             
    
    # Plan characteristics
    DEDUCTIBLE_CATEGORY,       
    PCP_copay,                 
    Specialist_copay,          
    ED_copay,                 
    HSA_Flag_final,                   
    
    # Visit Costs
    BILLEDAMOUNT,
    ALLOWEDAMOUNT,
    PAIDAMOUNT,
    COINSURANCEAMOUNT,
    COPAYAMOUNT,
    DEDUCTIBLEAMOUNT,
    
    # Plan Year
    Plan_year,
    
    # Outcome variables
    is_primary_care,
    is_physical_therapy,
    is_mental_health,
    is_diagnostic_lab
  )

# Verify the dataset
summary_stats <- final_analytical_dataset %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    n_claims = n(),
    n_primary_care = sum(is_primary_care),
    n_physical_therapy = sum(is_physical_therapy),
    n_mental_health = sum(is_mental_health),
    n_diagnostic_lab = sum(is_diagnostic_lab),
    avg_paid = mean(PAIDAMOUNT, na.rm = TRUE)
  )

print("Summary Statistics by Group and Period:")
print(summary_stats)

# Check visit type distributions
visit_dist <- final_analytical_dataset %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    pct_primary_care = mean(is_primary_care) * 100,
    pct_physical_therapy = mean(is_physical_therapy) * 100,
    pct_mental_health = mean(is_mental_health) * 100,
    pct_diagnostic_lab = mean(is_diagnostic_lab) * 100
  )

print("\nVisit Type Distribution (%):")
print(visit_dist)

## C. Yearly Utilization----
# Calculate yearly utilization and cost measures per member
yearly_utilization <- final_analytical_dataset %>%
  group_by(
    # ID variables
    Treat_Control_Cohort, post_period, Plan_year,
    
    # Keep explanatory variables for DiD
    age_group, family_size_bins, comorbid_bins, PATIENTGENDER,
    DEDUCTIBLE_CATEGORY, HSA_Flag_final,
    PCP_copay, Specialist_copay, ED_copay,
    
    # Group by MVDID to get member-level data
    MVDID
  ) %>%
  summarise(
    # Visit counts
    n_primary_care = sum(is_primary_care),
    n_physical_therapy = sum(is_physical_therapy),
    n_mental_health = sum(is_mental_health),
    n_diagnostic_lab = sum(is_diagnostic_lab),
    
    # Total yearly costs
    total_paid = sum(PAIDAMOUNT, na.rm = TRUE),
    total_oop = sum(COINSURANCEAMOUNT + COPAYAMOUNT + DEDUCTIBLEAMOUNT, na.rm = TRUE),
    
    # Visit-type specific costs
    primary_care_paid = sum(PAIDAMOUNT * (is_primary_care == 1), na.rm = TRUE),
    primary_care_oop = sum((COINSURANCEAMOUNT + COPAYAMOUNT + DEDUCTIBLEAMOUNT) * (is_primary_care == 1), na.rm = TRUE),
    
    phys_therapy_paid = sum(PAIDAMOUNT * (is_physical_therapy == 1), na.rm = TRUE),
    phys_therapy_oop = sum((COINSURANCEAMOUNT + COPAYAMOUNT + DEDUCTIBLEAMOUNT) * (is_physical_therapy == 1), na.rm = TRUE),
    
    mental_health_paid = sum(PAIDAMOUNT * (is_mental_health == 1), na.rm = TRUE),
    mental_health_oop = sum((COINSURANCEAMOUNT + COPAYAMOUNT + DEDUCTIBLEAMOUNT) * (is_mental_health == 1), na.rm = TRUE),
    
    diagnostic_lab_paid = sum(PAIDAMOUNT * (is_diagnostic_lab == 1), na.rm = TRUE),
    diagnostic_lab_oop = sum((COINSURANCEAMOUNT + COPAYAMOUNT + DEDUCTIBLEAMOUNT) * (is_diagnostic_lab == 1), na.rm = TRUE),
    .groups = 'drop'
  )

# Verify the aggregation
summary_stats <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    n_members = n_distinct(MVDID),
    # Visit counts
    avg_primary_care = mean(n_primary_care),
    avg_physical_therapy = mean(n_physical_therapy),
    avg_mental_health = mean(n_mental_health),
    avg_diagnostic_lab = mean(n_diagnostic_lab),
    # Total costs
    avg_total_paid = mean(total_paid),
    avg_total_oop = mean(total_oop),
    # Visit-type specific costs
    avg_primary_care_paid = mean(primary_care_paid),
    avg_primary_care_oop = mean(primary_care_oop),
    avg_phys_therapy_paid = mean(phys_therapy_paid),
    avg_phys_therapy_oop = mean(phys_therapy_oop),
    avg_mental_health_paid = mean(mental_health_paid),
    avg_mental_health_oop = mean(mental_health_oop),
    avg_diagnostic_lab_paid = mean(diagnostic_lab_paid),
    avg_diagnostic_lab_oop = mean(diagnostic_lab_oop)
  )

print("Average Yearly Utilization and Costs per Member:")
print(summary_stats)

## D. Outliers----
### Inital identification----
# Summary statistics and boxplot data for each visit type
utilization_stats <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    # Primary Care
    min_primary = min(n_primary_care),
    q1_primary = quantile(n_primary_care, 0.25),
    median_primary = median(n_primary_care),
    mean_primary = mean(n_primary_care),
    q3_primary = quantile(n_primary_care, 0.75),
    max_primary = max(n_primary_care),
    
    # Physical Therapy
    min_phys = min(n_physical_therapy),
    q1_phys = quantile(n_physical_therapy, 0.25),
    median_phys = median(n_physical_therapy),
    mean_phys = mean(n_physical_therapy),
    q3_phys = quantile(n_physical_therapy, 0.75),
    max_phys = max(n_physical_therapy),
    
    # Mental Health
    min_mental = min(n_mental_health),
    q1_mental = quantile(n_mental_health, 0.25),
    median_mental = median(n_mental_health),
    mean_mental = mean(n_mental_health),
    q3_mental = quantile(n_mental_health, 0.75),
    max_mental = max(n_mental_health),
    
    # Diagnostic Lab
    min_lab = min(n_diagnostic_lab),
    q1_lab = quantile(n_diagnostic_lab, 0.25),
    median_lab = median(n_diagnostic_lab),
    mean_lab = mean(n_diagnostic_lab),
    q3_lab = quantile(n_diagnostic_lab, 0.75),
    max_lab = max(n_diagnostic_lab)
  )

# Calculate outlier thresholds (1.5 * IQR method)
outlier_thresholds <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    # Primary Care
    iqr_primary = IQR(n_primary_care),
    upper_threshold_primary = quantile(n_primary_care, 0.75) + 1.5 * IQR(n_primary_care),
    n_outliers_primary = sum(n_primary_care > (quantile(n_primary_care, 0.75) + 1.5 * IQR(n_primary_care))),
    
    # Physical Therapy
    iqr_phys = IQR(n_physical_therapy),
    upper_threshold_phys = quantile(n_physical_therapy, 0.75) + 1.5 * IQR(n_physical_therapy),
    n_outliers_phys = sum(n_physical_therapy > (quantile(n_physical_therapy, 0.75) + 1.5 * IQR(n_physical_therapy))),
    
    # Mental Health
    iqr_mental = IQR(n_mental_health),
    upper_threshold_mental = quantile(n_mental_health, 0.75) + 1.5 * IQR(n_mental_health),
    n_outliers_mental = sum(n_mental_health > (quantile(n_mental_health, 0.75) + 1.5 * IQR(n_mental_health))),
    
    # Diagnostic Lab
    iqr_lab = IQR(n_diagnostic_lab),
    upper_threshold_lab = quantile(n_diagnostic_lab, 0.75) + 1.5 * IQR(n_diagnostic_lab),
    n_outliers_lab = sum(n_diagnostic_lab > (quantile(n_diagnostic_lab, 0.75) + 1.5 * IQR(n_diagnostic_lab)))
  )

print("Distribution Statistics:")
print(utilization_stats)
print("\nOutlier Analysis:")
print(outlier_thresholds)

# Identify extreme outliers (if any)
extreme_outliers <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  filter(
    n_primary_care > quantile(n_primary_care, 0.75) + 3 * IQR(n_primary_care) |
      n_physical_therapy > quantile(n_physical_therapy, 0.75) + 3 * IQR(n_physical_therapy) |
      n_mental_health > quantile(n_mental_health, 0.75) + 3 * IQR(n_mental_health) |
      n_diagnostic_lab > quantile(n_diagnostic_lab, 0.75) + 3 * IQR(n_diagnostic_lab)
  ) %>%
  select(
    MVDID, Treat_Control_Cohort, post_period,
    n_primary_care, n_physical_therapy, n_mental_health, n_diagnostic_lab
  )

print("\nExtreme Outliers (> 3 IQR):")
print(extreme_outliers)

### a. Examine Percentile Distributions ----
# Calculate 95th and 99th percentiles for each visit type
percentile_dist <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    # Primary Care
    p95_primary = quantile(n_primary_care, 0.95),
    p99_primary = quantile(n_primary_care, 0.99),
    
    # Physical Therapy
    p95_phys = quantile(n_physical_therapy, 0.95),
    p99_phys = quantile(n_physical_therapy, 0.99),
    
    # Mental Health
    p95_mental = quantile(n_mental_health, 0.95),
    p99_mental = quantile(n_mental_health, 0.99),
    
    # Diagnostic Lab
    p95_lab = quantile(n_diagnostic_lab, 0.95),
    p99_lab = quantile(n_diagnostic_lab, 0.99)
  )

### b. Compare with Current Distribution ----
distribution_summary <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    # Primary Care
    mean_primary = mean(n_primary_care),
    median_primary = median(n_primary_care),
    max_primary = max(n_primary_care),
    
    # Physical Therapy
    mean_phys = mean(n_physical_therapy),
    median_phys = median(n_physical_therapy),
    max_phys = max(n_physical_therapy),
    
    # Mental Health
    mean_mental = mean(n_mental_health),
    median_mental = median(n_mental_health),
    max_mental = max(n_mental_health),
    
    # Diagnostic Lab
    mean_lab = mean(n_diagnostic_lab),
    median_lab = median(n_diagnostic_lab),
    max_lab = max(n_diagnostic_lab)
  )

### c. Count Cases Above Thresholds ----
cases_above_threshold <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    # Primary Care
    n_above_95_primary = sum(n_primary_care > quantile(n_primary_care, 0.95)),
    n_above_99_primary = sum(n_primary_care > quantile(n_primary_care, 0.99)),
    
    # Physical Therapy
    n_above_95_phys = sum(n_physical_therapy > quantile(n_physical_therapy, 0.95)),
    n_above_99_phys = sum(n_physical_therapy > quantile(n_physical_therapy, 0.99)),
    
    # Mental Health
    n_above_95_mental = sum(n_mental_health > quantile(n_mental_health, 0.95)),
    n_above_99_mental = sum(n_mental_health > quantile(n_mental_health, 0.99)),
    
    # Diagnostic Lab
    n_above_95_lab = sum(n_diagnostic_lab > quantile(n_diagnostic_lab, 0.95)),
    n_above_99_lab = sum(n_diagnostic_lab > quantile(n_diagnostic_lab, 0.99))
  )

print("95th and 99th Percentile Thresholds:")
print(percentile_dist)

print("\nCurrent Distribution Summary:")
print(distribution_summary)

print("\nNumber of Cases Above Thresholds:")
print(cases_above_threshold)

### d. Winsorize at 99th Percentile ----
# Create winsorized version of yearly utilization
yearly_utilization_winsorized <- yearly_utilization %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  mutate(
    # Winsorize primary care visits
    n_primary_care_w = pmin(n_primary_care, 
                            quantile(n_primary_care, 0.99)),
    
    # Winsorize physical therapy visits
    n_physical_therapy_w = pmin(n_physical_therapy, 
                                quantile(n_physical_therapy, 0.99)),
    
    # Winsorize mental health visits
    n_mental_health_w = pmin(n_mental_health, 
                             quantile(n_mental_health, 0.99)),
    
    # Winsorize diagnostic lab visits
    n_diagnostic_lab_w = pmin(n_diagnostic_lab, 
                              quantile(n_diagnostic_lab, 0.99))
  ) %>%
  ungroup()

#### d.1.. Verify Winsorization ----
# Compare original vs winsorized distributions
winsorized_summary <- yearly_utilization_winsorized %>%
  group_by(Treat_Control_Cohort, post_period) %>%
  summarise(
    # Original means
    mean_primary_orig = mean(n_primary_care),
    mean_phys_orig = mean(n_physical_therapy),
    mean_mental_orig = mean(n_mental_health),
    mean_lab_orig = mean(n_diagnostic_lab),
    
    # Winsorized means
    mean_primary_w = mean(n_primary_care_w),
    mean_phys_w = mean(n_physical_therapy_w),
    mean_mental_w = mean(n_mental_health_w),
    mean_lab_w = mean(n_diagnostic_lab_w),
    
    # Original max values
    max_primary_orig = max(n_primary_care),
    max_phys_orig = max(n_physical_therapy),
    max_mental_orig = max(n_mental_health),
    max_lab_orig = max(n_diagnostic_lab),
    
    # Winsorized max values
    max_primary_w = max(n_primary_care_w),
    max_phys_w = max(n_physical_therapy_w),
    max_mental_w = max(n_mental_health_w),
    max_lab_w = max(n_diagnostic_lab_w)
  )

print("Summary of Original vs Winsorized Distributions:")
print(winsorized_summary)

### e. Create Final Analytical Dataset ----
# Keep winsorized versions for analysis
final_analytical_dataset_w <- yearly_utilization_winsorized %>%
  select(
    # Keep all original variables
    Treat_Control_Cohort, post_period, Plan_year,
    age_group, family_size_bins, comorbid_bins, PATIENTGENDER,
    DEDUCTIBLE_CATEGORY, HSA_Flag_final,
    PCP_copay, Specialist_copay, ED_copay,
    
    # Include both original and winsorized utilization measures
    n_primary_care, n_primary_care_w,
    n_physical_therapy, n_physical_therapy_w,
    n_mental_health, n_mental_health_w,
    n_diagnostic_lab, n_diagnostic_lab_w,
    
    # Keep cost variables
    total_paid, total_oop,
    primary_care_paid, primary_care_oop,
    phys_therapy_paid, phys_therapy_oop,
    mental_health_paid, mental_health_oop,
    diagnostic_lab_paid, diagnostic_lab_oop
  )

# 4. DiD Analysis---- 
library(MASS)
library(margins)
library(ggplot2)
library(dplyr)
library(gridExtra)

## A. Negative Binomial Models ----
start.time <- Sys.time()

# Function to run negative binomial models
run_nb_model <- function(dependent_var, data) {
  formula <- as.formula(paste(
    dependent_var, "~ Treat_Control_Cohort*post_period + 
    age_group + family_size_bins + comorbid_bins + 
    PATIENTGENDER + DEDUCTIBLE_CATEGORY + 
    PCP_copay + Specialist_copay + ED_copay + 
    HSA_Flag_final + factor(Plan_year)"
  ))
  
  model <- glm.nb(formula, data = data)
  return(model)
}

# Run models with winsorized variables
models_w <- list(
  pcp = run_nb_model("n_primary_care_w", final_analytical_dataset_w),
  pt = run_nb_model("n_physical_therapy_w", final_analytical_dataset_w),
  mh = run_nb_model("n_mental_health_w", final_analytical_dataset_w),
  lab = run_nb_model("n_diagnostic_lab_w", final_analytical_dataset_w)
)

# Print summaries
lapply(models_w, summary)

## B. Clustered Standard Errors ----
library(sandwich)
library(lmtest)

# Function to run clustered standard errors
run_clustered_se <- function(model, cluster_var) {
  coeftest(model, vcov. = vcovCL(model, cluster = cluster_var, type = "HC0"))
}

# Calculate clustered SEs
clustered_results_w <- lapply(models_w, function(model) {
  run_clustered_se(model, final_analytical_dataset_w$MVDID)
})

## C. Margins ----
# Function to calculate margins
calculate_margins <- function(model) {
  margins <- margins(model)
  summary(margins)
}

# Calculate margins for all models
margins_results_w <- lapply(models_w, calculate_margins)

### c1. Store Results ----
model_results_w <- list(
  models = models_w,
  clustered_se = clustered_results_w,
  margins = margins_results_w
)

end.time <- Sys.time()
time.taken <- end.time - start.time

print(paste("Time taken for analysis:", time.taken))

### c2. Extract Key Results ----
# Function to extract key coefficients and standard errors
extract_key_results <- function(model, clustered_se) {
  interaction_term <- coef(model)["Treat_Control_Cohort:post_period"]
  clustered_se_value <- clustered_se["Treat_Control_Cohort:post_period", "Std. Error"]
  p_value <- clustered_se["Treat_Control_Cohort:post_period", "Pr(>|z|)"]
  
  return(c(
    estimate = interaction_term,
    std_error = clustered_se_value,
    p_value = p_value
  ))
}

# Extract results for each model
key_results_w <- lapply(names(models_w), function(model_name) {
  extract_key_results(
    models_w[[model_name]], 
    clustered_results_w[[model_name]]
  )
})
names(key_results_w) <- names(models_w)

# Create summary table
results_summary_w <- do.call(rbind, key_results_w)
rownames(results_summary_w) <- c("Primary Care", "Physical Therapy", "Mental Health", "Diagnostic Lab")
print("\nKey DiD Results (Winsorized):")
print(results_summary_w)


## B. Hurdle Model----
# Hurdle Model Section
# Create binary indicator


winsor_99 <- final_analytical_dataset %>% 
  mutate(part1 = ifelse(tot_e_m <= 0, 0, 1))

### b1. Part 1----
# Logit model
hurdle_formula <- formula(
  "part1 ~ Embed_Deduc_Flag + AGE_BIN + SEX + FAM_BINS + 
   COMORBID_BINS + Plan_year + Embed_Deduc_Flag*AGE_BIN"
)

hurd_1 <- glm(hurdle_formula,
              family = binomial(link = 'logit'),
              data = winsor_99)

# Add predictions
winsor_99$part2 <- predict(hurd_1, newdata = winsor_99, type = 'response')

### b2. Part II----
# Second part of hurdle model
em_hurd <- glm.nb(tot_e_m ~ Embed_Deduc_Flag + AGE_BIN + SEX + 
                    FAM_BINS + COMORBID_BINS + Plan_year + 
                    Embed_Deduc_Flag*AGE_BIN + part2,
                  data = winsor_99,
                  link = log)

# Predictions and clustering for hurdle model
winsor_99$hurd_pred <- predict(em_hurd, winsor_99)
hurdle_cluster <- run_clustered_se(em_hurd, 
                                   winsor_99$SUBSCRIBER[!is.na(winsor_99$hurd_pred)])

# Hurdle margins
hurdle_margins <- calculate_margins(em_hurd)

# Subset Analysis
sub_winsor <- winsor_99 %>% filter(tot_e_m >= 1)

# Subset model
subset_formula <- formula(
  "tot_e_m ~ Embed_Deduc_Flag + AGE_BIN + SEX + FAM_BINS + 
   COMORBID_BINS + Plan_year + Embed_Deduc_Flag*AGE_BIN"
)

embed_neg_bi_em_sub <- glm.nb(subset_formula,
                              data = sub_winsor,
                              link = log)

# Subset clustering and margins
subset_cluster <- run_clustered_se(embed_neg_bi_em_sub, sub_winsor$SUBSCRIBER)
subset_margins <- calculate_margins(embed_neg_bi_em_sub)

# Plotting Function
create_prediction_plot <- function(model, title) {
  preds <- ggpredict(model, terms = c("AGE_BIN", "Embed_Deduc_Flag"))
  
  ggplot(preds, aes(x = x, y = predicted, color = group)) +
    geom_line() +
    geom_point() +
    geom_ribbon(aes(ymin = conf.low, ymax = conf.high, fill = group), alpha = 0.1) +
    labs(title = title,
         subtitle = "Based on a Negative Binomial GLM",
         x = "Age",
         y = "Predicted Count",
         color = "Deductible Type") +
    theme_minimal() +
    theme(text = element_text(size = 10, family = "Arial"),
          plot.title = element_text(face = "bold"),
          plot.subtitle = element_text(face = "italic"),
          legend.title = element_text(face = "bold"),
          legend.text = element_text(face = "plain"),
          axis.text.x = element_text(angle = 45, hjust = 1))
}

# Create plots
plots <- list(
  em = create_prediction_plot(embed_neg_bi_em_age, "E&M Visits"),
  lab = create_prediction_plot(embed_neg_bi_lab_age, "Lab Services"),
  pt = create_prediction_plot(embed_neg_bi_pt_age, "PT Visits")
)

# Print execution time
end.time <- Sys.time()
print(end.time - start.time)
beep(8)


# 5. Firm Level----
## A. Identify Firm-Level Cohorts----
start.time <- Sys.time()

# Filepath
filepath <- "R:/GraduateStudents/WatsonWilliamP/Deductible_Project/Deductible_Project/Data/"

# Define column names and types
col_names <- names(read_csv(paste0(filepath, "final.csv"), n_max = 1))
col_types <- cols(
  MVDID = col_character(),
  SUBSCRIBER = col_character(),  # Add SUBSCRIBER if not already present
  GROUP_NBR = col_character(),   # Add GROUP_NBR for firm identification
  Plan_year = col_double(),
  .default = col_guess()
)

# Create temporary file for firm-year combinations
write_csv(
  data.frame(GROUP_NBR = character(), Plan_year = numeric()), 
  paste0(filepath, "temp_firm_years.csv")
)

# Process chunks to get firm-year combinations with utilization metrics
process_firm_chunk <- function(x, pos) {
  chunk_firm_years <- x %>%
    mutate(Plan_year = as.numeric(Plan_year)) %>%
    group_by(GROUP_NBR, Plan_year) %>%
    summarise(
      n_members = n_distinct(MVDID),
      n_subscribers = n_distinct(SUBSCRIBER),
      total_claims = n(),
      .groups = "drop"
    ) %>%
    filter(total_claims > 0)  # Only include firms with claims
  
  write_csv(
    chunk_firm_years, 
    paste0(filepath, "temp_firm_years.csv"), 
    append = TRUE
  )
  
  print(paste("Processed chunk", pos))
  TRUE
}

# Read and process file in chunks
read_csv_chunked(
  paste0(filepath, "final.csv"),
  callback = process_firm_chunk,
  chunk_size = 1e6,
  col_names = col_names,
  col_types = col_types
)

# Process temporary file to identify firms present for at least 2 consecutive years
firm_cohorts <- read_csv(paste0(filepath, "temp_firm_years.csv")) %>%
  filter(!is.na(GROUP_NBR)) %>%
  distinct() %>%
  group_by(GROUP_NBR) %>%
  summarise(
    year_count = n_distinct(Plan_year),
    years = list(sort(unique(Plan_year))),
    avg_members = mean(n_members),
    avg_subscribers = mean(n_subscribers),
    avg_claims = mean(total_claims)
  ) %>%
  filter(year_count >= 2) %>%
  mutate(
    has_consecutive = map_lgl(years, function(x) {
      sorted_years <- sort(unique(x))
      any(diff(sorted_years) == 1)
    })
  ) %>%
  filter(has_consecutive)

# Create empty data frame with correct column names
empty_df <- as.data.frame(matrix(ncol = length(col_names), nrow = 0))
names(empty_df) <- col_names

# Create output file with headers
write_csv(empty_df, paste0(filepath, "firm_level.csv"))

# Process chunks again to save filtered data
process_filtered_firm_chunk <- function(x, pos) {
  filtered_chunk <- x %>%
    inner_join(
      firm_cohorts %>% select(GROUP_NBR),
      by = "GROUP_NBR"
    )
  
  write_csv(
    filtered_chunk, 
    paste0(filepath, "firm_level.csv"), 
    append = TRUE
  )
  
  print(paste("Processed filtered chunk", pos))
  TRUE
}

# Read and process file in chunks again for filtering
read_csv_chunked(
  paste0(filepath, "final.csv"),
  callback = process_filtered_firm_chunk,
  chunk_size = 1e6,
  col_names = col_names,
  col_types = col_types
)

# Generate summary statistics
firm_summary <- firm_cohorts %>%
  summarise(
    n_firms = n(),
    avg_firm_size = mean(avg_members),
    median_firm_size = median(avg_members),
    avg_years_present = mean(year_count),
    total_subscribers = sum(avg_subscribers)
  )

# Print summary statistics
print("Firm-Level Cohort Summary:")
print(firm_summary)

# Generate year-by-year firm statistics
firm_yearly_stats <- read_csv(paste0(filepath, "temp_firm_years.csv")) %>%
  inner_join(firm_cohorts %>% select(GROUP_NBR), by = "GROUP_NBR") %>%
  group_by(Plan_year) %>%
  summarise(
    n_firms = n_distinct(GROUP_NBR),
    total_members = sum(n_members),
    total_subscribers = sum(n_subscribers),
    total_claims = sum(total_claims),
    avg_members_per_firm = mean(n_members),
    avg_claims_per_firm = mean(total_claims)
  )

print("\nYearly Statistics:")
print(firm_yearly_stats)

# Clean up temporary file
unlink(paste0(filepath, "temp_firm_years.csv"))

end.time <- Sys.time()
print(paste("\nTotal processing time:", end.time - start.time))
beep(8)

## B. Firm-Level Deductible Switches----
filepath <- "R:/GraduateStudents/WatsonWilliamP/Deductible_Project/Deductible_Project/Data/"

library(readr)
library(data.table)
library(dplyr)

start.time <- Sys.time()

# Create temporary file for firm patterns with subscriber-based metrics
temp_patterns <- data.frame(
  GROUP_NBR = character(),
  Plan_year = numeric(),
  deductible_types = character(),
  n_subscribers = numeric(),  # Primary metric for firm size
  n_total_members = numeric(), # Secondary metric
  total_claims = numeric()
)
write_csv(temp_patterns, paste0(filepath, "temp_firm_patterns.csv"))

# Define column types
col_types <- cols(
  GROUP_NBR = col_character(),
  MVDID = col_character(),
  SUBSCRIBERID = col_character(),
  Plan_year = col_double(),
  deductible_types = col_character(),
  PAIDAMOUNT = col_double(),
  .default = col_guess()
)

# Process chunks to get firm-level patterns based on subscribers
process_firm_chunk <- function(x, pos) {
  chunk_patterns <- x %>%
    filter(deductible_types %in% c("Aggregate Only", "Embedded Only")) %>%
    group_by(GROUP_NBR, Plan_year, deductible_types) %>%
    summarize(
      n_subscribers = n_distinct(SUBSCRIBERID),  # Primary size metric
      n_total_members = n_distinct(MVDID),       # Secondary metric
      total_claims = n(),
      total_paid = sum(PAIDAMOUNT, na.rm = TRUE),
      avg_members_per_subscriber = n_distinct(MVDID) / n_distinct(SUBSCRIBERID),
      .groups = 'drop'
    ) %>%
    # Determine dominant deductible type for each firm-year
    group_by(GROUP_NBR, Plan_year) %>%
    slice_max(n_subscribers, n = 1) %>%  # Use subscriber count for dominance
    ungroup()
  
  write_csv(chunk_patterns, paste0(filepath, "temp_firm_patterns.csv"), append = TRUE)
  
  print(paste("Processed chunk", pos))
  TRUE
}

read_csv_chunked(paste0(filepath, "firm_level.csv"),
                 callback = process_firm_chunk,
                 chunk_size = 1e6,
                 col_types = col_types)

# Identify control firms (Aggregate-only that never switch)
aggregate_only_control <- read_csv(paste0(filepath, "temp_firm_patterns.csv")) %>%
  group_by(GROUP_NBR) %>%
  summarize(
    n_years = n_distinct(Plan_year),
    all_types = list(unique(deductible_types)),
    avg_subscribers = mean(n_subscribers),        # Primary size metric
    avg_total_members = mean(n_total_members),    # Secondary metric
    avg_members_per_subscriber = mean(avg_members_per_subscriber),
    avg_claims = mean(total_claims),
    total_paid = sum(total_paid),
    .groups = 'drop'
  ) %>%
  filter(
    n_years >= 2,  # Must have at least 2 years of data
    map_lgl(all_types, function(x) length(x) == 1 && x == "Aggregate Only")  # Only ever Aggregate
  )

# Save the control group firms
write_csv(aggregate_only_control, paste0(filepath, "aggregate_only_control_firms.csv"))

# Process patterns for switching firms
firm_switches <- read_csv(paste0(filepath, "temp_firm_patterns.csv")) %>%
  group_by(GROUP_NBR) %>%
  arrange(Plan_year) %>%
  mutate(
    next_year = lead(Plan_year),
    next_year_type = lead(deductible_types),
    prev_year = lag(Plan_year),
    prev_year_type = lag(deductible_types),
    
    # Identify valid switches
    is_switch = (deductible_types != next_year_type & 
                   next_year == Plan_year + 1),
    
    # Calculate firm stability based on subscribers
    size_change = abs(n_subscribers - lag(n_subscribers)) / lag(n_subscribers),
    stable_size = is.na(size_change) | size_change <= 0.2  # Allow 20% variation
  ) %>%
  filter(is_switch & stable_size) %>%
  summarize(
    switch_year = min(Plan_year[is_switch == TRUE]) + 1,
    pre_switch_year = min(Plan_year[is_switch == TRUE]),
    switch_from = first(deductible_types[is_switch == TRUE]),
    switch_to = first(next_year_type[is_switch == TRUE]),
    avg_subscribers = mean(n_subscribers),        # Primary size metric
    avg_total_members = mean(n_total_members),    # Secondary metric
    avg_members_per_subscriber = mean(avg_members_per_subscriber),
    total_claims = sum(total_claims),
    total_paid = sum(total_paid),
    .groups = 'drop'
  ) %>%
  mutate(
    switch_type = paste(switch_from, "to", switch_to),
    size_category = case_when(
      avg_subscribers < 25 ~ "Small",      # Adjusted thresholds for subscriber counts
      avg_subscribers < 100 ~ "Medium",
      TRUE ~ "Large"
    )
  )

# Generate treatment and control groups
treatment_firms <- firm_switches %>%
  filter(switch_from == "Aggregate Only" & switch_to == "Embedded Only")

# Match control firms based on subscriber count and claims
matched_control_firms <- aggregate_only_control %>%
  mutate(
    size_category = case_when(
      avg_subscribers < 25 ~ "Small",      # Matching thresholds
      avg_subscribers < 100 ~ "Medium",
      TRUE ~ "Large"
    )
  ) %>%
  inner_join(
    treatment_firms %>% 
      group_by(size_category) %>%
      summarize(n_needed = n(), .groups = 'drop'),
    by = "size_category"
  ) %>%
  group_by(size_category) %>%
  slice_sample(n = first(n_needed)) %>%
  ungroup()

# Save treatment and control groups
write_csv(treatment_firms, paste0(filepath, "treatment_firms.csv"))
write_csv(matched_control_firms, paste0(filepath, "matched_control_firms.csv"))

# Generate summary statistics
print("\nSwitch Direction Summary:")
switch_summary <- firm_switches %>%
  group_by(switch_type, size_category) %>%
  summarise(
    n_firms = n(),
    avg_subscribers = mean(avg_subscribers),
    avg_members_per_subscriber = mean(avg_members_per_subscriber),
    total_claims = sum(total_claims),
    total_paid = sum(total_paid),
    .groups = 'drop'
  )
print(switch_summary)

# Print matching summary
print("\nMatching Summary:")
matching_summary <- bind_rows(
  treatment_firms %>% mutate(group = "Treatment"),
  matched_control_firms %>% mutate(group = "Control")
) %>%
  group_by(group, size_category) %>%
  summarise(
    n_firms = n(),
    avg_subscribers = mean(avg_subscribers),
    avg_members_per_subscriber = mean(avg_members_per_subscriber),
    total_claims = sum(total_claims),
    .groups = 'drop'
  )
print(matching_summary)

end.time <- Sys.time()
print(paste("Total processing time:", end.time - start.time))
beep(8)

## C. Treat/Control Claims----
library(lubridate)
library(tidyverse)
library(beepr)

start.time <- Sys.time()

# Define column types
col_types <- cols(
  GROUP_NBR = col_character(),
  MVDID = col_character(),
  SUBSCRIBERID = col_character(),
  Plan_year = col_double(),
  PAIDAMOUNT = col_double(),
  .default = col_guess()
)

process_chunk <- function(chunk, treatment_firms, control_firms) {
  processed <- chunk %>%
    mutate(Plan_year = as.numeric(Plan_year)) %>%
    # First filter to only treatment/control firms
    inner_join(
      bind_rows(
        treatment_firms %>% mutate(group_type = "treatment"),
        control_firms %>% mutate(group_type = "control")
      ),
      by = "GROUP_NBR"
    ) %>%
    mutate(
      period = case_when(
        group_type == "treatment" & Plan_year == pre_switch_year ~ "Pre-Switch",
        group_type == "treatment" & Plan_year == switch_year ~ "Switch Year",
        group_type == "control" ~ "Control",
        TRUE ~ "Other"
      )
    ) %>%
    filter(period != "Other")
  
  return(processed)
}

get_claims_chunked <- function(filepath, treatment_firms, control_firms, chunk_size = 1e6) {
  claims_analyzed <- tibble()
  
  callback <- function(chunk, pos) {
    processed_chunk <- process_chunk(chunk, treatment_firms, control_firms)
    
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

# Get claims for individuals in treatment/control firms
claims_data <- get_claims_chunked(filepath, treatment_firms, matched_control_firms)

#### 1. Elixhauser Comorbidities----
# Calculate Elixhauser scores by individual and year
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

# Calculate total comorbidities and create bins
elix_scores <- elix %>%
  mutate(
    tot_comorbidities = rowSums(select(., -MVDID, -Plan_year))
  ) %>%
  select(MVDID, Plan_year, tot_comorbidities)

claims_with_elix <- claims_data %>%
  left_join(elix_scores, by = c("MVDID", "Plan_year")) %>%
  mutate(
    tot_comorbidities = coalesce(tot_comorbidities, 0),
    comorbid_bins = cut(tot_comorbidities, 
                        breaks = c(0, 1, 2, 3, Inf),
                        labels = c("0", "1", "2", "3+"),
                        include.lowest = TRUE,
                        right = FALSE)
  )

#### 2. Age Bins----
claims_with_elix_age <- claims_with_elix %>%
  mutate(
    age_group = cut(age_at_plan_year_start, 
                    breaks = c(0, 4, 12, 17, 34, 49, 64),
                    labels = c("0-4", "5-12", "13-17", "18-34", "35-49", "50-64"),
                    include.lowest = TRUE,
                    right = FALSE)
  )

#### 3. Family Size----
claims_elix_age_fam <- claims_with_elix_age %>%
  # Calculate family size by SUBSCRIBERID and year
  group_by(SUBSCRIBERID, Plan_year) %>%
  mutate(
    family_size = n_distinct(MVDID)
  ) %>%
  ungroup() %>%
  # Create family size bins
  mutate(
    family_size_bins = cut(family_size,
                           breaks = c(0, 2, 3, 4, 5, Inf),
                           labels = c("2", "3", "4", "5", "6+"),
                           include.lowest = TRUE,
                           right = FALSE)
  )

# Add treatment/control group characteristics
final_analytical_dataset <- claims_elix_age_fam %>%
  mutate(
    treatment = group_type == "treatment",
    post = period == "Switch Year"
  )

# Generate summary statistics
member_summary <- final_analytical_dataset %>%
  group_by(group_type, period) %>%
  summarise(
    n_members = n_distinct(MVDID),
    n_subscribers = n_distinct(SUBSCRIBERID),
    n_firms = n_distinct(GROUP_NBR),
    avg_age = mean(age_at_plan_year_start),
    avg_comorbidities = mean(tot_comorbidities),
    avg_family_size = mean(family_size),
    .groups = "drop"
  )

print("Member-Level Summary Statistics:")
print(member_summary)

# Save processed data
save(final_analytical_dataset, 
     file = paste0(filepath, "final_analytical_dataset.RData"))

end.time <- Sys.time()
print(paste("Total processing time:", end.time - start.time))
beep(8)