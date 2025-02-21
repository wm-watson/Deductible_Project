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

end.time <- Sys.time()
print(paste("Total processing time:", end.time - start.time))

# Print summary
qualified_mvids <- nrow(mvdid_counts)
print(paste("Number of MVDIDs present in 2+ consecutive years:", qualified_mvids))
beep(8)

#### A1. Aggregate Only People----

final_one <- read_csv("final.csv", n_max = 50)
individual_one <- read_csv("individual.csv", n_max = 50)

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


### C. Treatment/Claims----
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
breaks <- c(0, 4, 12, 17, 34, 49, 64)
labels <- c("0-4", "5-12", "13-17", "18-34", "35-49", "50-64")

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

#### 4. Benefit Information----
library(tidyverse)
library(data.table)


# Column specifications
col_types_benefits <- cols(
  MVDID = col_character(),
  MEMBERID = col_character(),
  GROUP_NBR = col_character(),
  BENEFIT_PKG = col_character(),
  DEDUCTIBLE = col_character(),
  CP_AMT_T1 = col_character(),
  CP_AMT_T2 = col_character(),
  CP_AMT_T3 = col_character(),
  EFF_DATE = col_date(),
  TERM_DATE = col_date()
)

# Improved amount conversion function
convert_amount <- function(x, field_type) {
  numeric_value <- as.numeric(x)
  final_value <- numeric_value
  final_value[numeric_value == 999999999] <- 0
  
  if(field_type == "DEDUCTIBLE") {
    # Convert cents to dollars for deductibles
    cents_pattern <- final_value >= 100000 & final_value <= 5000000
    final_value[cents_pattern] <- final_value[cents_pattern] / 100
    # Cap unreasonable deductibles
    final_value[final_value > 50000] <- NA
  } else {
    # Copay conversion patterns
    conversion_rules <- list(
      CP_AMT_T1 = list(min = 1000, max = 5000, div = 100, cap = 50),
      CP_AMT_T2 = list(min = 1500, max = 7500, div = 100, cap = 75),
      CP_AMT_T3 = list(min = 2500, max = 10000, div = 100, cap = 500)
    )
    
    if(field_type %in% names(conversion_rules)) {
      rule <- conversion_rules[[field_type]]
      cents_pattern <- final_value >= rule$min & 
        final_value <= rule$max & 
        final_value %% (rule$div) == 0
      final_value[cents_pattern] <- final_value[cents_pattern] / 100
      final_value[final_value > rule$cap] <- NA
    }
  }
  return(final_value)
}

# Clean and combine files
clean_benefits <- function(file_path, platform) {
  # Read the raw data first to examine column names
  raw_data <- read_csv(file_path, col_types = col_types_benefits)
  
  # Find the correct column names
  group_nbr_col <- names(raw_data)[grep("GROUP_NBR", names(raw_data))][1]
  benefit_pkg_col <- names(raw_data)[grep("BENEFIT_PKG", names(raw_data))][1]
  
  raw_data %>%
    select(
      MVDID,
      MEMBERID,
      GROUP_NBR = !!sym(group_nbr_col),
      BENEFIT_PKG = !!sym(benefit_pkg_col),
      DEDUCTIBLE,
      PCP_COPAY = CP_AMT_T1,
      SPECIALIST_COPAY = CP_AMT_T2,
      ER_COPAY = CP_AMT_T3,
      EFF_DATE,
      TERM_DATE
    ) %>%
    mutate(
      DEDUCTIBLE = convert_amount(DEDUCTIBLE, "DEDUCTIBLE"),
      PCP_COPAY = convert_amount(PCP_COPAY, "CP_AMT_T1"),
      SPECIALIST_COPAY = convert_amount(SPECIALIST_COPAY, "CP_AMT_T2"),
      ER_COPAY = convert_amount(ER_COPAY, "CP_AMT_T3"),
      Platform = platform,
      Year = year(EFF_DATE)
    )
}

# Process files
message("Reading benefit files...")
combined_benefits <- bind_rows(
  clean_benefits("AMIBA_Ben_Pckgs.csv", "AMIBA"),
  clean_benefits("AMIHA_Ben_Pckgs.csv", "AMIHA")
) %>%
  # Remove redundant 9999-12-31 records
  group_by(MVDID) %>%
  filter(!(TERM_DATE == as.Date("9999-12-31") & 
             any(TERM_DATE < as.Date("9999-12-31")))) %>%
  ungroup() %>%
  # Add validation flags
  mutate(
    invalid_deductible = DEDUCTIBLE > 50000 | DEDUCTIBLE < 0,
    invalid_pcp = PCP_COPAY > 50 | PCP_COPAY < 0,
    invalid_specialist = SPECIALIST_COPAY > 75 | SPECIALIST_COPAY < 0,
    invalid_er = ER_COPAY > 500 | ER_COPAY < 0
  )

# Generate summary statistics
summary_stats <- combined_benefits %>%
  summarise(
    total_records = n(),
    unique_mvids = n_distinct(MVDID),
    year_range = paste(min(Year), "-", max(Year)),
    across(c(DEDUCTIBLE, PCP_COPAY, SPECIALIST_COPAY, ER_COPAY),
           list(
             median = ~median(., na.rm = TRUE),
             mean = ~mean(., na.rm = TRUE),
             missing = ~sum(is.na(.))
           ))
  )

# Display summary
print(summary_stats)

# Save cleaned data
fwrite(combined_benefits, "cleaned_benefits.csv")

# Display validation summary
message("\nValidation Summary:")
print(combined_benefits %>%
        summarise(
          invalid_records = sum(invalid_deductible | invalid_pcp | 
                                  invalid_specialist | invalid_er, na.rm = TRUE),
          invalid_deductible = sum(invalid_deductible, na.rm = TRUE),
          invalid_pcp = sum(invalid_pcp, na.rm = TRUE),
          invalid_specialist = sum(invalid_specialist, na.rm = TRUE),
          invalid_er = sum(invalid_er, na.rm = TRUE)
        ))
beep(8)

#### 5. Combine Benefits Info----

# Rename Year
clean_ben_hsa_mod <- clean_ben_hsa %>%
  rename(Plan_year = Year) %>%
  select(-MEMBERID) # Remove duplicate column

# Join on both MVDID and Plan_year
treatment_claims_final <- claims_elix_age_fam %>%
  left_join(clean_ben_hsa_mod,
            by = c("MVDID", "Plan_year"),
            suffix = c("", "_ben"))

### D. Control/Claims----
#### 1. Elixhauser Comorbidities ----
library(comorbidity)

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
breaks <- c(0, 4, 12, 17, 34, 49, 64)
labels <- c("0-4", "5-12", "13-17", "18-34", "35-49", "50-64")

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
clean_ben_hsa <- read_csv(paste0(filepath, "clean_ben_hsa.csv")) %>%
  rename(Plan_year = Year)  # Rename Year to Plan_year for joining

# Final Join
control_claims_final <- control_claims_elix_age_fam %>%
  left_join(clean_ben_hsa,
            by = c("MVDID", "Plan_year"),
            suffix = c("", "_ben"))

# ##### Final Dataset----
save(control_claims_final, file = paste0(filepath, "control_claims_final.RData"))

# Generate summary statistics
print("Control Group Summary Statistics:")
summary_stats <- control_claims_final %>%
  summarise(
    n_subscribers = n_distinct(SUBSCRIBERID),
    n_mvids = n_distinct(MVDID),
    avg_comorbidities = mean(tot_comorbidities, na.rm = TRUE),
    avg_family_size = mean(new_fam_size, na.rm = TRUE),
    pct_hsa = mean(hsa_ind, na.rm = TRUE) * 100
  )
print(summary_stats)

# 3. Prepare for DiD----
## A. Clean and standardize columns----

# Clean control claims
control_claims_clean <- control_claims_final %>%
  select(-matches("^\\.\\.\\.[0-9]+$"), -matches("MEMBERID_ben")) %>%
  # Keep all original columns for further variable construction
  select(
    everything(),
    -any_of(c("switch_year", "pre_switch_year", "switch_from", "switch_to", 
              "switch_type", "switch_details", "treatment", "period"))
  )

# Clean treatment claims
treatment_claims_clean <- treatment_claims_final %>%
  select(-matches("^\\.\\.\\.[0-9]+$"), -matches("MEMBERID_ben")) %>%
  # Keep treatment-specific columns
  select(everything())

## B. Set up diff-in-diff structure ----
# Create treatment indicators
control_claims_did <- control_claims_clean %>%
  group_by(MVDID) %>%
  mutate(
    switch_year = median(treatment_claims_clean$switch_year, na.rm = TRUE),
    pre_switch_year = switch_year - 1,
    period = case_when(
      Plan_year < switch_year ~ "Pre",
      Plan_year >= switch_year ~ "Post",
      TRUE ~ NA_character_
    ),
    switch_from = "No Switch",
    switch_to = "No Switch",
    switch_type = "Control",
    switch_details = "Control Group - No Switch",
    treatment = 0
  ) %>%
  ungroup()

treatment_claims_did <- treatment_claims_clean %>%
  mutate(
    period = case_when(
      Plan_year < switch_year ~ "Pre",
      Plan_year >= switch_year ~ "Post",
      TRUE ~ NA_character_
    ),
    treatment = 1
  )

## C. Combine datasets and create balanced window ----
diff_in_diff_data <- bind_rows(
  control_claims_did,
  treatment_claims_did
) %>%
  mutate(
    post = ifelse(period == "Post", 1, 0),
    treat_post = treatment * post,
    time_to_switch = Plan_year - switch_year
  ) %>%
  # Add balanced window calculations
  group_by(MVDID) %>%
  mutate(
    years_pre = sum(period == "Pre"),
    years_post = sum(period == "Post"),
    balanced_window = years_pre >= 1 & years_post >= 1  # At least 1 year pre and post
  ) %>%
  ungroup()

## D. Generate summary statistics ----
# Check balance before and after window restriction
balance_comparison <- diff_in_diff_data %>%
  group_by(treatment) %>%
  summarise(
    total_members = n_distinct(MVDID),
    balanced_members = n_distinct(MVDID[balanced_window == TRUE]),
    pct_balanced = balanced_members/total_members * 100,
    .groups = 'drop'
  )

# Summary stats for balanced sample
balanced_summary <- diff_in_diff_data %>%
  filter(balanced_window == TRUE) %>%
  group_by(treatment, period) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID),
    avg_paid = mean(PAIDAMOUNT, na.rm = TRUE),
    med_paid = median(PAIDAMOUNT, na.rm = TRUE),
    avg_deductible = mean(DEDUCTIBLE, na.rm = TRUE),
    avg_comorbidities = mean(tot_comorbidities, na.rm = TRUE),
    .groups = 'drop'
  )

# Print results
print("\nBalance Comparison Before/After Window Restriction:")
print(balance_comparison)

print("\nSummary Statistics for Balanced Sample:")
print(balanced_summary)

# Create final balanced dataset
balanced_did_data <- diff_in_diff_data %>%
  filter(balanced_window == TRUE)

# Save the balanced dataset
save(balanced_did_data, file = paste0(filepath, "balanced_did_data.RData"))

## E. Utilization categories----
# STEP 1: CREATE PROCEDURE CATEGORIES
procedure_categories <- balanced_did_data %>%
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
  group_by(treatment, period, procedure_category) %>%
  summarise(
    n_claims = n(),
    n_members = n_distinct(MVDID),
    total_paid = sum(PAIDAMOUNT, na.rm = TRUE),
    .groups = 'drop'
  ) %>%
  group_by(treatment, period) %>%
  mutate(
    pct_of_total = n_claims / sum(n_claims) * 100,
    claims_per_member = n_claims / n_members,
    avg_paid = total_paid / n_claims
  ) %>%
  arrange(treatment, period, -n_claims)

# STEP 3: CALCULATE CHANGES BETWEEN PERIODS
utilization_changes <- utilization_freq %>%
  select(treatment, period, procedure_category, n_claims, pct_of_total) %>%
  pivot_wider(
    names_from = period,
    values_from = c(n_claims, pct_of_total)
  ) %>%
  mutate(
    pct_change = ((n_claims_Post - n_claims_Pre) / n_claims_Pre) * 100,
    share_change = pct_of_total_Post - pct_of_total_Pre
  ) %>%
  arrange(treatment, -pct_change)

# STEP 4: PRINT RESULTS
# Control Group - Pre
print("\nControl Group - Pre Period:")
print(utilization_freq %>% 
        filter(treatment == 0, period == "Pre") %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Control Group - Post
print("\nControl Group - Post Period:")
print(utilization_freq %>% 
        filter(treatment == 0, period == "Post") %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Treatment Group - Pre
print("\nTreatment Group - Pre Period:")
print(utilization_freq %>% 
        filter(treatment == 1, period == "Pre") %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Treatment Group - Post
print("\nTreatment Group - Post Period:")
print(utilization_freq %>% 
        filter(treatment == 1, period == "Post") %>%
        select(procedure_category, n_claims, pct_of_total, claims_per_member, avg_paid) %>%
        arrange(-n_claims))

# Percentage Changes
print("\nPercentage Changes by Group:")
print(utilization_changes %>%
        select(treatment, procedure_category, pct_change, share_change) %>%
        arrange(treatment, -abs(pct_change)))

### e1. Analytical Dataset----
top_10_procedures <- procedure_categories %>%
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

print(top_10_procedures)

# Distribution by treatment group and period
top_10_by_treatment <- procedure_categories %>%
  filter(PROCEDURECODE %in% top_10_procedures$PROCEDURECODE) %>%
  group_by(PROCEDURECODE, procedure_category, treatment, period) %>%
  summarise(
    frequency = n(),
    avg_paid = mean(PAIDAMOUNT, na.rm = TRUE),
    n_members = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  arrange(PROCEDURECODE, treatment, period)

print(top_10_by_treatment)

# Average cost per member
top_10_member_costs <- procedure_categories %>%
  filter(PROCEDURECODE %in% top_10_procedures$PROCEDURECODE) %>%
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

print(top_10_member_costs)

## F. Analytical Dataset----
final_analytical_dataset <- balanced_did_data %>%
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
    # DiD variables
    treatment, post, treat_post,
    
    # Demographics
    age_group,                  
    family_size_bins,          
    comorbid_bins,             
    PATIENTGENDER,             
    
    # Plan characteristics
    DEDUCTIBLE_CATEGORY,       
    CP_AMT_T1,                 
    CP_AMT_T2,                 
    CP_AMT_T3,                 
    hsa_ind,                   
    
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

library(dplyr)
library(stringr)

# Step 1: Remove .x columns
final_analytical_dataset <- final_analytical_dataset %>%
  select(-matches("\\.x$"))

# Step 2: Remove .y suffix from remaining columns
colnames(final_analytical_dataset) <- gsub("\\.y$", "", colnames(final_analytical_dataset))

# Let's check what we have
str(final_analytical_dataset)
head(colnames(final_analytical_dataset))

# Check for any remaining duplicate columns
duplicated_cols <- colnames(final_analytical_dataset)[duplicated(colnames(final_analytical_dataset))]
print("Duplicated columns:")
print(duplicated_cols)

#### f2. Get Bens----

# Perform the merge
merged_data <- balanced_did_data %>%
  left_join(clean_ben_hsa,
            by = c("MVDID", "MEMBERID", "Plan_year"))

# Now let's check what columns we got
head(names(merged_data))

# The merge should have created .x and .y versions of duplicate columns
# Let's verify if we have the benefits columns with suffixes
benefits_cols <- grep("CP_AMT|DEDUCTIBLE|hsa_ind", names(merged_data), value = TRUE)
print("Benefits columns in merged data:")
print(benefits_cols)

# Now let's try to summarize
summary_stats <- merged_data %>%
  summarise(
    total_rows = n(),
    na_cp_t1_x = sum(is.na(`CP_AMT_T1.x`)),
    na_cp_t1_y = sum(is.na(`CP_AMT_T1.y`)),
    na_deductible_x = sum(is.na(`DEDUCTIBLE.x`)),
    na_deductible_y = sum(is.na(`DEDUCTIBLE.y`))
  )

print(summary_stats)

# 4. DiD Analysis---- 
library(MASS)
library(margins)
library(ggplot2)
library(dplyr)
library(gridExtra)

## A. Negative Binomial----
start.time <- Sys.time()

# Function to run negative binomial models
run_nb_model <- function(dependent_var, data) {
  formula <- as.formula(paste(
    dependent_var, "~ treatment*post + 
    age_group + family_size_bins + comorbid_bins + 
    PATIENTGENDER + DEDUCTIBLE_CATEGORY + 
    CP_AMT_T1 + CP_AMT_T2 + CP_AMT_T3 + 
    hsa_ind + factor(Plan_year)"
  ))
  
  model <- glm.nb(formula, data = data)
  return(model)
}

# Run all models
models <- list(
  pcp = run_nb_model("is_primary_care", final_analytical_dataset),
  pt = run_nb_model("is_physical_therapy", final_analytical_dataset),
  mh = run_nb_model("is_mental_health", final_analytical_dataset),
  lab = run_nb_model("is_diagnostic_lab", final_analytical_dataset)
)

# Print summaries
lapply(models, summary)

### a1. Cluster Standard Error----
# Function to run clustered standard errors
run_clustered_se <- function(model, cluster_var) {
  coeftest(model, vcov. = vcovCL(model, cluster = cluster_var, type = "HC0"))
}

# Calculate clustered SEs
clustered_results <- lapply(models, function(model) {
  run_clustered_se(model, final_analytical_dataset$SUBSCRIBER)
})

### b2. Margins----
# Function to calculate margins
calculate_margins <- function(model) {
  avg_slopes(model)
}

# Calculate margins for all models
margins_results <- lapply(models, calculate_margins)


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