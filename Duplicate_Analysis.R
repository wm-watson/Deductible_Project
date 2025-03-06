# Analyze claim numbers vs claim lines
claim_line_analysis <- combined_claims %>%
  group_by(MVDID, SERVICEFROMDATE, procedure_category, CLAIMNUMBER) %>%
  summarise(
    lines_per_claim = n(),
    unique_procedures = n_distinct(PROCEDURECODE),
    total_billed = sum(BILLEDAMOUNT, na.rm = TRUE),
    procedures = paste(unique(PROCEDURECODE), collapse = ", "),
    .groups = 'drop'
  ) %>%
  group_by(MVDID, SERVICEFROMDATE, procedure_category) %>%
  summarise(
    total_lines = sum(lines_per_claim),
    unique_claims = n_distinct(CLAIMNUMBER),
    avg_lines_per_claim = mean(lines_per_claim),
    total_billed = sum(total_billed),
    .groups = 'drop'
  ) %>%
  filter(total_lines > 1) %>%
  arrange(desc(total_lines))

print("\nAnalysis of claim lines vs unique claims:")
print(head(claim_line_analysis, 10))

# Look at the extreme cases we found earlier with claim numbers
extreme_case_detail <- combined_claims %>%
  filter(MVDID == "161C17DED4F0D8259CC4", 
         SERVICEFROMDATE == as.Date("2023-12-19")) %>%
  group_by(CLAIMNUMBER) %>%
  summarise(
    lines = n(),
    unique_procedures = n_distinct(PROCEDURECODE),
    procedures = paste(unique(PROCEDURECODE), collapse = ", "),
    total_billed = sum(BILLEDAMOUNT, na.rm = TRUE),
    .groups = 'drop'
  )

print("\nDetailed look at extreme case (655 lines):")
print(extreme_case_detail)

# Summary of lines per claim by procedure category
procedure_claim_summary <- combined_claims %>%
  group_by(procedure_category, CLAIMNUMBER) %>%
  summarise(
    lines_per_claim = n(),
    unique_procedures = n_distinct(PROCEDURECODE),
    .groups = 'drop'
  ) %>%
  group_by(procedure_category) %>%
  summarise(
    total_claims = n_distinct(CLAIMNUMBER),
    avg_lines_per_claim = mean(lines_per_claim),
    max_lines_per_claim = max(lines_per_claim),
    pct_multi_line = mean(lines_per_claim > 1) * 100,
    .groups = 'drop'
  ) %>%
  arrange(desc(avg_lines_per_claim))

print("\nSummary of lines per claim by procedure category:")
print(procedure_claim_summary)

# Identify potential true duplicates (same claim details on different claim numbers)
potential_true_duplicates <- combined_claims %>%
  group_by(MVDID, SERVICEFROMDATE, PROCEDURECODE, BILLEDAMOUNT, ALLOWEDAMOUNT) %>%
  summarise(
    claim_count = n_distinct(CLAIMNUMBER),
    claim_numbers = paste(unique(CLAIMNUMBER), collapse = ", "),
    .groups = 'drop'
  ) %>%
  filter(claim_count > 1) %>%
  arrange(desc(claim_count))

print("\nPotential true duplicates (same details, different claim numbers):")
print(head(potential_true_duplicates))

# Analyze the suspicious duplicate case
suspicious_case <- combined_claims %>%
  filter(MVDID == "16B8874FF4710B3F8B4D",
         PROCEDURECODE == "92507",
         SERVICEFROMDATE >= as.Date("2021-03-17"),
         SERVICEFROMDATE <= as.Date("2021-03-31")) %>%
  arrange(SERVICEFROMDATE, CLAIMNUMBER) %>%
  dplyr::select(SERVICEFROMDATE, CLAIMNUMBER, PROCEDURECODE, BILLEDAMOUNT, 
                ALLOWEDAMOUNT, PLACEOFSERVICE)

print("\nDetailed look at suspicious duplicate case:")
print(suspicious_case)

# Look for patterns in timing of duplicate submissions
duplicate_timing <- combined_claims %>%
  group_by(MVDID, SERVICEFROMDATE, PROCEDURECODE, BILLEDAMOUNT, ALLOWEDAMOUNT) %>%
  filter(n_distinct(CLAIMNUMBER) > 1) %>%
  arrange(MVDID, SERVICEFROMDATE, CLAIMNUMBER) %>%
  dplyr::select(MVDID, SERVICEFROMDATE, CLAIMNUMBER, PROCEDURECODE, 
                BILLEDAMOUNT, ALLOWEDAMOUNT, PLACEOFSERVICE)

print("\nPatterns in duplicate submissions:")
print(head(duplicate_timing, 20))

# Add summary of duplicate patterns
duplicate_summary <- duplicate_timing %>%
  group_by(MVDID, SERVICEFROMDATE, PROCEDURECODE) %>%
  summarise(
    claim_count = n_distinct(CLAIMNUMBER),
    total_billed = sum(BILLEDAMOUNT),
    unique_places = n_distinct(PLACEOFSERVICE),
    .groups = 'drop'
  ) %>%
  arrange(desc(claim_count))

print("\nSummary of duplicate patterns:")
print(head(duplicate_summary))

# Analyze claim submission timing
claim_timing <- combined_claims %>%
  filter(MVDID == "16B8874FF4710B3F8B4D",
         SERVICEFROMDATE >= as.Date("2021-03-17"),
         SERVICEFROMDATE <= as.Date("2021-03-31")) %>%
  mutate(claim_month = substr(CLAIMNUMBER, 1, 4)) %>%
  group_by(SERVICEFROMDATE, PROCEDURECODE, claim_month) %>%
  summarise(
    claim_count = n(),
    total_billed = sum(BILLEDAMOUNT),
    .groups = 'drop'
  ) %>%
  arrange(SERVICEFROMDATE, PROCEDURECODE, claim_month)

# Look for providers with frequent duplicates
provider_duplicates <- combined_claims %>%
  group_by(MVDID, SERVICEFROMDATE, PROCEDURECODE) %>%
  filter(n_distinct(CLAIMNUMBER) > 1) %>%
  group_by(PLACEOFSERVICE) %>%
  summarise(
    duplicate_instances = n(),
    unique_patients = n_distinct(MVDID),
    total_billed = sum(BILLEDAMOUNT),
    .groups = 'drop'
  ) %>%
  arrange(desc(duplicate_instances))
print(provider_duplicates)
print(claim_timing)

# Analyze time lag between service and claim submission
claim_lag_analysis <- combined_claims %>%
  filter(n_distinct(CLAIMNUMBER) > 1) %>%
  mutate(
    claim_year_month = substr(CLAIMNUMBER, 1, 4),
    submission_lag = as.numeric(difftime(as.Date(paste0(claim_year_month, "01"), format="%y%m%d"), 
                                         SERVICEFROMDATE, 
                                         units="days"))
  ) %>%
  group_by(PLACEOFSERVICE) %>%
  summarise(
    avg_submission_lag = mean(submission_lag, na.rm=TRUE),
    max_submission_lag = max(submission_lag, na.rm=TRUE),
    min_submission_lag = min(submission_lag, na.rm=TRUE),
    claim_count = n(),
    .groups = 'drop'
  ) %>%
  arrange(desc(claim_count))

# Look at procedures with highest duplicate rates
procedure_duplicate_rates <- combined_claims %>%
  group_by(PROCEDURECODE) %>%
  summarise(
    total_claims = n(),
    duplicate_claims = sum(n_distinct(CLAIMNUMBER) > 1),
    total_billed = sum(BILLEDAMOUNT),
    unique_patients = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  filter(total_claims > 100) %>%  # Filter for frequently used procedures
  mutate(duplicate_rate = duplicate_claims/total_claims) %>%
  arrange(desc(duplicate_rate))

# Analyze time lag between service and claim submission
claim_lag_analysis <- combined_claims %>%
  filter(n_distinct(CLAIMNUMBER) > 1) %>%
  mutate(
    claim_year_month = substr(CLAIMNUMBER, 1, 4),
    submission_lag = as.numeric(difftime(as.Date(paste0(claim_year_month, "01"), format="%y%m%d"), 
                                         SERVICEFROMDATE, 
                                         units="days"))
  ) %>%
  group_by(PLACEOFSERVICE) %>%
  summarise(
    avg_submission_lag = mean(submission_lag, na.rm=TRUE),
    max_submission_lag = max(submission_lag, na.rm=TRUE),
    min_submission_lag = min(submission_lag, na.rm=TRUE),
    claim_count = n(),
    .groups = 'drop'
  ) %>%
  arrange(desc(claim_count))

# Look at procedures with highest duplicate rates
procedure_duplicate_rates <- combined_claims %>%
  group_by(PROCEDURECODE) %>%
  summarise(
    total_claims = n(),
    duplicate_claims = sum(n_distinct(CLAIMNUMBER) > 1),
    total_billed = sum(BILLEDAMOUNT),
    unique_patients = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  filter(total_claims > 100) %>%  # Filter for frequently used procedures
  mutate(duplicate_rate = duplicate_claims/total_claims) %>%
  arrange(desc(duplicate_rate))

print(claim_lag_analysis)
print(procedure_duplicate_rates)

# Analyze claims with negative submission lags
negative_lag_analysis <- combined_claims %>%
  mutate(
    claim_year_month = substr(CLAIMNUMBER, 1, 4),
    submission_lag = as.numeric(difftime(as.Date(paste0(claim_year_month, "01"), format="%y%m%d"), 
                                         SERVICEFROMDATE, 
                                         units="days"))
  ) %>%
  filter(submission_lag < 0) %>%
  group_by(PLACEOFSERVICE, procedure_category) %>%
  summarise(
    claim_count = n(),
    avg_billed = mean(BILLEDAMOUNT),
    total_billed = sum(BILLEDAMOUNT),
    unique_patients = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  arrange(desc(claim_count))

print("\nClaims with negative submission lags (pre-dated claims):")
print(negative_lag_analysis)

# Analyze claims with very long submission lags
long_lag_analysis <- combined_claims %>%
  mutate(
    claim_year_month = substr(CLAIMNUMBER, 1, 4),
    submission_lag = as.numeric(difftime(as.Date(paste0(claim_year_month, "01"), format="%y%m%d"), 
                                         SERVICEFROMDATE, 
                                         units="days"))
  ) %>%
  filter(submission_lag > 365) %>%  # Claims submitted more than 1 year after service
  group_by(PLACEOFSERVICE, procedure_category) %>%
  summarise(
    claim_count = n(),
    avg_billed = mean(BILLEDAMOUNT),
    total_billed = sum(BILLEDAMOUNT),
    unique_patients = n_distinct(MVDID),
    .groups = 'drop'
  ) %>%
  arrange(desc(claim_count))

print("\nClaims with submission lag > 1 year:")
print(long_lag_analysis)

# Analyze high-volume delayed claims by patient
patient_delayed_claims <- combined_claims %>%
  mutate(
    claim_year_month = substr(CLAIMNUMBER, 1, 4),
    submission_lag = as.numeric(difftime(as.Date(paste0(claim_year_month, "01"), format="%y%m%d"), 
                                         SERVICEFROMDATE, 
                                         units="days"))
  ) %>%
  filter(submission_lag > 365) %>%
  group_by(MVDID, PLACEOFSERVICE, procedure_category) %>%
  summarise(
    claim_count = n(),
    avg_billed = mean(BILLEDAMOUNT),
    total_billed = sum(BILLEDAMOUNT),
    unique_procedures = n_distinct(PROCEDURECODE),
    date_range = paste(min(SERVICEFROMDATE), max(SERVICEFROMDATE)),
    .groups = 'drop'
  ) %>%
  filter(claim_count > 50) %>%  # Looking at patients with many delayed claims
  arrange(desc(claim_count))

print("\nPatients with high volume of delayed claims (>50 claims):")
print(patient_delayed_claims)

# Look at claim sequence for these high-volume cases
high_volume_sequence <- combined_claims %>%
  filter(MVDID %in% (patient_delayed_claims %>% pull(MVDID))) %>%
  mutate(
    claim_year_month = substr(CLAIMNUMBER, 1, 4),
    submission_lag = as.numeric(difftime(as.Date(paste0(claim_year_month, "01"), format="%y%m%d"), 
                                         SERVICEFROMDATE, 
                                         units="days"))
  ) %>%
  arrange(MVDID, SERVICEFROMDATE, CLAIMNUMBER) %>%
  group_by(MVDID) %>%
  slice(1:10)  # Show first 10 claims for each high-volume patient

print("\nClaim sequence for high-volume delayed claims (first 10 claims per patient):")
print(high_volume_sequence)