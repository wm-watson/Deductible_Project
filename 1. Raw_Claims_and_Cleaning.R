# 1. Raw Claims ----
## Raw Claims ----
library(tidyverse)
library(beepr)

# Read and examine data

# Filepath
filepath <- "C:/Users/1187507/OneDrive - University of Arkansas for Medical Sciences/Deductible_Project/Deductibles/Data/final.csv"

# Read the header and the first 1 million lines (including the header)
lines <- read_lines(filepath, n_max = 1e6 + 1)  # +1 to include the header

# Write the selected lines to a new file
write_lines(lines, "selected_first_1m_rows.csv")

selected_data <- read_csv("selected_first_1m_rows.csv")
beep(8)

#Unique_mem, df, final