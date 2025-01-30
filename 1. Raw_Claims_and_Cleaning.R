# 1. Raw Claims ----
## Raw Claims ----
library(tidyverse)

# Base Filepath
filepath <- "/Users/williamwatson/Library/CloudStorage/OneDrive-UniversityofArkansasforMedicalSciences/Deductible_Project/Deductibles/Data/"

# Read data by just adding filename to filepath
data <- read.csv(paste0(filepath, "final.csv"))

#Bring in df, unique_mem, and final
