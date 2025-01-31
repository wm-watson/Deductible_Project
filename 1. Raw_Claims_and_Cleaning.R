# 1. Raw Claims ----
## Raw Claims ----
library(tidyverse)
library(beepr)

#Filepath
filepath <- "/Users/williamwatson/Library/CloudStorage/OneDrive-UniversityofArkansasforMedicalSciences/Deductible_Project/Deductibles/"

# Read data by adding filename to filepath
final <- read.csv(paste0(filepath, "final.csv"),
                  header = TRUE)
beep(8)
#Bring in df, unique_mem, and final
