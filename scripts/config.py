import os

# Get the project root directory dynamically
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Directories for Bronze Layer storage
DATA_LAKE_BASE_PATH = os.path.join(PROJECT_ROOT, "data-lake/bronze")
BRONZE_PATHS = {
    "kaggle": os.path.join(DATA_LAKE_BASE_PATH, "sp500_kaggle"),
    "fred": os.path.join(DATA_LAKE_BASE_PATH, "fred_data"),
}

# Retention policy
RETENTION_LIMIT = 4

# Kaggle API
KAGGLE_DATASET = "andrewmvd/sp-500-stocks"

# FRED API
FRED_INDICATORS = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]
