import os


class Config:

    DATA_LAKE_PATHS = {
        "bronze": os.path.join("data-lake", "bronze"),
        "silver": os.path.join("data-lake", "silver"),
        "gold": os.path.join("data-lake", "gold"),
    }

    BRONZE_PATHS = {
        "kaggle": os.path.join(DATA_LAKE_PATHS["bronze"], "kaggle"),
        "fred": os.path.join(DATA_LAKE_PATHS["bronze"], "fred"),
    }

    RETENTION_LIMIT = 4
    INDICATORS = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]  # FRED Indicators
