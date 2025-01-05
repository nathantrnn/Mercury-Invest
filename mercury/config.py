import os


class Config:
    BASE_PATH = "data-lake"
    DATA_LAKE_PATHS = {
        "bronze": os.path.join(BASE_PATH, "bronze"),
        "silver": os.path.join(BASE_PATH, "silver"),
        "gold": os.path.join(BASE_PATH, "gold"),
    }

    BRONZE_PATHS = {
        "kaggle": os.path.join(DATA_LAKE_PATHS["bronze"], "kaggle"),
        "fred": os.path.join(DATA_LAKE_PATHS["bronze"], "fred"),
    }

    RETENTION_LIMIT = 4
    INDICATORS = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]
