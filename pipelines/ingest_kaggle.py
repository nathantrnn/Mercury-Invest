import os
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
from config import Config


def ingest_kaggle():
    kaggle_path = Config.BRONZE_PATHS["kaggle"]
    os.makedirs(kaggle_path, exist_ok=True)

    dataset = "andrewmvd/sp-500-stocks"
    today_date = datetime.now().strftime("%Y%m%d")

    # Authenticate and download files
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path=kaggle_path, unzip=True)

    # Add timestamp to files
    for file in os.listdir(kaggle_path):
        src = os.path.join(kaggle_path, file)
        if file.endswith(".csv") and not file.startswith(today_date):
            dest = os.path.join(kaggle_path, f"{today_date}_{file}")
            os.rename(src, dest)
            print(f"Saved Kaggle data: {dest}")


if __name__ == "__main__":
    ingest_kaggle()
