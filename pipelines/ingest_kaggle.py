import os
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi


def ingest_kaggle():
    kaggle_path = "data-lake/bronze/kaggle"
    os.makedirs(kaggle_path, exist_ok=True)

    dataset = "andrewmvd/sp-500-stocks"
    today_date = datetime.now().strftime("%Y%m%d")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path=kaggle_path, unzip=True)

    # Rename files with timestamp only for new files
    for file in os.listdir(kaggle_path):
        src = os.path.join(kaggle_path, file)

        # Skip invalid or hidden files (e.g., .DS_Store, non-CSV files)
        if not file.endswith(".csv") or file.startswith("."):
            continue

        # Avoid adding duplicate timestamp to already-prefixed files
        if file.startswith(today_date):
            print(f"File already processed: {src}")
            continue

        # Add timestamp prefix
        dest = os.path.join(kaggle_path, f"{today_date}_{file}")
        os.rename(src, dest)
        print(f"Kaggle data saved: {dest}")


if __name__ == "__main__":
    ingest_kaggle()
