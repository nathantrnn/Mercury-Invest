import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from mercury.config import Config
from mercury.utils import ensure_directory_exists, save_csv


def ingest_kaggle():
    kaggle_path = Config.BRONZE_PATHS["kaggle"]
    ensure_directory_exists(kaggle_path)

    api = KaggleApi()
    api.authenticate()

    try:
        api.dataset_download_files("andrewmvd/sp-500-stocks", path=kaggle_path, unzip=True)
        for file in [f for f in os.listdir(kaggle_path) if f.endswith(".csv")]:
            df = pd.read_csv(os.path.join(kaggle_path, file))
            # Use save_csv instead of save_with_prefix
            save_csv(df, kaggle_path, file, prefix="raw_")
            os.remove(os.path.join(kaggle_path, file))
    except Exception as e:
        print(f"Error: {e}")


def main():
    print("Starting Kaggle data ingestion...")
    ingest_kaggle()
    print("Kaggle data ingestion completed!")


if __name__ == "__main__":
    main()
