import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from mercury.config import Config
from mercury.utils import ensure_directory_exists, save_csv


def ingest_kaggle():
    kaggle_path = Config.BRONZE_PATHS["kaggle"]
    ensure_directory_exists(kaggle_path)  # Ensure the target directory exists

    api = KaggleApi()
    api.authenticate()  # Authenticate with Kaggle API

    try:
        # Download and unzip the dataset
        api.dataset_download_files("andrewmvd/sp-500-stocks", path=kaggle_path, unzip=True)

        # Iterate through all CSV files in the directory
        for file in [f for f in os.listdir(kaggle_path) if f.endswith(".csv")]:
            file_path = os.path.join(kaggle_path, file)

            # Load the CSV into a DataFrame
            df = pd.read_csv(file_path)

            # Check if the file already has the "raw_" prefix
            if not file.startswith("raw_"):
                # Save the new file with the prefix
                save_csv(df, kaggle_path, file, prefix="raw_")

            # Delete the old file to clean up the directory
            os.remove(file_path)

            # Log the deletion for better traceability
            print(f"Deleted old file: {file_path}")

    except Exception as e:
        # Handle and log any errors during the process
        print(f"Error during Kaggle data ingestion: {e}")


def main():
    print("Starting Kaggle data ingestion...")
    ingest_kaggle()
    print("Kaggle data ingestion completed!")


if __name__ == "__main__":
    main()
