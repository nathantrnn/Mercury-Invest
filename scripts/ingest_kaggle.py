import os
import kagglehub
from datetime import datetime
import logging
import sys
from utils import setup_directories, configure_logging

# Constants
KAGGLE_DATASET = "andrewmvd/sp-500-stocks"
BRONZE_PATH = "data-lake/bronze/sp500_kaggle"


def ingest_sp500_kaggle():
    """
    Downloads the S&P 500 dataset from Kaggle and saves CSVs with a date prefix.
    """
    try:
        logging.info("Starting ingestion of S&P 500 data from Kaggle.")

        # Download the Kaggle dataset
        dataset_path = kagglehub.dataset_download(KAGGLE_DATASET)
        logging.info(f"Kaggle dataset downloaded to: {dataset_path}")

        today_str = datetime.now().strftime("%Y%m%d")

        # Move/Copy CSV files to BRONZE_PATH with date prefix
        for file_name in os.listdir(dataset_path):
            if file_name.endswith(".csv"):
                src = os.path.join(dataset_path, file_name)
                date_prefixed = f"{today_str}_{file_name}"
                dst = os.path.join(BRONZE_PATH, date_prefixed)

                with open(src, 'rb') as f_src, open(dst, 'wb') as f_dst:
                    f_dst.write(f_src.read())

                logging.info(f"Saved Kaggle CSV: {dst}")

        logging.info("Completed ingestion of S&P 500 data from Kaggle.")

    except Exception as e:
        logging.error(f"Error during Kaggle ingestion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Configure logging for this script
    configure_logging('ingest_kaggle.log')

    # Setup necessary directories
    setup_directories([BRONZE_PATH])

    # Run ingestion
    ingest_sp500_kaggle()
