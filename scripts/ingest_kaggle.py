import os
import logging
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
from utils import configure_logging, setup_directories

BRONZE_PATH = "data-lake/bronze/sp500_kaggle"
KAGGLE_DATASET = "andrewmvd/sp-500-stocks"


def ingest_kaggle():
    """
    Download and process the S&P 500 Kaggle dataset.
    """
    try:
        logging.info("Starting Kaggle SP500 dataset ingestion...")
        today_str = datetime.now().strftime("%Y%m%d")

        # Initialize Kaggle API
        api = KaggleApi()
        try:
            logging.info("Authenticating with Kaggle API...")
            api.authenticate()
            logging.info("Kaggle API authentication successful.")
        except Exception as auth_error:
            logging.critical("Kaggle API authentication failed. Ensure your credentials are set up correctly.")
            raise auth_error

        # Download the Kaggle dataset
        temp_path = "temp_kaggle"
        os.makedirs(temp_path, exist_ok=True)  # To store temporary files
        try:
            logging.info(f"Attempting to download dataset: {KAGGLE_DATASET}")
            api.dataset_download_files(KAGGLE_DATASET, path=temp_path, unzip=True)
            logging.info(f"Kaggle dataset '{KAGGLE_DATASET}' downloaded and unzipped into '{temp_path}'.")
        except Exception as download_error:
            logging.critical(f"Failed to download Kaggle dataset: {download_error}")
            raise download_error

        # Verify if the files are downloaded correctly
        csv_files = [f for f in os.listdir(temp_path) if f.endswith(".csv")]
        if not csv_files:
            logging.error(f"No CSV files were found in the downloaded dataset at '{temp_path}'. Verify the dataset.")
            raise FileNotFoundError(f"No CSV files found in {temp_path} after extraction.")

        # Process and move CSV files to Bronze layer
        logging.info(f"{len(csv_files)} CSV files found. Moving files to Bronze layer.")
        if not os.path.exists(BRONZE_PATH):
            os.makedirs(BRONZE_PATH)
            logging.info(f"Created Bronze layer directory: {BRONZE_PATH}")

        for file_name in csv_files:
            src = os.path.join(temp_path, file_name)
            dst = os.path.join(BRONZE_PATH, f"{today_str}_{file_name}")
            try:
                os.rename(src, dst)
                logging.info(f"Moved file to Bronze layer: {dst}")
            except Exception as move_error:
                logging.error(f"Failed to move file {file_name} to Bronze layer: {move_error}")
                raise move_error

        # Cleanup temporary directory
        logging.info("Cleaning up temporary directory.")
        for file_name in os.listdir(temp_path):
            os.remove(os.path.join(temp_path, file_name))
        os.rmdir(temp_path)
        logging.info("Temporary files cleaned up.")

        logging.info("Kaggle dataset ingestion completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during Kaggle ingestion: {e}")
        raise


if __name__ == "__main__":
    # Configure logging to log both to a file and to the console
    configure_logging("ingest_kaggle.log")

    # Enable console logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logging.getLogger().addHandler(console_handler)

    # Ensure Bronze layer directory exists
    setup_directories([BRONZE_PATH])
    ingest_kaggle()
