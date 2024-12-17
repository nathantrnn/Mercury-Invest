import os
import logging
from utils import configure_logging, setup_directories

# Paths for Bronze Layer
KAGGLE_BRONZE = "data-lake/bronze/sp500_kaggle"
FRED_BRONZE = "data-lake/bronze/fred_data"

# Logging configuration
configure_logging("main_ingestion.log")

def run_kaggle_ingestion():
    logging.info("Starting Kaggle ingestion...")
    os.system("python ingest_kaggle.py")
    logging.info("Completed Kaggle ingestion.")

def run_fred_ingestion():
    logging.info("Starting FRED ingestion...")
    os.system("python ingest_fred.py")
    logging.info("Completed FRED ingestion.")

def apply_retention():
    logging.info("Applying retention policy...")
    os.system("python retention.py")
    logging.info("Retention policy applied.")

if __name__ == "__main__":
    logging.info("Starting main ingestion pipeline...")

    # Setup directories
    setup_directories([KAGGLE_BRONZE, FRED_BRONZE])

    # Run each step
    run_kaggle_ingestion()
    run_fred_ingestion()
    apply_retention()

    logging.info("Main ingestion pipeline completed successfully.")
