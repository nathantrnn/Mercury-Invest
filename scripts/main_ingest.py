import logging
from utils import configure_logging, setup_directories, run_script_in_subprocess

# Paths for Bronze Layer storage
BRONZE_PATHS = {
    "kaggle": "data-lake/bronze/sp500_kaggle",
    "fred": "data-lake/bronze/fred_data"
}


def main():
    logging.info("==== Starting Main Ingestion Pipeline ====")

    try:
        # Ensure necessary directories exist
        logging.info("Setting up directories...")
        setup_directories(list(BRONZE_PATHS.values()))
        logging.info("Directory setup completed.")

        # Step 1: Ingest Kaggle data
        run_script_in_subprocess("scripts/ingest_kaggle.py")

        # Step 2: Ingest FRED data
        run_script_in_subprocess("scripts/ingest_fred.py")

        # Step 3: Apply retention policy
        run_script_in_subprocess("scripts/retention.py")

        logging.info("==== Main Ingestion Pipeline Completed Successfully ====")
    except Exception as e:
        logging.critical(f"Critical error during the Ingestion Pipeline: {e}")
        exit(1)


if __name__ == "__main__":
    configure_logging("main_ingestion.log")
    main()
