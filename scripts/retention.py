import os
import logging
from datetime import datetime
import sys
from utils import setup_directories, configure_logging

# Constants
BRONZE_PATH = "data-lake/bronze"
RETENTION_LIMIT = 4  # Number of latest files to retain


def apply_retention_policy():
    """
    Retains only the latest RETENTION_LIMIT files in each subdirectory of BRONZE_PATH.
    Deletes older files beyond the retention limit.
    """
    try:
        logging.info("Starting retention policy application.")

        # Iterate over each subdirectory in BRONZE_PATH
        for subdir in os.listdir(BRONZE_PATH):
            subdir_path = os.path.join(BRONZE_PATH, subdir)
            if os.path.isdir(subdir_path):
                # List all CSV files in the subdirectory
                files = [f for f in os.listdir(subdir_path) if f.endswith(".csv")]

                # Extract date prefixes and sort files by date descending
                try:
                    files_sorted = sorted(
                        files,
                        key=lambda x: datetime.strptime(x.split('_')[0], "%Y%m%d"),
                        reverse=True
                    )
                except ValueError as ve:
                    logging.error(f"Filename format incorrect in {subdir_path}: {ve}")
                    continue

                # Determine files to delete
                files_to_delete = files_sorted[RETENTION_LIMIT:]

                for file_name in files_to_delete:
                    file_path = os.path.join(subdir_path, file_name)
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted old file: {file_path}")
                    except Exception as e:
                        logging.error(f"Failed to delete {file_path}: {e}")

        logging.info("Completed retention policy application.")

    except Exception as e:
        logging.error(f"Error during retention policy application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Configure logging for this script
    configure_logging('retention.log')

    # Setup necessary directories (assuming bronze directories already exist)
    setup_directories([BRONZE_PATH])

    # Apply retention policy
    apply_retention_policy()
