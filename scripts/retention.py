import os
import logging
from datetime import datetime
from utils import configure_logging, setup_directories

BRONZE_PATH = "data-lake/bronze"
RETENTION_LIMIT = 4


def apply_retention():
    """
    Retain only the latest RETENTION_LIMIT files in each subdirectory.
    """
    try:
        logging.info("Applying retention policy...")

        for subdir in os.listdir(BRONZE_PATH):
            subdir_path = os.path.join(BRONZE_PATH, subdir)
            if os.path.isdir(subdir_path):
                files = [f for f in os.listdir(subdir_path) if f.endswith(".csv")]
                try:
                    # Sort by date prefix in the filename
                    files_sorted = sorted(files, key=lambda x: datetime.strptime(x.split('_')[0], "%Y%m%d"),
                                          reverse=True)
                except ValueError as ve:
                    logging.error(f"Skipping improperly formatted file in {subdir_path}: {ve}")
                    continue

                # Delete files beyond retention limit
                for file_to_delete in files_sorted[RETENTION_LIMIT:]:
                    os.remove(os.path.join(subdir_path, file_to_delete))
                    logging.info(f"Deleted old file: {file_to_delete}")

        logging.info("Retention policy applied successfully.")

    except Exception as e:
        logging.error(f"Error applying retention policy: {e}")
        raise


if __name__ == "__main__":
    configure_logging("retention.log")
    setup_directories([BRONZE_PATH])
    apply_retention()
