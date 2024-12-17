import os
import logging
from dotenv import load_dotenv
import sys

# Load environment variables from .env
load_dotenv()

# Constants
LOGS_PATH = "logs"


def setup_directories(bronze_paths):
    """
    Ensure that necessary directories exist.
    """
    try:
        for path in bronze_paths:
            os.makedirs(path, exist_ok=True)
            logging.info(f"Verified existence of directory: {path}")

        os.makedirs(LOGS_PATH, exist_ok=True)
        logging.info(f"Verified existence of logs directory: {LOGS_PATH}")

    except Exception as e:
        logging.critical(f"Failed to create directories: {e}")
        sys.exit(1)


def configure_logging(log_filename):
    """
    Configure logging for the script.
    """
    try:
        os.makedirs(LOGS_PATH, exist_ok=True)
    except Exception as e:
        print(f"Failed to create logs directory: {e}")
        sys.exit(1)

    logging.basicConfig(
        filename=os.path.join(LOGS_PATH, log_filename),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
