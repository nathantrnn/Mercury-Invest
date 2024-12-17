import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from scripts.utils import configure_logging, clean_directory
from scripts.config import BRONZE_PATHS, RETENTION_LIMIT


def apply_retention():
    logging.info("Applying retention policy...")
    for _, dir_path in BRONZE_PATHS.items():
        clean_directory(dir_path, retention_limit=RETENTION_LIMIT)
    logging.info("Retention policy applied.")


if __name__ == "__main__":
    configure_logging("retention.log")
    apply_retention()
