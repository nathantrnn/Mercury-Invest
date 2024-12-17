import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
from scripts.utils import configure_logging, setup_directories, move_files
from scripts.config import BRONZE_PATHS, KAGGLE_DATASET

BRONZE_PATH = BRONZE_PATHS["kaggle"]


def ingest_kaggle():
    logging.info("Starting Kaggle ingestion...")
    temp_dir = "temp_kaggle"
    today_str = datetime.now().strftime("%Y%m%d")

    setup_directories([temp_dir, BRONZE_PATH])
    api = KaggleApi()
    api.authenticate()

    try:
        api.dataset_download_files(KAGGLE_DATASET, path=temp_dir, unzip=True)
        move_files(temp_dir, BRONZE_PATH, prefix=today_str)
    finally:
        for file in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, file))
        os.rmdir(temp_dir)
    logging.info("Kaggle ingestion completed.")


if __name__ == "__main__":
    configure_logging("ingest_kaggle.log")
    ingest_kaggle()
