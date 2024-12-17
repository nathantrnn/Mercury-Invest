import sys
import os
import logging

# Dynamically add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
from scripts.config import BRONZE_PATHS, FRED_INDICATORS
from scripts.utils import configure_logging, setup_directories

BRONZE_PATH = BRONZE_PATHS["fred"]


def fetch_and_save_fred_data(fred, indicators, save_path, date_prefix):
    """
    Fetches data from FRED API and saves it as CSV files to the given path.

    :param fred: FRED API client
    :param indicators: List of indicators to fetch
    :param save_path: Destination directory for saving the data
    :param date_prefix: Prefix to add to the saved file names (e.g., today's date)
    """
    for indicator in indicators:
        try:
            series = fred.get_series(indicator)
            df = pd.DataFrame(series, columns=["Value"]).reset_index()
            df.columns = ["Date", "Value"]  # Ensure consistent column naming
            csv_path = os.path.join(save_path, f"{date_prefix}_{indicator}.csv")
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved FRED data: {csv_path}")
        except Exception as e:
            logging.error(f"Failed to fetch {indicator}: {e}")


def ingest_fred():
    """
    Main function to handle FRED data ingestion.
    """
    logging.info("Starting FRED ingestion...")
    try:
        load_dotenv()
        fred = Fred(api_key=os.getenv("FRED_API_KEY"))
        today_str = datetime.now().strftime("%Y%m%d")

        fetch_and_save_fred_data(fred, FRED_INDICATORS, BRONZE_PATH, today_str)
        logging.info("FRED ingestion completed successfully.")
    except Exception as e:
        logging.error(f"FRED ingestion failed: {e}")


if __name__ == "__main__":
    configure_logging("ingest_fred.log")
    setup_directories([BRONZE_PATH])
    ingest_fred()
