import os
from fredapi import Fred
import pandas as pd
from datetime import datetime
import logging
import sys
from utils import setup_directories, configure_logging

# Constants
FRED_API_KEY = os.getenv("FRED_API_KEY")
INDICATORS = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]
BRONZE_PATH = "data-lake/bronze/fred_data"


def ingest_fred_data():
    """
    Fetches selected FRED indicators and saves them as CSVs with a date prefix.
    """
    if not FRED_API_KEY:
        logging.critical("FRED_API_KEY is not set in the environment.")
        sys.exit(1)

    try:
        logging.info("Starting ingestion of FRED data.")

        fred = Fred(api_key=FRED_API_KEY)
        today_str = datetime.now().strftime("%Y%m%d")

        for series_id in INDICATORS:
            try:
                logging.info(f"Fetching {series_id} from FRED.")
                data_series = fred.get_series(series_id)
                df = pd.DataFrame(data_series, columns=["Value"])
                df["Date"] = df.index
                df.reset_index(drop=True, inplace=True)
                df["SeriesID"] = series_id

                # Save to CSV with date prefix
                csv_filename = f"{today_str}_{series_id}.csv"
                csv_path = os.path.join(BRONZE_PATH, csv_filename)
                df.to_csv(csv_path, index=False)

                logging.info(f"Saved FRED CSV: {csv_path}")
            except Exception as inner_e:
                logging.error(f"Failed to fetch or save {series_id}: {inner_e}")

        logging.info("Completed ingestion of FRED data.")

    except Exception as e:
        logging.error(f"Error during FRED ingestion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Configure logging for this script
    configure_logging('ingest_fred.log')

    # Setup necessary directories
    setup_directories([BRONZE_PATH])

    # Run ingestion
    ingest_fred_data()