import os
import logging
from datetime import datetime
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
from scripts.config import BRONZE_PATHS, FRED_INDICATORS
from scripts.utils import configure_logging, setup_directories

BRONZE_PATH = BRONZE_PATHS["fred"]


def ingest_fred():
    logging.info("Starting FRED ingestion...")
    load_dotenv()
    fred = Fred(api_key=os.getenv("FRED_API_KEY"))
    today_str = datetime.now().strftime("%Y%m%d")

    for indicator in FRED_INDICATORS:
        try:
            series = fred.get_series(indicator)
            df = pd.DataFrame(series, columns=["Value"])
            df["Date"] = series.index
            csv_path = os.path.join(BRONZE_PATH, f"{today_str}_{indicator}.csv")
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved FRED data: {csv_path}")
        except Exception as e:
            logging.error(f"Failed to fetch {indicator}: {e}")
    logging.info("FRED ingestion completed.")


if __name__ == "__main__":
    configure_logging("ingest_fred.log")
    setup_directories([BRONZE_PATH])
    ingest_fred()
