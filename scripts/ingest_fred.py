import os
import logging
from datetime import datetime
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
from utils import configure_logging, setup_directories

# Load environment variables from the .env file
load_dotenv()

BRONZE_PATH = "data-lake/bronze/fred_data"
FRED_API_KEY = os.getenv("FRED_API_KEY")
INDICATORS = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]


def configure_logging_with_console(log_filename):
    """
    Configure logging to log both to a file and the console.
    """
    from logging.handlers import RotatingFileHandler

    # Ensure log directory exists
    os.makedirs("logs", exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(f"logs/{log_filename}", maxBytes=5 * 1024 * 1024, backupCount=3),  # File logging
            logging.StreamHandler()  # Console logging
        ],
    )


def ingest_fred():
    """
    Download and process data from FRED.
    """
    if not FRED_API_KEY:
        logging.critical("FRED_API_KEY is not set.")
        raise EnvironmentError("Missing FRED API key.")

    try:
        logging.info("Starting FRED dataset ingestion...")
        fred = Fred(api_key=FRED_API_KEY)
        today_str = datetime.now().strftime("%Y%m%d")

        for indicator in INDICATORS:
            try:
                series = fred.get_series(indicator)
                df = pd.DataFrame(series, columns=["Value"])
                df["Date"] = series.index
                csv_path = os.path.join(BRONZE_PATH, f"{today_str}_{indicator}.csv")
                df.to_csv(csv_path, index=False)
                logging.info(f"Saved FRED data to: {csv_path}")
            except Exception as e:
                logging.error(f"Failed to fetch {indicator}: {e}",
                              exc_info=True)  # Add stack trace for detailed logging

    except Exception as e:
        logging.error(f"Error during FRED ingestion: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    configure_logging("ingest_fred.log", add_console=True)
    setup_directories([BRONZE_PATH])
    ingest_fred()