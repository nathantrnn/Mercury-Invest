import os
import logging
import yfinance as yf
from mercury.utils import ensure_directory_exists, load_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# File paths
COMPANIES_FILE = "data-lake/bronze/kaggle/raw_sp500_companies.csv"
YFINANCE_DIR = "data-lake/bronze/yfinance"


def extract_symbols(file_path: str) -> list:
    """Extract 'Symbol' column from the given S&P 500 companies file."""
    logging.info(f"Extracting symbols from {file_path}...")
    companies = load_csv(file_path)
    if "Symbol" not in companies.columns:
        raise ValueError("Missing 'Symbol' column.")
    return companies["Symbol"].tolist()


def fetch_and_save_data(symbols: list, output_dir: str, start_date: str = "1995-01-01"):
    """Fetch and save historical stock data for each symbol as CSV."""
    ensure_directory_exists(output_dir)
    for symbol in symbols:
        file_path = os.path.join(output_dir, f"{symbol}.csv")
        if os.path.exists(file_path):
            logging.info(f"{symbol}: File exists. Skipping...")
            continue
        try:
            logging.info(f"Fetching data for {symbol}...")
            data = yf.Ticker(symbol).history(start=start_date, auto_adjust=False)
            if not data.empty:
                data.index.name = "Date"
                data.to_csv(file_path)
                logging.info(f"{symbol}: Data saved.")
            else:
                logging.warning(f"{symbol}: No data found.")
        except Exception as e:
            logging.error(f"{symbol}: Fetch failed with error: {e}")


if __name__ == "__main__":
    try:
        symbols = extract_symbols(COMPANIES_FILE)
        fetch_and_save_data(symbols, YFINANCE_DIR)
        logging.info("Completed fetching all available data.")
    except Exception as e:
        logging.error(f"Process failed: {e}")
