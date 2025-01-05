import os
import logging
from typing import List
import yfinance as yf
from mercury.utils import ensure_directory_exists, load_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# File paths
COMPANIES_FILE = "data-lake/bronze/kaggle/raw_sp500_companies.csv"
YFINANCE_DIR = "data-lake/bronze/yfinance"


def extract_symbols(file_path: str) -> List[str]:
    logging.info(f"Validating input file: {file_path}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    logging.info(f"Extracting symbols from {file_path}...")
    companies = load_csv(file_path)
    if "Symbol" not in companies.columns:
        raise ValueError("Required column 'Symbol' is missing in the input file.")

    symbols = companies["Symbol"].dropna().unique().tolist()
    logging.info(f"Extracted {len(symbols)} unique symbols.")
    return symbols


def fetch_data_for_symbol(symbol: str, output_path: str, start_date: str) -> bool:
    try:
        logging.info(f"Fetching data for {symbol}...")
        data = yf.Ticker(symbol).history(start=start_date, auto_adjust=False)

        if not data.empty:
            data.index.name = "Date"
            data.to_csv(output_path)
            logging.info(f"{symbol}: Data saved successfully.")
            return True
        else:
            logging.warning(f"{symbol}: No data found.")
            return False
    except Exception as e:
        logging.error(f"{symbol}: Fetch failed with error: {e}")
        return False


def fetch_and_save_data(symbols: List[str], output_dir: str, start_date: str = "1995-01-01"):
    ensure_directory_exists(output_dir)
    total_symbols = len(symbols)
    success_count = 0
    failed_symbols = []

    logging.info(f"Starting data fetch for {total_symbols} symbols...")

    for symbol in symbols:
        file_path = os.path.join(output_dir, f"{symbol}.csv")
        if os.path.exists(file_path):
            logging.info(f"{symbol}: File already exists. Skipping...")
            continue

        success = fetch_data_for_symbol(symbol, file_path, start_date)
        if success:
            success_count += 1
        else:
            failed_symbols.append(symbol)

    logging.info(f"Data fetch completed: {success_count}/{total_symbols} tickers succeeded.")
    if failed_symbols:
        logging.error(f"Failed to fetch data for {len(failed_symbols)} tickers: {failed_symbols}")


if __name__ == "__main__":
    try:
        symbols = extract_symbols(COMPANIES_FILE)
        fetch_and_save_data(symbols, YFINANCE_DIR)
        logging.info("Completed fetching all available data.")
    except Exception as e:
        logging.error(f"Process failed: {e}")