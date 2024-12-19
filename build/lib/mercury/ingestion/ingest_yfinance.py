import os
import pandas as pd
import logging
import yfinance as yf
from mercury.utils import ensure_directory_exists, load_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# Define file paths
COMPANIES_FILE = "data-lake/bronze/kaggle/raw_sp500_companies.csv"
YFINANCE_DIR = "data-lake/bronze/yfinance"
OUTPUT_FILE = "data-lake/bronze/raw_sp500_adj_close.csv"


def extract_symbols_and_metadata(file_path: str) -> pd.DataFrame:
    """
    Extract 'Symbol', 'Shortname', 'Industry', and 'Sector' from the given S&P500 companies CSV.
    Args:
        file_path (str): Path to the S&P500 companies CSV file.
    Returns:
        pd.DataFrame: Filtered DataFrame with columns 'Symbol', 'Shortname', 'Industry', 'Sector'.
    """
    logging.info(f"Extracting metadata from '{file_path}'...")
    try:
        companies = load_csv(file_path)
        required_columns = {"Symbol", "Shortname", "Industry", "Sector"}

        missing_columns = required_columns - set(companies.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        logging.info(f"Loaded metadata for {len(companies)} companies.")
        return companies[list(required_columns)]
    except Exception as e:
        logging.error(f"Error extracting metadata: {e}")
        raise


def fetch_and_save_ticker_data(symbols: pd.Series, output_dir: str, start_date: str = "1995-01-01",
                               batch_size: int = 10):
    """
    Fetch historical stock data in batches and save them as CSV files.
    Args:
        symbols (pd.Series): Series of stock ticker symbols.
        output_dir (str): Directory to save the CSV files.
        start_date (str): Start date for historical data.
        batch_size (int): Number of tickers to process in a single batch.
    """
    ensure_directory_exists(output_dir)
    logging.info(f"Fetching data for {len(symbols)} tickers in batches of {batch_size}...")

    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i + batch_size]
        logging.info(f"Processing batch {i // batch_size + 1}...")
        for ticker in batch_symbols:
            file_path = os.path.join(output_dir, f"{ticker}.csv")
            if os.path.exists(file_path):
                logging.info(f"Data for {ticker} already exists. Skipping...")
                continue
            try:
                data = yf.Ticker(ticker).history(start=start_date, auto_adjust=False)
                if data.empty:
                    logging.warning(f"No data found for {ticker}. Skipping...")
                    continue

                data.index.name = "Date"  # Set index name
                data.to_csv(file_path)
                logging.info(f"Data for {ticker} saved to {file_path}.")
            except Exception as e:
                logging.error(f"Error fetching data for {ticker}: {e}")


def combine_adj_close_to_dataframe(companies: pd.DataFrame, input_dir: str) -> pd.DataFrame:
    """
    Combine 'Adj Close' data for all tickers into a single DataFrame.
    Args:
        companies (pd.DataFrame): DataFrame with metadata (Symbol, Shortname, Industry, Sector).
        input_dir (str): Directory containing individual ticker CSV files.
    Returns:
        pd.DataFrame: Combined 'Adj Close' DataFrame with metadata.
    """
    logging.info(f"Combining 'Adj Close' data from {len(companies)} tickers in {input_dir}...")
    combined_data = []

    for _, row in companies.iterrows():
        file_path = os.path.join(input_dir, f"{row['Symbol']}.csv")
        if not os.path.exists(file_path):
            logging.warning(f"File not found for {row['Symbol']}. Skipping...")
            continue
        try:
            data = pd.read_csv(file_path, usecols=["Date", "Adj Close"], index_col="Date", parse_dates=True)
            data.rename(columns={"Adj Close": row["Symbol"]}, inplace=True)
            combined_data.append(data)
        except Exception as e:
            logging.error(f"Error combining data for {row['Symbol']}: {e}")

    # Concatenate all data along columns (ticker as column name)
    if combined_data:
        combined_df = pd.concat(combined_data, axis=1)
        logging.info("All 'Adj Close' data combined.")
        return combined_df
    else:
        logging.warning("No valid 'Adj Close' data to combine.")
        return pd.DataFrame()


def save_combined_data_with_metadata(combined_df: pd.DataFrame, companies: pd.DataFrame, output_file: str):
    """
    Save the combined 'Adj Close' DataFrame with metadata as a structured CSV.
    Args:
        combined_df (pd.DataFrame): Combined 'Adj Close' DataFrame.
        companies (pd.DataFrame): Metadata DataFrame with columns Symbol, Shortname, Industry, Sector.
        output_file (str): Path to save the final CSV file.
    """
    if combined_df.empty:
        logging.warning("Combined DataFrame is empty. Skipping save operation.")
        return

    logging.info("Transforming combined data into structured format...")
    combined_df = combined_df.reset_index().melt(id_vars="Date", var_name="Symbol", value_name="Adj Close")
    combined_with_metadata = pd.merge(combined_df, companies, on="Symbol", how="left")

    # Pivot with metadata as index and dates as columns
    final_df = combined_with_metadata.pivot_table(
        index=["Symbol", "Shortname", "Industry", "Sector"],
        columns="Date",
        values="Adj Close"
    ).reset_index()

    ensure_directory_exists(os.path.dirname(output_file))
    final_df.to_csv(output_file, index=False)
    logging.info(f"Final combined data saved to {output_file}.")


if __name__ == "__main__":
    try:
        # Step 1: Extract metadata and symbols
        companies = extract_symbols_and_metadata(COMPANIES_FILE)

        # Step 2: Fetch and save data for each ticker
        fetch_and_save_ticker_data(companies["Symbol"], YFINANCE_DIR)

        # Step 3: Combine 'Adj Close' data and save as final output
        adj_close_df = combine_adj_close_to_dataframe(companies, YFINANCE_DIR)
        save_combined_data_with_metadata(adj_close_df, companies, OUTPUT_FILE)

        logging.info("Workflow completed successfully.")
    except Exception as e:
        logging.error(f"Error during workflow execution: {e}")
