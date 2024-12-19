import logging
from mercury.utils import ensure_directory_exists, save_csv, load_csv
import pandas as pd

# Configure logging for the module
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def calculate_returns(df, col="S&P500"):
    """
    Calculate daily and cumulative returns for the given column in the DataFrame.
    """
    logging.info("Calculating daily and cumulative market returns...")
    df["Daily Returns"] = df[col].pct_change()
    df["Cumulative Returns"] = (1 + df["Daily Returns"]).cumprod() - 1
    return df


def calculate_volatility(df, col="Daily Returns", window=30):
    """
    Calculate rolling volatility for the given column in the DataFrame.
    """
    logging.info(f"Calculating {window}-day rolling volatility...")
    df["Volatility"] = df[col].rolling(window).std()
    return df


def process_macro(files, save_dir, prefix="processed_"):
    """
    Combine multiple macroeconomic datasets into a single DataFrame and save the result.
    """
    try:
        logging.info("Processing macroeconomic data...")
        macro_data = pd.concat([load_csv(file) for file in files], axis=1, join="inner")
        save_csv(macro_data, save_dir, "macro_metrics.csv", prefix)
        logging.info("Macroeconomic metrics saved successfully.")
    except Exception as e:
        logging.error(f"Failed to process macroeconomic data: {e}")
        raise


def process_market_metrics(df, save_dir, prefix="processed_"):
    """
    Calculate and save market-level metrics (returns and volatility).
    """
    try:
        ensure_directory_exists(save_dir)
        logging.info("Processing market metrics...")

        # Calculate metrics
        df = calculate_returns(df)
        df = calculate_volatility(df)

        # Save metrics
        save_csv(df[["Date", "Daily Returns", "Cumulative Returns"]], save_dir, "market_returns.csv", prefix)
        save_csv(df[["Date", "Volatility"]], save_dir, "market_volatility.csv", prefix)
        logging.info("Market metrics processed and saved successfully.")
    except Exception as e:
        logging.error(f"Failed to process market metrics: {e}")
        raise
