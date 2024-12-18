import sys
import os
import pandas as pd
import logging
from market_metrics import calculate_and_save_market_metrics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def validate_path(file_path):
    """Validate that a file or directory path exists."""
    if not os.path.exists(file_path):
        logging.error(f"Path not found: {file_path}")
        raise FileNotFoundError(f"Required file/path not found: {file_path}")
    return file_path


if __name__ == "__main__":
    # Step 1: Display working directory
    cwd = os.getcwd()
    logging.info(f"Current Working Directory: {cwd}")

    # Step 2: Validate and set up paths
    data_lake_path = validate_path("data-lake/silver")
    analytics_path = os.path.join(data_lake_path, "analytics")
    market_path = validate_path(os.path.join(data_lake_path, "stocks/cleaned_sp500_index.csv"))
    market_save_path = os.path.join(analytics_path, "market")

    # Step 3: Load market data
    logging.info("Loading market data...")
    market_df = pd.read_csv(market_path)
    logging.info(f"Market data loaded successfully with {len(market_df)} rows.")

    # Step 4: Generate and save market metrics
    logging.info("Calculating market metrics...")
    calculate_and_save_market_metrics(
        market_df=market_df,
        save_path=market_save_path,
    )
    logging.info(f"Market metrics saved successfully to {market_save_path}")
