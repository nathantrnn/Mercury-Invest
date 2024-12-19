import os
import logging
from mercury.transformation.market_metrics import process_market_metrics, process_macro
from mercury.transformation.stock_metrics import process_stock_metrics
from mercury.utils import validate_path, ensure_directory_exists, load_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

if __name__ == "__main__":
    logging.info("Starting analytics transformation...")

    try:
        # Paths configuration
        silver_path = validate_path("data-lake/silver")
        analytics_path = os.path.join(silver_path, "analytics")
        ensure_directory_exists(analytics_path)  # Ensure analytics directory exists

        # Process Market Metrics
        logging.info("Processing market metrics...")
        market_file = validate_path(os.path.join(silver_path, "stocks/cleaned_sp500_index.csv"))
        market_save_dir = os.path.join(analytics_path, "market")
        market_data = load_csv(market_file)
        process_market_metrics(market_data, market_save_dir, prefix="processed_")

        # Process Stock Metrics
        logging.info("Processing stock metrics...")
        stock_file = validate_path(os.path.join(silver_path, "stocks/cleaned_sp500_stocks.csv"))
        stock_save_dir = os.path.join(analytics_path, "stocks")
        process_stock_metrics(stock_file, stock_save_dir, market_data, moving_avg_windows=[50, 200])

        # Process Macroeconomic Metrics
        logging.info("Processing macroeconomic metrics...")
        macro_files = [
            validate_path(os.path.join(silver_path, "macro", file))
            for file in [
                "cleaned_DGS10.csv",
                "cleaned_FEDFUNDS.csv",
                "cleaned_GDP.csv",
                "cleaned_UNRATE.csv",
                "cleaned_CPIAUCSL.csv",
            ]
        ]
        macro_save_dir = os.path.join(analytics_path, "macro")
        process_macro(macro_files, macro_save_dir)

        # Process Sector Metrics
        stock_file = validate_path(os.path.join(silver_path, "stocks/cleaned_sp500_stocks.csv"))
        stock_save_dir = os.path.join(analytics_path, "stocks")
        process_stock_metrics(stock_file, stock_save_dir, market_data, moving_avg_windows=[50, 200])

        # Log completion
        logging.info("All metrics processed successfully with 'processed_' prefix.")

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
