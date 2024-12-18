import pandas as pd
import os
import logging

# Configure logging if this script is run standalone
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def calculate_market_returns(market):
    """Calculate daily and cumulative market returns."""
    logging.info("Calculating daily and cumulative returns...")
    market["Daily Returns"] = market["S&P500"].pct_change()
    market["Cumulative Returns"] = (market["Daily Returns"] + 1).cumprod() - 1
    logging.info("Market returns calculated successfully.")
    return market


def calculate_market_volatility(market, window=30):
    """Calculate market volatility over a rolling window."""
    logging.info(f"Calculating market volatility using a {window}-day window...")
    market["Volatility"] = market["Daily Returns"].rolling(window).std()
    logging.info("Market volatility calculated successfully.")
    return market


def calculate_and_save_market_metrics(market_df, save_path):
    """Calculate and save market metrics."""
    # Create output directory if it doesn't exist
    os.makedirs(save_path, exist_ok=True)
    logging.info(f"Output directory validated: {save_path}")

    # Step 1: Calculate returns
    market_df = calculate_market_returns(market_df)

    # Step 2: Calculate volatility
    market_df = calculate_market_volatility(market_df)

    # Step 3: Save results
    logging.info("Saving market metrics to files...")
    market_df[["Date", "Daily Returns", "Cumulative Returns"]].to_csv(
        os.path.join(save_path, "market_returns.csv"),
        index=False
    )
    market_df[["Date", "Volatility"]].to_csv(
        os.path.join(save_path, "market_volatility.csv"),
        index=False
    )
    logging.info("Market metrics files saved successfully.")
