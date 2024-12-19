import pandas as pd
import logging
from mercury.utils import batch_process_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def add_stock_metrics(df, market_data, moving_avg_windows=[50, 200]):
    """Add daily returns, cumulative returns, rolling volatility, and moving averages to the DataFrame."""
    logging.info("Calculating daily returns...")
    df["Daily Returns"] = df.groupby("Symbol")["Adj Close"].pct_change()

    logging.info("Calculating cumulative returns...")
    df["Cumulative Returns"] = df.groupby("Symbol")["Adj Close"].transform(lambda x: (x / x.iloc[0]) - 1)

    logging.info("Calculating rolling volatility...")
    df["Volatility"] = df.groupby("Symbol")["Daily Returns"].rolling(30).std().reset_index(level=0, drop=True)

    logging.info("Calculating moving averages...")
    for window in moving_avg_windows:
        df[f"MA_{window}"] = (
            df.groupby("Symbol")["Adj Close"].rolling(window).mean().reset_index(level=0, drop=True)
        )

    logging.info("Calculating beta values...")
    df = add_beta(df, market_data)
    return df


def add_beta(stocks_df, market_df):
    """Add beta values to the stocks dataframe by comparing with market returns."""
    if "Market Returns" not in market_df.columns:
        logging.info("Calculating market returns for beta calculation...")
        market_df["Market Returns"] = market_df["S&P500"].pct_change()

    logging.info("Calculating beta for each stock symbol...")
    betas = {}
    for symbol in stocks_df["Symbol"].unique():
        stock_returns = stocks_df.loc[stocks_df["Symbol"] == symbol, "Daily Returns"]
        aligned_market = market_df["Market Returns"].reindex(stock_returns.index)
        betas[symbol] = stock_returns.corr(aligned_market)

    stocks_df["Beta"] = stocks_df["Symbol"].map(betas)
    return stocks_df


def process_stock_metrics(input_path, save_path, market_data, moving_avg_windows=[50, 200]):
    """Load, calculate, and save stock metrics."""
    try:
        logging.info(f"Loading stock data from {input_path}...")
        stocks_df = pd.read_csv(input_path, parse_dates=["Date"])
        if stocks_df.empty:
            raise ValueError("Stock data is empty.")

        logging.info("Adding stock-level metrics...")
        stocks_df = add_stock_metrics(stocks_df, market_data, moving_avg_windows)

        logging.info("Saving stock metrics...")
        batch_process_csv(
            [
                stocks_df[["Date", "Symbol", "Daily Returns"]],
                stocks_df[["Date", "Symbol", "Cumulative Returns"]],
                stocks_df[["Date", "Symbol", "Volatility"]],
                stocks_df[["Date", "Symbol", "Beta"]],
            ],
            ["daily_returns.csv", "cumulative_returns.csv", "volatility.csv", "beta.csv"],
            save_path,
        )
        logging.info("Stock metrics have been successfully processed and saved.")
    except Exception as e:
        logging.error(f"Error processing stock metrics: {e}")
        raise
