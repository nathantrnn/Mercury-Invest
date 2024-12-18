import pandas as pd
import os


def calculate_market_returns(market):
    market["Daily Returns"] = market["S&P500"].pct_change()
    market["Cumulative Returns"] = (market["Daily Returns"] + 1).cumprod() - 1
    return market


def calculate_market_volatility(market, window=30):
    market["Volatility"] = market["Daily Returns"].rolling(window).std()
    return market


def calculate_and_save_market_metrics(market_path, save_path):
    # Load data
    market_df = pd.read_csv(market_path)

    # Calculate metrics
    market_df = calculate_market_returns(market_df)
    market_df = calculate_market_volatility(market_df)

    # Save results
    market_df[["Date", "Daily Returns", "Cumulative Returns"]].to_csv(os.path.join(save_path, "market_returns.csv"),
                                                                      index=False)
    market_df[["Date", "Volatility"]].to_csv(os.path.join(save_path, "market_volatility.csv"), index=False)
