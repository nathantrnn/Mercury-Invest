import pandas as pd
import os


def calculate_daily_returns(df):
    df["Daily Returns"] = df.groupby("Symbol")["Adj Close"].pct_change()
    return df


def calculate_cumulative_returns(df):
    df["Cumulative Returns"] = df.groupby("Symbol")["Adj Close"].apply(lambda x: (x / x.iloc[0]) - 1)
    return df


def calculate_volatility(df, window=30):
    df["Volatility"] = df.groupby("Symbol")["Daily Returns"].rolling(window).std().reset_index(level=0, drop=True)
    return df


def calculate_moving_averages(df):
    for window in [50, 200]:
        df[f"MA_{window}"] = df.groupby("Symbol")["Adj Close"].rolling(window).mean().reset_index(level=0, drop=True)
    return df


def calculate_beta(stocks, market):
    market["Market Returns"] = market["S&P500"].pct_change()

    beta_values = {}
    for symbol in stocks["Symbol"].unique():
        stock_returns = stocks.loc[stocks["Symbol"] == symbol, "Daily Returns"].dropna()
        market_returns = market["Market Returns"].dropna()
        beta = stock_returns.corr(market_returns) * stock_returns.std() / market_returns.std()
        beta_values[symbol] = beta

    stocks["Beta"] = stocks["Symbol"].map(beta_values)
    return stocks


def calculate_and_save_stock_metrics(input_path, save_path, market_data):
    # Load stock data
    stocks_df = pd.read_csv(input_path)

    # Calculate metrics
    stocks_df = calculate_daily_returns(stocks_df)
    stocks_df = calculate_cumulative_returns(stocks_df)
    stocks_df = calculate_volatility(stocks_df)
    stocks_df = calculate_moving_averages(stocks_df)
    stocks_df = calculate_beta(stocks_df, market_data)

    # Save results
    stocks_df[["Date", "Symbol", "Daily Returns"]].to_csv(os.path.join(save_path, "daily_returns.csv"), index=False)
    stocks_df[["Date", "Symbol", "Cumulative Returns"]].to_csv(os.path.join(save_path, "cumulative_returns.csv"),
                                                               index=False)
    stocks_df[["Date", "Symbol", "Volatility"]].to_csv(os.path.join(save_path, "volatility.csv"), index=False)
    stocks_df[["Date", "Symbol", "MA_50", "MA_200"]].to_csv(os.path.join(save_path, "moving_averages.csv"), index=False)
    stocks_df[["Date", "Symbol", "Beta"]].to_csv(os.path.join(save_path, "beta.csv"), index=False)
