import os
import pandas as pd


def enrich_companies_data(df):
    """
    Enrich SP500 Companies data with indicators like EBITDA Margin, Revenue per Employee, and Sector Averages.
    """
    # Ensure numeric types
    numeric_columns = ["Currentprice", "Marketcap", "Ebitda", "Revenuegrowth", "Fulltimeemployees"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Define derived metrics
    df["EBITDA_Margin"] = (df["Ebitda"] / (df["Revenuegrowth"])).round(4)
    df["Revenue_Per_Employee"] = (df["Revenuegrowth"] / df["Fulltimeemployees"]).round(2)

    # Calculate sector averages
    sector_averages = df.groupby("Sector")[["Currentprice", "Marketcap", "EBITDA_Margin"]].mean().reset_index()
    sector_averages.rename(columns={
        "Currentprice": "Avg_Currentprice",
        "Marketcap": "Avg_Marketcap",
        "EBITDA_Margin": "Avg_EBITDA_Margin"
    }, inplace=True)

    return df, sector_averages


def enrich_stocks_data(df):
    """
    Enrich SP500 Stocks data with daily returns, moving averages, and volatility metrics.
    """
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df.sort_values(by=["Symbol", "Date"], inplace=True)

    # Define derived metrics
    df["Daily_Returns"] = df.groupby("Symbol")["Adj Close"].pct_change().round(4)
    df["30Day_Volatility"] = df.groupby("Symbol")["Daily_Returns"].rolling(30).std().reset_index(level=0, drop=True)

    # Calculate moving averages
    for window in [5, 50, 200]:
        df[f"{window}Day_MA"] = (
            df.groupby("Symbol")["Adj Close"].rolling(window).mean().reset_index(level=0, drop=True)
        )

    return df


def enrich_index_data(df):
    """
    Enrich SP500 Index data with daily returns and rolling volatility.
    """
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df.sort_values(by="Date", inplace=True)

    # Daily Returns for SP500
    df["Daily_Returns"] = df["S&P500"].pct_change().round(4)

    # 30-Day Rolling Volatility
    df["30Day_Volatility"] = df["Daily_Returns"].rolling(30).std().round(4)

    return df


def enrich_macro_data(df, indicator_name):
    """
    Clean and enrich macroeconomic data by filtering for relevant dates and rounding values.
    """
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df[df["Date"] >= pd.Timestamp("1995-01-01")]  # Filter for relevant dates
    df["Value"] = df["Value"].round(4)
    df.rename(columns={"Value": indicator_name}, inplace=True)
    return df


def link_datasets(companies, stocks, index, macros):
    """
    Link datasets together using Date and Symbol to create a unified Silver-layer dataframe.
    """
    # Merge companies with stock data on Symbol
    combined = stocks.merge(companies, on="Symbol", how="left")

    # Merge with SP500 Index on Date
    combined = combined.merge(index, on="Date", how="left")

    # Merge with macroeconomic indicators on Date
    for macro_name, macro_data in macros.items():
        combined = combined.merge(macro_data, on="Date", how="left")

    return combined


def transform_to_silver(bronze_path, silver_path):
    """
    Full pipeline to transform Bronze data into Silver.
    """
    # Load datasets from Bronze layer
    companies = pd.read_csv(os.path.join(bronze_path, "20241218_sp500_companies.csv"))
    stocks = pd.read_csv(os.path.join(bronze_path, "20241218_sp500_stocks.csv"))
    index = pd.read_csv(os.path.join(bronze_path, "20241218_sp500_index.csv"))

    macro_files = [
        "20241218_UNRATE.csv",
        "20241218_FEDFUNDS.csv",
        "20241218_CPIAUCSL.csv",
        "20241218_GDP.csv",
        "20241218_DGS10.csv"
    ]

    macros = {}
    for macro_file in macro_files:
        indicator_name = macro_file.split("_")[1].replace(".csv", "")
        macros[indicator_name] = enrich_macro_data(
            pd.read_csv(os.path.join(bronze_path, macro_file)),
            indicator_name
        )

    # Enrich datasets
    companies, sector_averages = enrich_companies_data(companies)
    stocks = enrich_stocks_data(stocks)
    index = enrich_index_data(index)

    # Link datasets
    combined = link_datasets(companies, stocks, index, macros)

    # Save results to Silver layer
    os.makedirs(silver_path, exist_ok=True)
    combined.to_csv(os.path.join(silver_path, "sp500_combined_silver.csv"), index=False)
    sector_averages.to_csv(os.path.join(silver_path, "sector_averages_silver.csv"), index=False)
    print(f"Silver data saved to {silver_path}")


if __name__ == "__main__":
    transform_to_silver("../data-lake/bronze/kaggle", "../data-lake/silver/analytics")
