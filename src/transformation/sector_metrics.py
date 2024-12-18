import pandas as pd
import os


def calculate_sector_returns(stocks, companies):
    merged_df = stocks.merge(companies[["Symbol", "Sector"]], on="Symbol")
    sector_returns = merged_df.groupby("Sector")["Cumulative Returns"].mean().reset_index(name="Avg Sector Returns")
    return sector_returns


def calculate_sector_marketcap(companies):
    marketcap_by_sector = companies.groupby("Sector")["Marketcap"].sum().reset_index(name="Total Marketcap")
    return marketcap_by_sector


def calculate_and_save_sector_metrics(stocks_path, companies_path, save_path):
    # Load data
    stocks_df = pd.read_csv(stocks_path)
    companies_df = pd.read_csv(companies_path)

    # Calculate metrics
    sector_returns = calculate_sector_returns(stocks_df, companies_df)
    sector_marketcap = calculate_sector_marketcap(companies_df)

    # Save results
    sector_returns.to_csv(os.path.join(save_path, "sector_performance.csv"), index=False)
    sector_marketcap.to_csv(os.path.join(save_path, "marketcap_by_sector.csv"), index=False)
