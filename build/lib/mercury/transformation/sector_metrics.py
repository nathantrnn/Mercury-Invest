import logging
from mercury.utils import load_multiple_csv, batch_process_csv


def calculate_sector_returns(stocks, companies):
    merged_df = stocks.merge(companies[["Symbol", "Sector"]], on="Symbol")
    return merged_df.groupby("Sector")["Cumulative Returns"].mean().reset_index(name="Avg Sector Returns")


def calculate_sector_marketcap(companies):
    return companies.groupby("Sector")["Marketcap"].sum().reset_index(name="Total Marketcap")


def calculate_and_save_sector_metrics(stocks_path, companies_path, save_path):
    try:
        logging.info("Loading data...")
        stocks_df, companies_df = load_multiple_csv([stocks_path, companies_path])

        if stocks_df.empty or companies_df.empty:
            raise ValueError("Stocks or companies data is empty.")

        # Calculate and Save Metrics
        sector_returns = calculate_sector_returns(stocks_df, companies_df)
        sector_marketcap = calculate_sector_marketcap(companies_df)

        save_dataframes(
            [sector_returns, sector_marketcap],
            ["sector_performance.csv", "marketcap_by_sector.csv"],
            save_path
        )
        logging.info("Sector metrics saved successfully.")
    except Exception as e:
        logging.error(f"Error during sector metrics calculation: {e}")
        raise
