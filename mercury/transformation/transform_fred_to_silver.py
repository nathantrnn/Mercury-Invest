import os
import logging
import pandas as pd
from mercury.config import Config
from mercury.utils import (
    load_csv,
    clean_dataframe,
    save_csv,
    compute_percentage_change,
    compute_difference
)
  # Use centralized configuration paths

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

BRONZE_PATH = Config.BRONZE_PATHS["fred"]
SILVER_PATH = os.path.join(Config.DATA_LAKE_PATHS["silver"], "macro")


def transform_fred_to_silver():
    # Paths to raw FRED data
    gdp_file = os.path.join(BRONZE_PATH, "raw_GDP.csv")
    cpi_file = os.path.join(BRONZE_PATH, "raw_CPIAUCSL.csv")
    fedfunds_file = os.path.join(BRONZE_PATH, "raw_FEDFUNDS.csv")
    unrate_file = os.path.join(BRONZE_PATH, "raw_UNRATE.csv")
    dgs10_file = os.path.join(BRONZE_PATH, "raw_DGS10.csv")

    # Load and clean each dataset
    # GDP: Quarterly data
    gdp_df = clean_dataframe(load_csv(gdp_file), date_col="Date", value_col="Value")

    # CPI: Monthly data
    cpi_df = clean_dataframe(load_csv(cpi_file), date_col="Date", value_col="Value")

    # Fed Funds: Monthly data
    fedfunds_df = clean_dataframe(load_csv(fedfunds_file), date_col="Date", value_col="Value")

    # UNRATE: Monthly data
    unrate_df = clean_dataframe(load_csv(unrate_file), date_col="Date", value_col="Value")

    # DGS10: Daily data
    dgs10_df = clean_dataframe(load_csv(dgs10_file), date_col="Date", value_col="Value")

    # Compute GDP Growth (quarter-over-quarter)
    # periods=1 gives the % change from one quarter to the next
    gdp_df = compute_percentage_change(gdp_df, value_col="Value", periods=1, new_col="GDP_Growth")

    # Compute YoY Inflation using CPI (year-over-year, periods=12 for monthly data)
    cpi_df = compute_percentage_change(cpi_df, value_col="Value", periods=12, new_col="Inflation_YoY")

    # Compute month-over-month difference in Fed Funds Rate
    fedfunds_df = compute_difference(fedfunds_df, value_col="Value", new_col="FedFunds_Change")

    # Compute day-over-day difference in 10-year Treasury Yield
    dgs10_df = compute_difference(dgs10_df, value_col="Value", new_col="DGS10_Change")

    # Rename columns for clarity
    gdp_df.rename(columns={"Value": "GDP"}, inplace=True)
    cpi_df.rename(columns={"Value": "CPI"}, inplace=True)
    fedfunds_df.rename(columns={"Value": "FedFunds"}, inplace=True)
    unrate_df.rename(columns={"Value": "UNRATE"}, inplace=True)
    dgs10_df.rename(columns={"Value": "DGS10"}, inplace=True)

    # Merge all dataframes on Date with an outer join
    df_merged = gdp_df.merge(cpi_df[["Date", "CPI", "Inflation_YoY"]], on="Date", how="outer")
    df_merged = df_merged.merge(fedfunds_df[["Date", "FedFunds", "FedFunds_Change"]], on="Date", how="outer")
    df_merged = df_merged.merge(unrate_df[["Date", "UNRATE"]], on="Date", how="outer")
    df_merged = df_merged.merge(dgs10_df[["Date", "DGS10", "DGS10_Change"]], on="Date", how="outer")

    # Sort by Date after merging
    df_merged.sort_values("Date", inplace=True)

    # Filter data to include only rows after 01/01/1995
    start_date = pd.Timestamp("1995-01-01")
    df_merged = df_merged[df_merged["Date"] >= start_date]

    if df_merged.empty:
        logging.warning("No data available after 1995-01-01. Skipping save.")
    else:
        # Save final dataset to silver
        save_csv(df_merged, path=SILVER_PATH, file_name="cleaned_macro_indicators.csv")
        logging.info("Transformation to silver (macro) completed successfully.")


if __name__ == "__main__":
    transform_fred_to_silver()
