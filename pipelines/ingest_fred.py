import os
from datetime import datetime
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
from config import Config

load_dotenv()


def ingest_fred():
    fred_path = Config.BRONZE_PATHS["fred"]
    os.makedirs(fred_path, exist_ok=True)

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("Missing `FRED_API_KEY`. Check `.env` configuration.")

    indicators = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]
    fred = Fred(api_key=api_key)
    today_date = datetime.now().strftime("%Y%m%d")

    for indicator in indicators:
        try:
            # Fetch data from FRED API
            df = (
                pd.DataFrame(fred.get_series(indicator))  # Fetch series data
                .reset_index()  # Reset the index to get dates as a column
                .rename(columns={"index": "Date", 0: "Value"})  # Correct column renaming
            )

            # Ensure Date column is parsed correctly and enforce data types
            df["Date"] = pd.to_datetime(df["Date"])  # Ensure Date is a datetime
            df["Value"] = pd.to_numeric(df["Value"], errors="coerce")  # Coerce Value to numeric

            # Save to the file system
            filepath = os.path.join(fred_path, f"{today_date}_{indicator}.csv")
            df.to_csv(filepath, index=False)
            print(f"Saved FRED data: {filepath}")
        except Exception as e:
            print(f"Error with {indicator}: {e}")


if __name__ == "__main__":
    ingest_fred()
