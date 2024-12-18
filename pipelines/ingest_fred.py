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
            df = (
                pd.DataFrame(fred.get_series(indicator))
                .reset_index()
                .rename(columns={0: "Date", "index": "Value"})
            )
            filepath = os.path.join(fred_path, f"{today_date}_{indicator}.csv")
            df.to_csv(filepath, index=False)
            print(f"Saved FRED data: {filepath}")
        except Exception as e:
            print(f"Error with {indicator}: {e}")


if __name__ == "__main__":
    ingest_fred()
