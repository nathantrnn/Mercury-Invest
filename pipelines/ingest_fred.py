import os
from datetime import datetime
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv  # NEW: Import dotenv

load_dotenv()


def ingest_fred():
    fred_path = "data-lake/bronze/fred"
    os.makedirs(fred_path, exist_ok=True)

    api_key = os.getenv("FRED_API_KEY")  # Load API key from environment
    if not api_key:
        raise ValueError("FRED_API_KEY is missing. Check your .env file or environment variables.")

    indicators = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]
    fred = Fred(api_key=api_key)
    today_date = datetime.now().strftime("%Y%m%d")

    for indicator in indicators:
        try:
            data = fred.get_series(indicator)
            df = pd.DataFrame(data).reset_index()
            df.columns = ["Date", "Value"]

            filename = os.path.join(fred_path, f"{today_date}_{indicator}.csv")
            df.to_csv(filename, index=False)
            print(f"FRED data saved: {filename}")

        except Exception as e:
            print(f"Error fetching indicator {indicator}: {e}")


if __name__ == "__main__":
    ingest_fred()
