import os
import pandas as pd
from datetime import datetime
from fredapi import Fred

BRONZE_PATH = "data-lake/bronze/fred_data"


INDICATORS = ["FEDFUNDS", "CPIAUCSL", "GDP", "UNRATE", "DGS10"]

def ingest_fred_data():

    fred_api_key = os.environ.get("FRED_API_KEY")
    if not fred_api_key:
        raise EnvironmentError("FRED_API_KEY not set in environment.")

    fred = Fred(api_key=fred_api_key)
    os.makedirs(BRONZE_PATH, exist_ok=True)

    today_str = datetime.now().strftime("%Y%m%d")

    for series_id in INDICATORS:
        print(f"Fetching {series_id} from FRED...")
        data_series = fred.get_series(series_id)
        # data_series is a pandas Series with Date as index
        df = pd.DataFrame(data_series, columns=["Value"])
        df["Date"] = df.index
        df.reset_index(drop=True, inplace=True)
        df["SeriesID"] = series_id

        file_name = f"{today_str}_{series_id}.csv"
        csv_path = os.path.join(BRONZE_PATH, file_name)
        df.to_csv(csv_path, index=False)
        print(f"Saved {series_id} to {csv_path}")

if __name__ == "__main__":
    ingest_fred_data()
