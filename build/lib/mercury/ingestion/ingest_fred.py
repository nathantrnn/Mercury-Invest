import os
from fredapi import Fred
from dotenv import load_dotenv
from mercury.config import Config
from mercury.utils import save_csv

load_dotenv()


def ingest_fred():
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("Missing `FRED_API_KEY`. Check `.env` configuration.")

    fred = Fred(api_key=api_key)

    try:
        for indicator in Config.INDICATORS:
            df = fred.get_series(indicator).reset_index(name="Value")
            df.rename(columns={"index": "Date"}, inplace=True)
            save_csv(df, Config.BRONZE_PATHS["fred"], f"{indicator}.csv", prefix="raw_")
    except Exception as e:
        print(f"Error processing indicator: {e}")


def main():
    print("Starting FRED data ingestion...")
    ingest_fred()
    print("FRED data ingestion completed!")


if __name__ == "__main__":
    main()
