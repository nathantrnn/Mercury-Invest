import os
import pandas as pd
from mercury.config import Config


def handle_companies_data(df):

    # Numeric columns to clean
    numeric_columns = ["Currentprice", "Marketcap", "Ebitda", "Revenuegrowth", "Fulltimeemployees", "Weight"]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    # Drop rows with missing essential columns
    essential_columns = ["Symbol", "Shortname", "Sector", "Industry", "Currentprice"]
    df.dropna(subset=essential_columns, inplace=True)

    # Replace NaN for non-essential fields
    defaults = {"City": "Unknown", "State": "Unknown", "Country": "Unknown", "Fulltimeemployees": 0}
    df.fillna(value=defaults, inplace=True)

    # Trim 'Longbusinesssummary' to 500 characters
    df["Longbusinesssummary"] = df["Longbusinesssummary"].str.slice(0, 500)

    return df


def handle_stocks_data(df):

    # Format and validate data
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    numeric_columns = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    # Drop rows with missing essential fields
    essential_columns = ["Date", "Symbol"] + numeric_columns[:-1]  # Exclude "Volume"
    df.dropna(subset=essential_columns, inplace=True)

    # Filter on Date and round numeric columns
    df = df[df["Date"] >= pd.Timestamp("2010-01-01")]
    df[numeric_columns] = df[numeric_columns].round(4)

    return df


def handle_index_data(df):

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["S&P500"] = pd.to_numeric(df["S&P500"], errors="coerce")

    # Drop rows with missing essential fields
    df.dropna(subset=["Date", "S&P500"], inplace=True)

    # Round the numeric column
    df["S&P500"] = df["S&P500"].round(4)

    return df


def transform_to_silver_kaggle(bronze_path, silver_path):

    # Ensure Silver directory exists
    os.makedirs(silver_path, exist_ok=True)

    # Map file types to their respective handlers
    handlers = {
        "sp500_companies": handle_companies_data,
        "sp500_stocks": handle_stocks_data,
        "sp500_index": handle_index_data,
    }

    for file in os.listdir(bronze_path):
        if file.endswith(".csv"):
            try:
                bronze_file_path = os.path.join(bronze_path, file)

                # Standardize filename: Replace `raw_` with `cleaned_`
                cleaned_file_name = f"cleaned_{file.removeprefix('raw_')}"
                silver_file_path = os.path.join(silver_path, cleaned_file_name)

                # Process dataset
                print(f"Processing: {bronze_file_path}")
                df = pd.read_csv(bronze_file_path)

                # Identify and process files based on their type
                for key, handler in handlers.items():
                    if key in file:
                        df = handler(df)
                        break
                else:
                    print(f"Unknown file format in '{file}'. Skipping...\n")
                    continue

                # Validate that data isn't empty post-cleaning
                if df.empty:
                    print(f"No valid data remaining in '{file}'. Skipping...\n")
                    continue

                # Save cleaned dataset
                df.to_csv(silver_file_path, index=False)
                print(f"Transformed file saved to: {silver_file_path}\n")

            except Exception as e:
                print(f"Error processing '{file}': {e}\n")


if __name__ == "__main__":
    bronze_path = Config.BRONZE_PATHS["kaggle"]  # Bronze Kaggle data directory
    silver_path = "data-lake/silver/stocks"  # Silver Kaggle data directory

    print(f"Transforming Kaggle data from Bronze ({bronze_path}) to Silver ({silver_path})...")
    transform_to_silver_kaggle(bronze_path, silver_path)
    print("Transformation complete.")
