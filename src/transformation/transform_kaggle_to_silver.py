import os
import pandas as pd
from src.ingestion.config import Config


def handle_companies_data(df):
    """
    Cleans the SP500 Companies dataset.
    - Ensures valid data types for all columns.
    - Drops rows with missing essential fields like Symbol, Shortname, etc.
    - Fills in missing values for non-critical fields with defaults (if applicable).
    """
    # Data type enforcement
    df["Currentprice"] = pd.to_numeric(df["Currentprice"], errors="coerce")
    df["Marketcap"] = pd.to_numeric(df["Marketcap"], errors="coerce")
    df["Ebitda"] = pd.to_numeric(df["Ebitda"], errors="coerce")
    df["Revenuegrowth"] = pd.to_numeric(df["Revenuegrowth"], errors="coerce")
    df["Fulltimeemployees"] = pd.to_numeric(df["Fulltimeemployees"], errors="coerce")
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")

    # Drop rows with missing essential fields
    essential_columns = ["Symbol", "Shortname", "Sector", "Industry", "Currentprice"]
    df.dropna(subset=essential_columns, inplace=True)

    # Replace NaN or missing values in non-essential fields with defaults
    df["City"].fillna("Unknown", inplace=True)
    df["State"].fillna("Unknown", inplace=True)
    df["Country"].fillna("Unknown", inplace=True)
    df["Fulltimeemployees"].fillna(0, inplace=True)

    # Limit length of 'Longbusinesssummary' (if very long text exists)
    df["Longbusinesssummary"] = df["Longbusinesssummary"].str.slice(0, 500)

    return df


def handle_stocks_data(df):
    """
    Cleans the SP500 Stocks dataset.
    - Ensures valid data types for all columns.
    - Drops invalid rows (e.g., non-parsable dates or missing Symbol).
    """
    # Ensuring correct data types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Adj Close"] = pd.to_numeric(df["Adj Close"], errors="coerce")
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df["High"] = pd.to_numeric(df["High"], errors="coerce")
    df["Low"] = pd.to_numeric(df["Low"], errors="coerce")
    df["Open"] = pd.to_numeric(df["Open"], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # Drop rows with missing essential fields
    essential_columns = ["Date", "Symbol", "Adj Close", "Close", "High", "Low", "Open"]
    df.dropna(subset=essential_columns, inplace=True)

    # Filter: Make sure all dates are valid and after a certain threshold, if needed
    df = df[df["Date"] >= pd.Timestamp("2010-01-01")]

    numeric_columns = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]
    df[numeric_columns] = df[numeric_columns].round(4)

    return df


def handle_index_data(df):
    """
    Cleans the SP500 Index dataset.
    - Ensures valid data types.
    - Drops rows with invalid or missing values.
    """
    # Ensuring correct data types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["S&P500"] = pd.to_numeric(df["S&P500"], errors="coerce")

    # Drop rows with missing essential fields
    essential_columns = ["Date", "S&P500"]
    df.dropna(subset=essential_columns, inplace=True)

    return df

def transform_to_silver_kaggle(bronze_path, silver_path):
    """
    Transforms all Kaggle datasets (SP500 Companies, SP500 Stocks, SP500 Index) from the Bronze
    layer to the Silver layer by cleaning and reformatting data.
    """
    # Ensure Silver directory exists
    os.makedirs(silver_path, exist_ok=True)

    for file in os.listdir(bronze_path):
        if file.endswith(".csv"):
            try:
                bronze_file_path = os.path.join(bronze_path, file)

                # Remove the prefix (YYYYMMDD) and add 'cleaned_' to the filename
                if "_" in file and file[:8].isdigit():  # Check if first 8 characters are digits
                    cleaned_file_name = f"cleaned_{file.split('_', 1)[1]}"  # Remove the prefix entirely
                else:
                    cleaned_file_name = f"cleaned_{file}"  # For files without YYYYMMDD pattern

                silver_file_path = os.path.join(silver_path, cleaned_file_name)

                # Load Bronze data
                print(f"Processing: {bronze_file_path}")
                df = pd.read_csv(bronze_file_path)

                if "sp500_companies" in file:
                    # Handle SP500 Companies data
                    df = handle_companies_data(df)
                elif "sp500_stocks" in file:
                    # Handle SP500 Stocks data
                    df = handle_stocks_data(df)
                elif "sp500_index" in file:
                    # Handle SP500 Index data
                    df = handle_index_data(df)
                else:
                    print(f"Unknown file format: {file}. Skipping...")
                    continue

                # Validate that the data is not empty post-cleaning
                if df.empty:
                    print(f"No valid data remaining in {file}. Skipping...")
                    continue

                # Save transformed data to Silver directory with cleaned naming convention
                df.to_csv(silver_file_path, index=False)
                print(f"Transformed file saved to: {silver_file_path}\n")

            except Exception as e:
                print(f"Error processing {file}: {e}\n")


if __name__ == "__main__":
    bronze_path = Config.BRONZE_PATHS["kaggle"]  # Bronze Kaggle data directory
    silver_path = "data-lake/silver/stocks"  # Silver Kaggle data directory

    print(f"Transforming Kaggle data from Bronze ({bronze_path}) to Silver ({silver_path})...")
    transform_to_silver_kaggle(bronze_path, silver_path)
    print("Transformation complete.")
