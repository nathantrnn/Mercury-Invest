import os
import pandas as pd
from ingestion.config import Config


def transform_to_silver(bronze_path, silver_path, date_threshold="1995-01-01"):
    """
    Transforms Bronze data files into Silver layer by cleaning and filtering.
    """
    # Ensure Silver directory exists
    os.makedirs(silver_path, exist_ok=True)

    for file in os.listdir(bronze_path):
        if file.endswith(".csv"):
            try:
                bronze_file_path = os.path.join(bronze_path, file)

                # Remove the prefix (YYYYMMDD) and add 'cleaned_' to the filename
                if "_" in file and file[:8].isdigit():  # Check if the first 8 characters are digits
                    cleaned_file_name = f"cleaned_{file.split('_', 1)[1]}"  # Remove the prefix entirely
                else:
                    cleaned_file_name = f"cleaned_{file}"  # For files without YYYYMMDD pattern

                silver_file_path = os.path.join(silver_path, cleaned_file_name)

                # Load the Bronze data
                df = pd.read_csv(bronze_file_path)

                # Clean and filter data
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
                df.dropna(subset=["Date", "Value"], inplace=True)
                date_threshold_dt = pd.to_datetime(date_threshold)
                df = df[df["Date"] >= date_threshold_dt]
                df["Value"] = df["Value"].round(2)

                # Rename the 'Value' column to match the macroeconomic indicator name
                indicator_name = cleaned_file_name.replace("cleaned_", "").replace(".csv", "")
                df.rename(columns={"Value": indicator_name}, inplace=True)

                # Save transformed data to Silver layer
                if not df.empty:
                    df.to_csv(silver_file_path, index=False)
                    print(f"Transformed file saved to: {silver_file_path}")
                else:
                    print(f"Skipped: No valid data in {file} after cleaning and filtering.")

            except Exception as e:
                print(f"Error processing {file}: {e}")


if __name__ == "__main__":
    bronze_path = Config.BRONZE_PATHS["fred"]
    silver_path = "data-lake/silver/macro"

    print(f"Transforming data from Bronze ({bronze_path}) to Silver ({silver_path})...")
    transform_to_silver(bronze_path, silver_path, date_threshold="1995-01-01")
    print("Transformation complete.")
