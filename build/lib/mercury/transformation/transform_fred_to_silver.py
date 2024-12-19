import os
import pandas as pd
from mercury.config import Config
from mercury.utils import ensure_directory_exists, save_with_prefix


def transform_to_silver_fred(bronze_path, silver_path, date_threshold="1995-01-01"):
    """
    Transforms FRED Bronze data into Silver by cleaning and filtering.
    """
    ensure_directory_exists(silver_path)
    date_threshold_dt = pd.to_datetime(date_threshold)

    for file in os.listdir(bronze_path):
        if file.endswith(".csv"):
            try:
                bronze_file_path = os.path.join(bronze_path, file)
                df = pd.read_csv(bronze_file_path)

                # Clean and filter data
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
                df.dropna(subset=["Date", "Value"], inplace=True)
                df = df[df["Date"] >= date_threshold_dt]
                df["Value"] = df["Value"].round(2)

                if df.empty:
                    print(f"Skipped: No valid data in {file} after cleaning and filtering.")
                    continue

                # Rename 'Value' to indicator name
                cleaned_file_name = f"cleaned_{file.split('_', 1)[-1]}"
                indicator_name = cleaned_file_name.replace("cleaned_", "").replace(".csv", "")
                df.rename(columns={"Value": indicator_name}, inplace=True)

                save_with_prefix(df, silver_path, cleaned_file_name, prefix="")
                print(f"Transformed file saved to: {os.path.join(silver_path, cleaned_file_name)}\n")

            except Exception as e:
                print(f"Error processing {file}: {e}\n")


if __name__ == "__main__":
    bronze_path = Config.BRONZE_PATHS["fred"]
    silver_path = "data-lake/silver/macro"
    print(f"Transforming FRED data from Bronze ({bronze_path}) to Silver ({silver_path})...")
    transform_to_silver_fred(bronze_path, silver_path, date_threshold="1995-01-01")
    print("Transformation complete.")
