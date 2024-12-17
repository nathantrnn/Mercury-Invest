import os
import kagglehub
from datetime import datetime

KAGGLE_DATASET = "andrewmvd/sp-500-stocks"
BRONZE_PATH = "data-lake/bronze/sp500_kaggle"


def ingest_sp500_kaggle():

    os.makedirs(BRONZE_PATH, exist_ok=True)

    # Download the Kaggle dataset.
    dataset_path = kagglehub.dataset_download(KAGGLE_DATASET)
    print("Kaggle dataset downloaded to:", dataset_path)

    today_str = datetime.now().strftime("%Y%m%d")

    # Move/Copy CSV files to BRONZE_PATH with date prefix
    for file_name in os.listdir(dataset_path):
        if file_name.endswith(".csv"):
            src = os.path.join(dataset_path, file_name)
            date_prefixed = f"{today_str}_{file_name}"
            dst = os.path.join(BRONZE_PATH, date_prefixed)

            with open(src, 'rb') as f_src, open(dst, 'wb') as f_dst:
                f_dst.write(f_src.read())

            print(f"Saved Kaggle CSV: {dst}")


if __name__ == "__main__":
    ingest_sp500_kaggle()
