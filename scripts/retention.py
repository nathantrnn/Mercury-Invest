import os
import re

def retain_latest_n_files(folder_path, n=4):

    if not os.path.exists(folder_path):
        return

    all_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    # Extract YYYYMMDD from each filename, e.g. 20231202_sp500_stocks.csv
    def get_date_prefix(filename):
        match = re.match(r'^(\d{8})_', filename)
        return match.group(1) if match else None

    # Pair each file with its date prefix
    files_with_dates = []
    for fname in all_files:
        prefix = get_date_prefix(fname)
        if prefix:
            files_with_dates.append((fname, prefix))

    # Sort by date prefix descending
    files_with_dates.sort(key=lambda x: x[1], reverse=True)

    # Keep only the n newest
    if len(files_with_dates) > n:
        to_delete = files_with_dates[n:]
        for fname, _ in to_delete:
            full_path = os.path.join(folder_path, fname)
            print(f"Deleting old file: {full_path}")
            os.remove(full_path)

def apply_retention():

    sp500_folder = "data-lake/bronze/sp500_kaggle"
    fred_folder = "data-lake/bronze/fred_data"

    print("Applying retention policy (4 copies) to Kaggle folder...")
    retain_latest_n_files(sp500_folder, 4)

    print("Applying retention policy (4 copies) to FRED folder...")
    retain_latest_n_files(fred_folder, 4)


if __name__ == "__main__":
    apply_retention()
