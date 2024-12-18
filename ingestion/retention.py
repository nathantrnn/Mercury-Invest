import os
from collections import defaultdict


def apply_retention(directory, retention_limit=4):
    print(f"Applying retention in: {directory}")

    grouped_files = defaultdict(list)

    # Group files by indicator
    for file in os.listdir(directory):
        if file.endswith(".csv") and not file.startswith("."):
            _, indicator = file.split("_", 1)
            grouped_files[indicator].append(os.path.join(directory, file))

    # Retain only the newest files per indicator
    for indicator, files in grouped_files.items():
        for file in sorted(files, key=os.path.getctime, reverse=True)[retention_limit:]:
            try:
                os.remove(file)
                print(f"Deleted old file: {file}")
            except Exception as e:
                print(f"Error deleting {file}: {e}")

    print(f"Retention completed for: {directory}\n")


if __name__ == "__main__":
    for folder in ["data-lake/bronze/kaggle", "data-lake/bronze/fred"]:
        apply_retention(folder)
