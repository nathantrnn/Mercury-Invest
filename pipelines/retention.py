import os
from collections import defaultdict


def apply_retention(directory, retention_limit=4):
    print(f"Applying retention policy in: {directory}")

    grouped_files = defaultdict(list)

    # Loop through all CSV files
    for file in os.listdir(directory):
        if file.endswith(".csv") and not file.startswith("."):
            _, indicator = file.split("_", 1)
            grouped_files[indicator].append(os.path.join(directory, file))

    for indicator, files in grouped_files.items():
        files = sorted(files, key=os.path.getctime, reverse=True)

        # Keep recent files and delete the rest
        for file in files[retention_limit:]:
            os.remove(file)
            print(f"Deleted: {file}")

    print(f"Retention complete for: {directory}\n")


if __name__ == "__main__":
    for layer in ["data-lake/bronze/kaggle", "data-lake/bronze/fred"]:
        apply_retention(layer)
