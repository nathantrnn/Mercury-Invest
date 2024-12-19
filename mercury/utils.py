import os
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def ensure_directory_exists(path):
    """Ensure the directory exists, creating it if necessary."""
    Path(path).mkdir(parents=True, exist_ok=True)
    logging.info(f"Directory validated or created: {path}")


def validate_path(file_path):
    """Check if a file exists and return its path."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File/path not found: {file_path}")
    return file_path


def load_csv(file_path):
    """Load a CSV file into a DataFrame."""
    try:
        df = pd.read_csv(validate_path(file_path))
        logging.info(f"Loaded {len(df)} rows from '{file_path}'")
        return df
    except Exception as e:
        logging.error(f"Failed to load CSV '{file_path}': {e}")
        raise


def save_csv(df, path, file_name, prefix=""):
    """Save a DataFrame to a CSV file with an optional prefix."""
    try:
        ensure_directory_exists(path)
        file_name = f"{prefix}{file_name}" if prefix else file_name
        df.to_csv(os.path.join(path, file_name), index=False)
        logging.info(f"Data saved to '{os.path.join(path, file_name)}'")
    except Exception as e:
        logging.error(f"Failed to save CSV '{file_name}': {e}")
        raise


def batch_process_csv(dataframes, filenames, save_path, prefix=""):
    """Save multiple DataFrames to CSV files."""
    for df, file_name in zip(dataframes, filenames):
        save_csv(df, save_path, file_name, prefix)


def load_multiple_csv(paths):
    """Load multiple CSV files into a list of DataFrames."""
    return [load_csv(path) for path in paths]

def save_dataframes(dataframes, filenames, path, prefix=""):
    """
    Save multiple DataFrames to CSV files.
    """
    if len(dataframes) != len(filenames):
        raise ValueError("Number of DataFrames must match the number of filenames.")

    ensure_directory_exists(path)
    for df, filename in zip(dataframes, filenames):
        save_csv(df, path, filename, prefix)
