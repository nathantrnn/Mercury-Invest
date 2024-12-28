import os
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def ensure_directory_exists(path: str):
    """
    Ensure the directory exists, creating it if necessary.
    """
    Path(path).mkdir(parents=True, exist_ok=True)
    logging.info(f"Directory validated or created: {path}")


def validate_path(file_path: str) -> str:
    """
    Check if a file exists and return its path.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File/path not found: {file_path}")
    return file_path


def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load a CSV file into a DataFrame with basic validation.
    """
    try:
        df = pd.read_csv(validate_path(file_path))
        logging.info(f"Loaded {len(df)} rows from '{file_path}'")
        return df
    except Exception as e:
        logging.error(f"Failed to load CSV '{file_path}': {e}")
        raise


def clean_dataframe(df: pd.DataFrame, date_col: str = "Date", value_col: str = "Value") -> pd.DataFrame:

    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        df.dropna(subset=[date_col, value_col], inplace=True)
        df.sort_values(by=date_col, inplace=True)
        logging.info(f"Cleaned DataFrame: {len(df)} rows remaining.")
        return df
    except Exception as e:
        logging.error("Failed to clean DataFrame: {e}")
        raise


def save_csv(df: pd.DataFrame, path: str, file_name: str, prefix: str = ""):

    try:
        ensure_directory_exists(path)
        file_name_with_prefix = f"{prefix}{file_name}" if prefix else file_name
        target_path = os.path.join(path, file_name_with_prefix)
        df.to_csv(target_path, index=False)
        logging.info(f"Data saved to '{target_path}'")
    except Exception as e:
        logging.error(f"Failed to save CSV '{file_name}': {e}")
        raise


def compute_percentage_change(df: pd.DataFrame, value_col: str, periods: int = 1,
                              new_col: str = "Percentage_Change") -> pd.DataFrame:

    df[new_col] = df[value_col].pct_change(periods=periods) * 100
    return df


def compute_difference(df: pd.DataFrame, value_col: str, new_col: str = "Difference") -> pd.DataFrame:

    df[new_col] = df[value_col].diff()
    return df
