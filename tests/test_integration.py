import os
import re
import pytest
from datetime import datetime

from pipelines.ingest_kaggle import ingest_sp500_kaggle, BRONZE_PATH as KAGGLE_BRONZE
from pipelines.ingest_fred import ingest_fred_data, BRONZE_PATH as FRED_BRONZE, INDICATORS

@pytest.fixture(scope="module")
def check_env():

    fred_key = os.environ.get("FRED_API_KEY")
    if not fred_key:
        pytest.skip("FRED_API_KEY not set in environment, skipping integration test.")
    return True


@pytest.fixture
def temp_kaggle_dir(tmp_path):

    test_dir = tmp_path / "sp500_kaggle"
    test_dir.mkdir(parents=True, exist_ok=True)
    yield test_dir

@pytest.fixture
def temp_fred_dir(tmp_path):

    test_dir = tmp_path / "fred_data"
    test_dir.mkdir(parents=True, exist_ok=True)
    yield test_dir


def test_kaggle_integration(check_env, temp_kaggle_dir):
    """
    Makes a real call to Kaggle dataset 'andrewmvd/sp-500-stocks', verifying
    the presence of CSV files with the correct naming prefix (YYYYMMDD_*) and non-empty data.
    """
    # Override the default Bronze path
    original_bronze_path = KAGGLE_BRONZE
    try:
        from pipelines.ingest_kaggle import ingest_sp500_kaggle
        ingest_sp500_kaggle.__globals__["BRONZE_PATH"] = str(temp_kaggle_dir)

        # Run the real ingestion
        ingest_sp500_kaggle()

        # Check CSV files in the temp directory
        files = list(temp_kaggle_dir.glob("*.csv"))
        assert len(files) >= 3, "Expected at least 3 CSV files from the Kaggle dataset."

        today_str = datetime.now().strftime("%Y%m%d")
        for f in files:
            assert re.match(rf"^{today_str}_.*\.csv", f.name), f"File {f.name} is missing prefix YYYYMMDD_"
            # Minimal check: ensure the file isn't empty
            lines = f.read_text().splitlines()
            assert len(lines) > 1, f"{f.name} appears empty or missing content."

    finally:
        # Restore original BRONZE_PATH
        from pipelines.ingest_kaggle import BRONZE_PATH
        ingest_sp500_kaggle.__globals__["BRONZE_PATH"] = original_bronze_path


def test_fred_integration(check_env, temp_fred_dir):
    """
    Makes a real call to FRED using the environment FRED_API_KEY, verifying
    CSV file structure (columns) and naming prefix.
    """
    original_fred_path = FRED_BRONZE
    try:
        from pipelines.ingest_fred import ingest_fred_data
        ingest_fred_data.__globals__["BRONZE_PATH"] = str(temp_fred_dir)

        # Run the real ingestion
        ingest_fred_data()

        files = list(temp_fred_dir.glob("*.csv"))
        assert len(files) == len(INDICATORS), f"Expected {len(INDICATORS)} CSV files, got {len(files)}."

        today_str = datetime.now().strftime("%Y%m%d")
        for f in files:
            assert re.match(rf"^{today_str}_.*\.csv", f.name), f"File {f.name} missing prefix YYYYMMDD_"

            lines = f.read_text().splitlines()
            # basic check for columns: 'Value', 'Date', 'SeriesID'
            headers = lines[0].split(",")
            for col in ["Value", "Date", "SeriesID"]:
                assert col in headers, f"{col} not found in {f.name}"

            assert len(lines) > 1, f"{f.name} might be empty (no data from FRED?)."

    finally:
        from pipelines.ingest_fred import BRONZE_PATH
        ingest_fred_data.__globals__["BRONZE_PATH"] = original_fred_path
