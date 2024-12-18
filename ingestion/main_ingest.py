from ingest_kaggle import ingest_kaggle
from ingest_fred import ingest_fred
from retention import apply_retention
from config import Config


def main_pipeline():
    print("Starting data pipeline...")

    # Bronze layer: Ingest data
    ingest_kaggle()
    ingest_fred()

    # Apply retention policy to Bronze layers
    for name, path in Config.BRONZE_PATHS.items():
        print(f"Applying retention for: {name}")
        apply_retention(path, Config.RETENTION_LIMIT)

    print("Pipeline completed!")


if __name__ == "__main__":
    main_pipeline()
