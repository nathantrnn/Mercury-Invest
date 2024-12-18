from ingest_kaggle import ingest_kaggle
from ingest_fred import ingest_fred
from retention import apply_retention


def main_pipeline():
    print("Starting data pipeline...")

    # Bronze layer: Ingest data
    ingest_kaggle()
    ingest_fred()

    # Apply retention policy to Bronze layer
    apply_retention("data-lake/bronze/kaggle")
    apply_retention("data-lake/bronze/fred")

    print("Pipeline completed!")

if __name__ == "__main__":
    main_pipeline()
