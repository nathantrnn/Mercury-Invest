import logging
from mercury.ingestion.ingest_fred import ingest_fred

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def main():
    logging.info("Starting the data pipeline...")
    ingest_fred()
    logging.info("Pipeline completed!")


if __name__ == "__main__":
    main()
    