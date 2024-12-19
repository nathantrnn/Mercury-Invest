from mercury.ingestion.ingest_fred import ingest_fred


def main():
    print("Starting the data pipeline...")
    ingest_fred()
    print("Pipeline completed!")


if __name__ == "__main__":
    main()
