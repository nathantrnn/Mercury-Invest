import logging
from scripts.utils import configure_logging, setup_directories, run_script_in_subprocess
from scripts.config import BRONZE_PATHS

SCRIPTS = [
    {"name": "Kaggle Ingestion", "script": "scripts/ingest_kaggle.py"},
    {"name": "FRED Ingestion", "script": "scripts/ingest_fred.py"},
    {"name": "Retention Policy", "script": "scripts/retention.py"},
]


def main():
    logging.info("==== Starting Main Ingestion Pipeline ====")

    try:
        setup_directories(BRONZE_PATHS.values())
        for task in SCRIPTS:
            logging.info(f"Running task: {task['name']}")
            run_script_in_subprocess(task["script"])
        logging.info("==== Main Ingestion Pipeline Completed Successfully ====")
    except Exception as e:
        logging.critical(f"Fatal error in pipeline: {e}")
        exit(1)


if __name__ == "__main__":
    configure_logging("main_ingestion.log")
    main()
