import os
import sys
import logging
import subprocess
from logging.handlers import RotatingFileHandler

LOGS_PATH = "../logs"


def configure_logging(log_filename, add_console=True):
    """
    Configure logging for a script to output to both file and console.
    """
    try:
        os.makedirs(LOGS_PATH, exist_ok=True)
    except Exception as e:
        print(f"Failed to create logs directory: {e}")
        sys.exit(1)

    log_file = os.path.join(LOGS_PATH, log_filename)
    handlers = [RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)]
    if add_console:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def setup_directories(paths):
    """
    Ensure directories exist.
    """
    for path in paths:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Directory verified/created: {path}")


def run_script_in_subprocess(script_path, retry_count=3):
    """
    Run a script as a subprocess with retries.
    """
    attempt = 0
    while attempt < retry_count:
        try:
            logging.info(f"Executing script: {script_path} (Attempt {attempt + 1}/{retry_count})")
            process = subprocess.Popen(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()

            if stdout.strip():
                logging.info(f"Output from {script_path}:\n{stdout.strip()}")

            if process.returncode == 0:
                if stderr.strip():
                    logging.warning(f"Warnings from {script_path}:\n{stderr.strip()}")
                logging.info(f"Script {script_path} completed successfully.")
                return
            else:
                raise RuntimeError(f"Script {script_path} failed with code {process.returncode}")
        except Exception as e:
            attempt += 1
            logging.error(f"Error executing {script_path}: {e}")
            if attempt >= retry_count:
                logging.critical(f"Script {script_path} failed after {retry_count} retries.")


def clean_directory(directory, retention_limit=4, file_extension=".csv"):
    """
    Retain only the latest `retention_limit` files in a directory.
    """
    try:
        files = [f for f in os.listdir(directory) if f.endswith(file_extension)]
        files_sorted = sorted(
            files, key=lambda x: os.path.getctime(os.path.join(directory, x)), reverse=True
        )
        for file_to_delete in files_sorted[retention_limit:]:
            os.remove(os.path.join(directory, file_to_delete))
            logging.info(f"Deleted old file: {file_to_delete}")
    except Exception as e:
        logging.error(f"Failed to clean up directory {directory}: {e}")


def move_files(source, destination, prefix=""):
    """
    Move files from source to destination with an optional prefix.
    """
    try:
        os.makedirs(destination, exist_ok=True)
        for file_name in os.listdir(source):
            source_file = os.path.join(source, file_name)
            dest_file = os.path.join(destination, f"{prefix}_{file_name}" if prefix else file_name)
            os.rename(source_file, dest_file)
            logging.info(f"Moved file: {source_file} -> {dest_file}")
    except Exception as e:
        logging.error(f"Error moving files from {source} to {destination}: {e}")
