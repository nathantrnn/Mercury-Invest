import os
import sys
import logging
import subprocess

# Global path for logs
LOGS_PATH = "logs"


def configure_logging(log_filename, add_console=True):
    """
    Configure logging for the current script.
    Allows logging to both file and console.
    """
    try:
        os.makedirs(LOGS_PATH, exist_ok=True)
    except Exception as e:
        print(f"Failed to create logs directory: {e}")
        sys.exit(1)

    log_file = os.path.join(LOGS_PATH, log_filename)

    # Base configuration for logging to a file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=log_file,
    )

    # Add console handler, if requested
    if add_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logging.getLogger().addHandler(console_handler)

def setup_directories(paths):
    """
    Create the given list of paths if they don't exist.
    """
    for path in paths:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Directory created/verified: {path}")


def run_script_in_subprocess(script_path):
    """
    Run a script as a subprocess and pipe its output and errors to the parent logger.
    """
    try:
        logging.info(f"Executing script: {script_path}")
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate()

        # Log output and errors
        if stdout:
            logging.info(f"Output from {script_path}:\n{stdout}")
        if stderr:
            logging.error(f"Errors from {script_path}:\n{stderr}")

        if process.returncode != 0:
            raise RuntimeError(f"{script_path} failed with return code {process.returncode}")

        logging.info(f"Script {script_path} completed successfully.")
    except Exception as e:
        logging.error(f"Error while running {script_path}: {e}")
        sys.exit(1)
