import json
import logging
import os

DEFAULT_ENABLED_CHECKS = {
    "source_code": True,
    "source_code_sha": True,
    "deprecated": True,
    "forks": False,
    "provenance": True,
    "code_signature": True,
    "aliased_packages": True,
}

CLONE_OPTIONS = {
    "blobless": "--filter=blob:none",
}

DEFAULT_CONFIG_PATH = ".dirty-waters.json"
DEFAULT_CONFIG = {"ignore": {}, "revisions": {}}


def load_config(config_path=None):
    """
    Load configuration from a JSON file.

    Args:
        config_path (str): Path to config file. If None, looks for .dirty-waters.json in current directory

    Returns:
        dict: Configuration dictionary
    """
    if not config_path:
        logging.info(f"No config file provided, using default config path: {DEFAULT_CONFIG_PATH}")
        config_path = DEFAULT_CONFIG_PATH

    if not os.path.exists(config_path):
        logging.warning(f"Config file not found at {config_path}, using default config: {DEFAULT_CONFIG}")
        return DEFAULT_CONFIG

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            logging.info(f"Found config file at {config_path}")
            return {**DEFAULT_CONFIG, **config}
    except Exception as e:
        logging.warning(f"Error loading config file: {str(e)}")
        return DEFAULT_CONFIG
