import logging
import pathlib
from datetime import datetime

from git import Repo

from tool.config import CLONE_OPTIONS


class PathManager:
    """
    Manage the paths for the results.
    """

    def __init__(self, base_dir="results"):
        self.base_dir = pathlib.Path(base_dir)

    def create_folders(self, version_tag):
        """
        Create the folders for the results.
        """

        current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        folder_name = f"results_{current_time}"
        result_folder_path = self.base_dir / folder_name
        result_folder_path.mkdir(parents=True, exist_ok=True)

        json_directory = result_folder_path / "sscs" / version_tag
        json_directory.mkdir(parents=True, exist_ok=True)
        diff_directory = result_folder_path / "diff"
        diff_directory.mkdir(parents=True, exist_ok=True)

        return result_folder_path, json_directory, diff_directory


def clone_repo(project_repo_name, release_version=None, blobless=False):
    """
    Clone the repository for the given project and release version.

    Args:
        project_repo_name (str): The name of the project repository.
        release_version (str): The release version of the project.
        blobless (bool): Whether to clone the repository without blobs.

    Returns:
        str: The path to the cloned repository.
    """

    repo_url = f"https://github.com/{project_repo_name}.git"

    # Clone to /tmp folder; if it is already cloned, an error will be raised
    try:
        options = [CLONE_OPTIONS["blobless"]] if blobless else []
        Repo.clone_from(repo_url, f"/tmp/{project_repo_name}", multi_options=options)
    except Exception:
        # If the repo is already cloned, just fetch the latest changes
        logging.info("Repo already cloned. Fetching the latest changes...")
        repo = Repo(f"/tmp/{project_repo_name}")

        # Fetch the latest changes
        repo.remotes.origin.fetch()
    # Checkout to the release version if provided
    if release_version:
        repo = Repo(f"/tmp/{project_repo_name}")
        repo.git.checkout(release_version)

    return f"/tmp/{project_repo_name}"


def setup_logger(log_file_path, debug=False):
    """
    Setup the logger for the analysis.
    """

    class CustomFormatter(logging.Formatter):
        """Custom formatter, includes color coding for log levels."""

        grey = "\x1b[38;20m"
        green = "\x1b[38;2;0;200;0m"
        yellow = "\x1b[38;2;255;255;0m"
        red = "\x1b[38;2;255;0;0m"
        bold_red = "\x1b[1;31m"
        reset = "\x1b[0m"
        fmt = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"

        FORMATS = {
            logging.DEBUG: grey + fmt + reset,
            logging.INFO: green + fmt + reset,
            logging.WARNING: yellow + fmt + reset,
            logging.ERROR: red + fmt + reset,
            logging.CRITICAL: bold_red + fmt + reset,
        }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_fmt)
            return formatter.format(record)

    # Set up the logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING if not debug else logging.INFO)

    # Create a file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)

    # Create a formatter and set it for both handlers
    formatter = CustomFormatter()
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def get_package_url(package_name, package_manager):
    """Construct a human-readable package URL for the given package manager."""
    if package_manager == "maven":
        ga, v = package_name.split("@")
        g, a = ga.split(":")
        return f"https://central.sonatype.com/artifact/{g}/{a}/{v}"
    elif package_manager in ["npm", "yarn-berry", "yarn-classic", "pnpm"]:
        name_in_url = "/v/".join(package_name.rsplit("@", 1))  # replaces last occurrence of @ for /v/
        return f"https://npmjs.com/package/{name_in_url}"
    raise ValueError("Package Manager not supported for acquiring package URL.")


def get_registry_url(package_name, package_manager):
    """Construct a registry API URL for the given package manager."""
    if package_manager == "maven":
        ga, v = package_name.split("@")
        g, a = ga.split(":")
        return f"https://central.sonatype.com/artifact/{g}/{a}/{v}"
    elif package_manager in ["npm", "yarn-berry", "yarn-classic", "pnpm"]:
        name_in_url = "/".join(package_name.rsplit("@", 1))  # replaces last occurrence of @ for /
        return f"https://registry.npmjs.com/{name_in_url}"
    raise ValueError("Package Manager not supported for acquiring registry URL.")
