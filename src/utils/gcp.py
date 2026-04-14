from pathlib import Path
import os
from dotenv import load_dotenv
from src.utils.paths import get_project_root


def setup_gcp_credentials() -> None:
    load_dotenv()

    project_root = get_project_root()

    credentials_relative_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_relative_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not found in .env")

    credentials_path = project_root / credentials_relative_path
    if not credentials_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)

def get_gcp_config():
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("GCP_DATASET_ID")

    if not project_id:
        raise ValueError("GCP_PROJECT_ID not set")

    if not dataset:
        raise ValueError("GCP_DATASET_ID not set")

    return project_id, dataset