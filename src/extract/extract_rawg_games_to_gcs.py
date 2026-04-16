import json
import math
import os

import requests
from dotenv import load_dotenv
from google.cloud import storage


load_dotenv()

def parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if value == "" or value.lower() in {"null", "none"}:
        return None
    return int(value)

# Environment variables and configuration
# GCP CREDENTIALS and CONFIG
API_KEY = os.getenv("RAWG_API_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW_PREFIX = os.getenv("GCS_RAW_PREFIX")
# Parameters for API request
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
PAGE_SIZE = 40
MAX_PAGES = parse_optional_int(os.getenv("MAX_PAGES"))
ORDERING = "-added"


if not API_KEY:
    raise ValueError("RAWG_API_KEY not found in environment variables")

if not GCS_BUCKET:
    raise ValueError("GCS_BUCKET not found in environment variables")

if not GCS_RAW_PREFIX:
    raise ValueError("GCS_RAW_PREFIX not found in environment variables")

if not START_DATE:
    raise ValueError("START_DATE not found in environment variables")

if not END_DATE:
    raise ValueError("END_DATE not found in environment variables")

print(f"MAX_PAGES parsed: {MAX_PAGES}")


start_year = START_DATE[:4]
end_year = END_DATE[:4]

YEAR = start_year

# Cleaned date strings for file naming
START_DATE_CLEAN = START_DATE.replace("-", "")
END_DATE_CLEAN = END_DATE.replace("-", "")



url = "https://api.rawg.io/api/games"

params = {
    "key": API_KEY,
    "dates": f"{START_DATE},{END_DATE}",
    "ordering": ORDERING,
    "page_size": PAGE_SIZE,
}

# GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)

# Info for validation
total_games = 0
pages_processed = 0
page = 1
total_results = None
total_pages = None
requests_count = 0

YEAR = START_DATE[:4]

while url:
    if MAX_PAGES is not None and pages_processed >= MAX_PAGES:
        break
    
    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 404:
        print(f"Reached last available page at page={page}. RAWG returned 404.")
        break

    response.raise_for_status()
    data = response.json()

    if total_results is None:
        total_results = data.get("count", 0)
        total_pages = math.ceil(total_results / PAGE_SIZE) if total_results else 0

    blob_name = (
        f"{GCS_RAW_PREFIX}/year={YEAR}/"
        f"rawg_games_{START_DATE_CLEAN}_{END_DATE_CLEAN}_{page:03}.json"
    )
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

    total_games += len(data.get("results", []))
    pages_processed += 1

    print(f"Status code: {response.status_code}")
    print(f"Count field: {data.get('count')}")
    print(f"Total pages (calculated): {total_pages}")
    print(f"Results returned: {len(data.get('results', []))}")
    print(f"Uploaded to: gs://{GCS_BUCKET}/{blob_name}")

    requests_count += 1
    url = data.get("next")
    params = None
    page += 1

print(f"Total API calls: {requests_count}")
print(f"Total games processed: {total_games}")
print(f"Pages processed: {pages_processed}")