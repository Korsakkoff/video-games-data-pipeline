import json
import os

from google.cloud import storage

GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW_PREFIX = os.getenv("GCS_RAW_PREFIX")

if not GCS_BUCKET:
    raise ValueError("GCS_BUCKET not found in environment variables")

if not GCS_RAW_PREFIX:
    raise ValueError("GCS_RAW_PREFIX not found in environment variables")

def read_rawg_games_gcs(year: int | str) -> list[dict]:
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    prefix = f"{GCS_RAW_PREFIX}/year={year}/"

    blobs = sorted(bucket.list_blobs(prefix=prefix), key=lambda blob: blob.name)

    all_games = []

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        content = blob.download_as_text()
        payload = json.loads(content)

        results = payload.get("results", [])

        if not isinstance(results, list):
            raise ValueError(f"'results' is not a list in blob {blob.name}")
        
        all_games.extend(results)

    return all_games