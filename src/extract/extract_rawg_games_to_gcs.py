import json
import math
import os
import time
from datetime import datetime, timedelta

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


def parse_date(date_str: str) -> datetime.date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def format_date(date_obj) -> str:
    return date_obj.strftime("%Y-%m-%d")


def clean_date(date_str: str) -> str:
    return date_str.replace("-", "")


def split_date_range(start_date: str, end_date: str) -> tuple[tuple[str, str], tuple[str, str]]:
    start = parse_date(start_date)
    end = parse_date(end_date)

    if start >= end:
        raise ValueError(f"Cannot split invalid range: {start_date} -> {end_date}")

    delta_days = (end - start).days
    mid = start + timedelta(days=delta_days // 2)

    left = (format_date(start), format_date(mid))
    right = (format_date(mid + timedelta(days=1)), format_date(end))

    return left, right


def get_with_retry(
    url: str,
    params: dict | None,
    timeout: int = 30,
    max_retries: int = 5,
) -> requests.Response:
    """
    Hace 1 intento inicial + hasta max_retries reintentos adicionales.
    Total máximo de intentos = 1 + max_retries.
    Reintenta solo en errores transitorios.
    """
    retryable_statuses = {502, 503, 504}
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)

            if response.status_code in retryable_statuses:
                if attempt == max_retries:
                    response.raise_for_status()

                wait_seconds = 2 * (attempt + 1)
                print(
                    f"Transient HTTP {response.status_code} for {response.url}. "
                    f"Retry {attempt + 1}/{max_retries} in {wait_seconds}s..."
                )
                time.sleep(wait_seconds)
                continue

            return response

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exception = exc

            if attempt == max_retries:
                raise

            wait_seconds = 2 * (attempt + 1)
            print(
                f"Network error on attempt {attempt + 1}/{max_retries}: {exc}. "
                f"Retrying in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)

        except requests.exceptions.RequestException as exc:
            # Otros errores de requests no se consideran transitorios por defecto
            raise exc

    if last_exception:
        raise last_exception

    raise RuntimeError("Request failed after retries")


def get_range_count(
    api_key: str,
    start_date: str,
    end_date: str,
    ordering: str,
    page_size: int,
    platforms: str,
    stores: str,
) -> int:
    url = "https://api.rawg.io/api/games"
    params = {
        "key": api_key,
        "dates": f"{start_date},{end_date}",
        "ordering": ordering,
        "page_size": 1,
        "page": 1,
        "platforms": platforms,
        "stores": stores,
    }

    response = get_with_retry(url=url, params=params, timeout=30, max_retries=5)
    response.raise_for_status()
    data = response.json()
    return data.get("count", 0)


def build_safe_ranges(
    api_key: str,
    start_date: str,
    end_date: str,
    ordering: str,
    page_size: int,
    max_records_per_range: int,
    platforms: str,
    stores: str,
) -> list[tuple[str, str]]:
    count = get_range_count(
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
        ordering=ordering,
        page_size=page_size,
        platforms=platforms,
        stores=stores,
    )

    print(f"Range check -> {start_date} to {end_date}: count={count}")

    if count == 0:
        return []

    if count <= max_records_per_range:
        return [(start_date, end_date)]

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start == end:
        print(
            f"WARNING: Single-day range {start_date} still has count={count}, "
            f"which exceeds max_records_per_range={max_records_per_range}."
        )
        return [(start_date, end_date)]

    left_range, right_range = split_date_range(start_date, end_date)

    return (
        build_safe_ranges(
            api_key=api_key,
            start_date=left_range[0],
            end_date=left_range[1],
            ordering=ordering,
            page_size=page_size,
            max_records_per_range=max_records_per_range,
            platforms=platforms,
            stores=stores,
        )
        + build_safe_ranges(
            api_key=api_key,
            start_date=right_range[0],
            end_date=right_range[1],
            ordering=ordering,
            page_size=page_size,
            max_records_per_range=max_records_per_range,
            platforms=platforms,
            stores=stores,
        )
    )


# Environment variables and configuration
API_KEY = os.getenv("RAWG_API_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW_PREFIX = os.getenv("GCS_RAW_PREFIX")

# API parameters
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
PAGE_SIZE = 40
MAX_PAGES = parse_optional_int(os.getenv("MAX_PAGES"))
ORDERING = "-released"

# Most coherent subset of the catalog
PLATFORMS = "4,7,187,186"  # PC, Nintendo Switch, PlayStation 5, Xbox Series S/X
STORES = "1,6,3,2,11"      # Steam, Nintendo Store, PlayStation Store, Xbox Store, Epic Games

# Practical safe limit per range
MAX_RECORDS_PER_RANGE = 9000


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

YEAR = START_DATE[:4]

storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)

safe_ranges = build_safe_ranges(
    api_key=API_KEY,
    start_date=START_DATE,
    end_date=END_DATE,
    ordering=ORDERING,
    page_size=PAGE_SIZE,
    max_records_per_range=MAX_RECORDS_PER_RANGE,
    platforms=PLATFORMS,
    stores=STORES,
)

print("Safe ranges to extract:")
for range_start, range_end in safe_ranges:
    print(f" - {range_start} -> {range_end}")

total_games = 0
pages_processed = 0
requests_count = 0
ranges_processed = 0

for range_start, range_end in safe_ranges:
    print(f"Starting extraction for range: {range_start} -> {range_end}")

    range_start_clean = clean_date(range_start)
    range_end_clean = clean_date(range_end)

    url = "https://api.rawg.io/api/games"
    params = {
        "key": API_KEY,
        "dates": f"{range_start},{range_end}",
        "ordering": ORDERING,
        "page_size": PAGE_SIZE,
        "platforms": PLATFORMS,
        "stores": STORES,
    }

    page = 1
    range_total_results = None
    range_total_pages = None

    while url:
        if MAX_PAGES is not None and pages_processed >= MAX_PAGES:
            print("MAX_PAGES limit reached. Stopping extraction.")
            break

        response = get_with_retry(url=url, params=params, timeout=30, max_retries=5)
        requests_count += 1

        if response.status_code == 404:
            print(
                f"Reached last available page for range {range_start} -> {range_end} "
                f"at page={page}. RAWG returned 404."
            )
            break

        response.raise_for_status()
        data = response.json()

        if range_total_results is None:
            range_total_results = data.get("count", 0)
            range_total_pages = math.ceil(range_total_results / PAGE_SIZE) if range_total_results else 0

        blob_name = (
            f"{GCS_RAW_PREFIX}/year={YEAR}/"
            f"rawg_games_{range_start_clean}_{range_end_clean}_{page:03}.json"
        )
        blob = bucket.blob(blob_name)

        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json",
        )

        results_count = len(data.get("results", []))
        total_games += results_count
        pages_processed += 1

        print(f"Status code: {response.status_code}")
        print(f"Range: {range_start} -> {range_end}")
        print(f"Count field: {data.get('count')}")
        print(f"Total pages (calculated for range): {range_total_pages}")
        print(f"Results returned: {results_count}")
        print(f"Uploaded to: gs://{GCS_BUCKET}/{blob_name}")

        url = data.get("next")
        params = None
        page += 1

    ranges_processed += 1

    if MAX_PAGES is not None and pages_processed >= MAX_PAGES:
        break

print(f"Total API calls: {requests_count}")
print(f"Total games processed: {total_games}")
print(f"Pages processed: {pages_processed}")
print(f"Ranges processed: {ranges_processed}")
print(f"Safe ranges generated: {len(safe_ranges)}")