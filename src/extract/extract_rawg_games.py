import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Parameters for API request
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
PAGE_SIZE = 40
MAX_PAGES = 10
ORDERING = "-added"

# Cleaned date strings for file naming
START_DATE_CLEAN = START_DATE.replace("-", "")
END_DATE_CLEAN = END_DATE.replace("-", "")

load_dotenv()

API_KEY = os.getenv("RAWG_API_KEY")

if not API_KEY:
    raise ValueError("RAWG_API_KEY not found in .env")

url = "https://api.rawg.io/api/games"

# Info for validation
total_games = 0
pages_processed = 0

for page in range(1, MAX_PAGES + 1):
    params = {
        "key": API_KEY,
        "dates": f"{START_DATE},{END_DATE}",
        "ordering": ORDERING,
        "page_size": PAGE_SIZE,
        "page": page,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"rawg_games_test_{START_DATE_CLEAN}_{END_DATE_CLEAN}_{page:03}.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    total_games += len(data.get('results', []))
    pages_processed += 1

    print(f"Status code: {response.status_code}")
    print(f"Count field: {data.get('count')}")
    print(f"Results returned: {len(data.get('results', []))}")
    print(f"Saved to: {output_file}")

print(f"Total games processed: {total_games}")
print(f"Pages processed: {pages_processed}")