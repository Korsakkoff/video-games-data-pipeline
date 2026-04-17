import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RAWG_API_KEY")
BASE_URL = "https://api.rawg.io/api/stores"
PAGE_SIZE = 40


def fetch_stores_df(api_key: str) -> pd.DataFrame:
    if not api_key:
        raise ValueError("RAWG_API_KEY not found")

    all_stores = []
    url = BASE_URL
    params = {
        "key": api_key,
        "page_size": PAGE_SIZE,
    }

    while url:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        all_stores.extend(data.get("results", []))

        url = data.get("next")
        params = None

    df = pd.DataFrame(all_stores)

    df = df[[
        "id",
        "name",
        "slug",
        "games_count",
        "domain",
        "image_background"
    ]].rename(columns={
        "id": "store_id",
        "name": "store_name",
        "slug": "store_slug"
    })

    return df


if __name__ == "__main__":
    df = fetch_stores_df(API_KEY)
    print(df.head())
    print(f"Total stores: {len(df)}")