import pandas as pd
import os

from src.extract.read_rawg_games_gcs import read_rawg_games_gcs

from src.transform.games import build_games_df
from src.transform.genres import build_genres_df
from src.transform.platforms import build_platforms_df
from src.transform.game_genre import build_game_genre_df
from src.transform.game_platform import build_game_platform_df


YEAR = os.getenv("YEAR")
GCS_BUCKET = os.getenv("GCS_BUCKET")


def validate_not_empty(df: pd.DataFrame, name: str) -> None:
    if df.empty:
        raise ValueError(f"{name} DataFrame is empty")


def validate_unique(df: pd.DataFrame, columns: list[str], name: str) -> None:
    duplicated_rows = df.duplicated(subset=columns).sum()
    if duplicated_rows > 0:
        raise ValueError(
            f"{name} has {duplicated_rows} duplicated rows for key {columns}"
        )

def main() -> None:
    if not YEAR:
        raise ValueError("YEAR not found in environment variables")
    
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET not found in environment variables")

    raw_games = read_rawg_games_gcs(YEAR)

    if not raw_games:
        raise ValueError("No raw games were loaded")

    print(f"Raw games loaded: {len(raw_games)}")

    games_df = build_games_df(raw_games)
    genres_df = build_genres_df(raw_games)
    platforms_df = build_platforms_df(raw_games)
    game_genre_df = build_game_genre_df(raw_games)
    game_platform_df = build_game_platform_df(raw_games)

    validate_not_empty(games_df, "games")
    validate_not_empty(genres_df, "genres")
    validate_not_empty(platforms_df, "platforms")
    validate_not_empty(game_genre_df, "game_genre")
    validate_not_empty(game_platform_df, "game_platform")

    validate_unique(games_df, ["game_id"], "games")
    validate_unique(genres_df, ["genre_id"], "genres")
    validate_unique(platforms_df, ["platform_id"], "platforms")
    validate_unique(game_genre_df, ["game_id", "genre_id"], "game_genre")
    validate_unique(game_platform_df, ["game_id", "platform_id"], "game_platform")

    base_path = f"gs://{GCS_BUCKET}/processed/year={YEAR}"

    games_df.to_parquet(f"{base_path}/games.parquet", index=False)
    genres_df.to_parquet(f"{base_path}/genres.parquet", index=False)
    platforms_df.to_parquet(f"{base_path}/platforms.parquet", index=False)
    game_genre_df.to_parquet(f"{base_path}/game_genre.parquet", index=False)
    game_platform_df.to_parquet(f"{base_path}/game_platform.parquet", index=False)

if __name__ == "__main__":
    main()