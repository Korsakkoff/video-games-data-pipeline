from pathlib import Path

import pandas as pd

from src.extract.read_rawg_games import read_raw_games

from src.transform.games import build_games_df
from src.transform.genres import build_genres_df
from src.transform.platforms import build_platforms_df
from src.transform.game_genre import build_game_genre_df
from src.transform.game_platform import build_game_platform_df

from src.load.load_to_bigquery import load_parquet_to_bigquery
from src.load.load_pipeline import load_all_tables

from src.utils.paths import get_project_root
from src.utils.gcp import setup_gcp_credentials, get_gcp_config


def validate_not_empty(df: pd.DataFrame, name: str) -> None:
    if df.empty:
        raise ValueError(f"{name} DataFrame is empty")


def validate_unique(df: pd.DataFrame, columns: list[str], name: str) -> None:
    duplicated_rows = df.duplicated(subset=columns).sum()
    if duplicated_rows > 0:
        raise ValueError(
            f"{name} has {duplicated_rows} duplicated rows for key {columns}"
        )


def save_parquet(df: pd.DataFrame, output_file: Path, name: str) -> None:
    df.to_parquet(output_file, index=False)

    if not output_file.exists():
        raise FileNotFoundError(f"{name} parquet file was not created: {output_file}")

    reloaded_df = pd.read_parquet(output_file)
    if len(reloaded_df) != len(df):
        raise ValueError(
            f"{name} row count mismatch after saving parquet. "
            f"Original: {len(df)}, Reloaded: {len(reloaded_df)}"
        )

    print(f"Saved {name}: {len(df)} rows -> {output_file}")


def main() -> None:
    project_root = get_project_root()

    raw_dir = project_root / "data" / "raw"
    output_dir = project_root / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    print(f"Project root: {project_root}")
    print(f"Raw directory: {raw_dir}")
    print(f"Output directory: {output_dir}")

    #extract
    raw_games = read_raw_games(raw_dir)

    if not raw_games:
        raise ValueError("No raw games were loaded")

    print(f"Raw games loaded: {len(raw_games)}")

    #transform
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

    #save
    save_parquet(games_df, output_dir / "games.parquet", "games")
    save_parquet(genres_df, output_dir / "genres.parquet", "genres")
    save_parquet(platforms_df, output_dir / "platforms.parquet", "platforms")
    save_parquet(game_genre_df, output_dir / "game_genre.parquet", "game_genre")
    save_parquet(game_platform_df, output_dir / "game_platform.parquet", "game_platform")

    print("Pipeline finished successfully.")
    print(
        "Summary: "
        f"games={len(games_df)}, "
        f"genres={len(genres_df)}, "
        f"platforms={len(platforms_df)}, "
        f"game_genre={len(game_genre_df)}, "
        f"game_platform={len(game_platform_df)}"
    )

    project_id, dataset = get_gcp_config()
    load_all_tables(project_id, dataset)


if __name__ == "__main__":
    setup_gcp_credentials()
    main()