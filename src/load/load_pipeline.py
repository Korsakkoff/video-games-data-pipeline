from src.load.load_to_bigquery import load_parquet_to_bigquery
from src.utils.paths import get_project_root

PROJECT_ID = "tu_proyecto"
DATASET = "tu_dataset"


def load_all_tables(project_id: str, dataset: str) -> None:
    root = get_project_root()

    tables = [
        ("games.parquet", "games"),
        ("genres.parquet", "genres"),
        ("platforms.parquet", "platforms"),
        ("game_genre.parquet", "game_genre"),
        ("game_platform.parquet", "game_platform"),
    ]

    for parquet_name, table_name in tables:
        parquet_path = root / "data" / "processed" / parquet_name
        table_id = f"{project_id}.{dataset}.{table_name}"

        load_parquet_to_bigquery(parquet_path, table_id)