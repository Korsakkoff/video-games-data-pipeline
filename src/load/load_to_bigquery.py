from pathlib import Path
from google.cloud import bigquery

from src.utils.gcp import setup_gcp_credentials


def load_parquet_to_bigquery(parquet_path: Path, table_id: str) -> None:
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",  # sobrescribe tabla
    )

    with open(parquet_path, "rb") as f:
        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config,
        )

    job.result()  # espera a que termine

    print(f"Loaded {parquet_path} → {table_id}")

if __name__ == "__main__":
    setup_gcp_credentials()

    parquet_path = Path("data/processed/games.parquet")
    table_id = "tu_proyecto.tu_dataset.games"

    load_parquet_to_bigquery(parquet_path, table_id)