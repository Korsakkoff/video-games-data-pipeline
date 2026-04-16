import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

YEAR = int(os.getenv("YEAR", "0"))
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_PROCESSED_PREFIX = os.getenv("GCS_PROCESSED_PREFIX")
BQ_DATASET = os.getenv("BQ_DATASET_RAW_PREFIX")
BQ_LOCATION = os.getenv("BQ_LOCATION") 

if not YEAR:
    raise ValueError("YEAR not found in environment variables")

if not GCP_PROJECT:
    raise ValueError("GCP_PROJECT not found in environment variables")

if not GCS_BUCKET:
    raise ValueError("GCS_BUCKET not found in environment variables")

if not BQ_DATASET:
    raise ValueError("BQ_DATASET not found in environment variables")

if not BQ_LOCATION:
    raise ValueError("BQ_LOCATION not found in environment variables")

if not GCS_PROCESSED_PREFIX:
    raise ValueError("GCS_PROCESSED_PREFIX not found in environment variables")

YEAR_INT = int(YEAR)

client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)

DATASET_REF = f"{GCP_PROJECT}.{BQ_DATASET}"
GCS_BASE_PATH = f"gs://{GCS_BUCKET}/{GCS_PROCESSED_PREFIX}/year={YEAR}"


def table_id(table_name: str) -> str:
    return f"{DATASET_REF}.{table_name}"


def table_exists(table_name: str) -> bool:
    try:
        client.get_table(table_id(table_name))
        return True
    except NotFound:
        return False
    
    
def run_query(sql: str, query_parameters: list | None = None) -> None:
    job_config = None

    if query_parameters:
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

    job = client.query(sql, job_config=job_config)
    job.result()


def load_parquet_from_gcs(
    source_uri: str,
    destination_table: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )

    job = client.load_table_from_uri(
        source_uris=source_uri,
        destination=table_id(destination_table),
        job_config=job_config,
        location=BQ_LOCATION,
    )
    job.result()

    print(f"Loaded {source_uri} -> {table_id(destination_table)} ({write_disposition})")


def delete_year_from_table(table_name: str, year: int) -> None:
    if not table_exists(table_name):
        print(f"Table {table_id(table_name)} does not exist yet, skipping delete")
        return

    sql = f"""
        DELETE FROM `{table_id(table_name)}`
        WHERE year = @year
    """

    query_parameters = [
        bigquery.ScalarQueryParameter("year", "INT64", year),
    ]

    run_query(sql, query_parameters)
    print(f"Deleted existing data for year={year} from {table_id(table_name)}")


def upsert_dimension(
    source_uri: str,
    target_table: str,
    temp_table: str,
    key_column: str,
    value_columns: list[str],
) -> None:
    # Cargar parquet del año a tabla temporal
    load_parquet_from_gcs(
        source_uri=source_uri,
        destination_table=temp_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    if not table_exists(target_table):
        select_columns = ",\n                ".join([key_column] + value_columns)

        sql = f"""
            CREATE TABLE `{table_id(target_table)}` AS
            SELECT DISTINCT
                {select_columns}
            FROM `{table_id(temp_table)}`
        """
        run_query(sql)
        print(f"Created dimension table {table_id(target_table)}")
    else:
        update_clause = ", ".join([f"T.{col} = S.{col}" for col in value_columns])
        insert_columns = ", ".join([key_column] + value_columns)
        insert_values = ", ".join([f"S.{key_column}"] + [f"S.{col}" for col in value_columns])

        select_value_exprs = ",\n                    ".join(
            [f"ANY_VALUE({col}) AS {col}" for col in value_columns]
        )

        sql = f"""
            MERGE `{table_id(target_table)}` AS T
            USING (
                SELECT
                    {key_column},
                    {select_value_exprs}
                FROM `{table_id(temp_table)}`
                GROUP BY {key_column}
            ) AS S
            ON T.{key_column} = S.{key_column}
            WHEN MATCHED THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_columns})
              VALUES ({insert_values})
        """
        run_query(sql)
        print(f"Merged dimension table {table_id(target_table)}")

    client.delete_table(table_id(temp_table), not_found_ok=True)
    print(f"Dropped temp table {table_id(temp_table)}")


def main() -> None:
    games_uri = f"{GCS_BASE_PATH}/games.parquet"
    genres_uri = f"{GCS_BASE_PATH}/genres.parquet"
    platforms_uri = f"{GCS_BASE_PATH}/platforms.parquet"
    game_genre_uri = f"{GCS_BASE_PATH}/game_genre.parquet"
    game_platform_uri = f"{GCS_BASE_PATH}/game_platform.parquet"

    # Facts / bridge tables: delete year and reload
    delete_year_from_table("games", YEAR_INT)
    delete_year_from_table("game_genre", YEAR_INT)
    delete_year_from_table("game_platform", YEAR_INT)

    load_parquet_from_gcs(games_uri, "games", bigquery.WriteDisposition.WRITE_APPEND)
    load_parquet_from_gcs(game_genre_uri, "game_genre", bigquery.WriteDisposition.WRITE_APPEND)
    load_parquet_from_gcs(game_platform_uri, "game_platform", bigquery.WriteDisposition.WRITE_APPEND)

    # Dimensions: merge deduplicated
    upsert_dimension(
        source_uri=genres_uri,
        target_table="genres",
        temp_table=f"_tmp_genres_{YEAR}",
        key_column="genre_id",
        value_columns=["name"],
    )

    upsert_dimension(
        source_uri=platforms_uri,
        target_table="platforms",
        temp_table=f"_tmp_platforms_{YEAR}",
        key_column="platform_id",
        value_columns=["name"],
    )

    print(f"BigQuery load completed for year={YEAR}")


if __name__ == "__main__":
    main()   