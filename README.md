# Video Games Data Pipeline (RAWG API)

End-to-end data pipeline that extracts video game data from the RAWG API, processes it in a data lake (GCS), loads it into BigQuery, and models it using dbt.

---

## Architecture
RAWG API
→ Kestra (orchestration)
→ GCS (raw → processed)
→ BigQuery (raw → staging → marts)
→ dbt (transformations)


---

## Tech Stack

- **Orchestration**: Kestra
- **Data Lake**: Google Cloud Storage (GCS)
- **Data Warehouse**: BigQuery
- **Transformations**: dbt
- **Language**: Python
- **Containerization**: Docker

---

## Pipeline Overview

The pipeline is orchestrated in Kestra and consists of four main stages:

### 1. Extract
- Fetches data from the RAWG API
- Stores raw JSON files in GCS:
    gs://<bucket>/raw/year=YYYY/

### 2. Process
- Reads raw JSON from GCS
- Normalizes and flattens nested structures
- Outputs clean parquet files:
    gs://<bucket>/processed/year=YYYY/


### 3. Load
- Loads processed parquet files into BigQuery
- Data is stored in the `rawg_raw` dataset

### 4. Transform (dbt)
- Staging models standardize and clean raw data
- Mart models generate analytical datasets

---

## Data Model

### BigQuery Datasets

- **rawg_raw** → Source tables loaded from GCS
- **rawg_staging** → Cleaned and standardized views (dbt)
- **rawg_marts** → Final analytical tables (dbt)

---

## dbt Models

### Staging
- `stg_games`
- `stg_genres`
- `stg_platforms`
- `stg_game_genre`
- `stg_game_platform`

### Marts
- `games_by_year`
- `games_by_genre`
- `games_avg_rating`
- `genre_platform_distribution`
- `genre_popularity`

---

## Project Structure
├── src/ # Python pipeline (extract, process, load)
├── dbt/ # dbt project
├── kestra/flows/ # Kestra workflows
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── README.md
└── .env.example


---

## How to Run

### 1. Set environment variables

Copy and configure:

```bash
cp .env.example .env

Fill in:

RAWG API key
GCP project and credentials
GCS bucket
BigQuery datasets

2. Start Kestra
docker compose up

Access UI:
http://localhost:8080

3. Run the pipeline

Execute the flow:
rawg_pipeline

Input:
select year (e.g. 2020)

Future Improvements
Implement incremental loading in BigQuery
Add partitioning and clustering
Extend dbt tests (data quality and relationships)
Add monitoring and alerting in Kestra
Optimize RAWG API extraction strategy

Notes
Data is processed by year to avoid full dataset reloads
Dimensions (genres, platforms) are handled via deduplication in dbt
Pipeline is designed for clarity and modularity rather than full production optimization

