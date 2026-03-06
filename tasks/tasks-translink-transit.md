## Relevant Files

- `translink/constants.py` - Translink API base URL, endpoint paths, API key from env, S3 bucket name.
- `translink/get_api.py` - Fetches real-time data from Translink REST API, converts JSON to Parquet, writes to S3.
- `translink/get_gtfs_static.py` - Downloads Translink GTFS zip, extracts CSVs, converts to Parquet, writes to S3 under static/gtfs/.
- `translink/transform.py` - Reads raw bus snapshots + GTFS dims from S3, joins into stop_arrival_fact, writes to transformed/.
- `translink/warehouse.py` - DuckDB connection with httpfs/S3 config, view definitions, and named query functions for the dashboard.
- `Dockerfile.translink` - Multi-stage uv build for the translink pipeline image; ENTRYPOINT is python, CMD selects the script.
- `dags/translink_orchestrator.py` - Two Airflow DAGs: 5-min realtime ingest+transform, and weekly GTFS static ingest.
- `translink/dashboard.py` - Streamlit app with Real-time and Historical tabs querying DuckDB warehouse.
- `tf/main.tf` - Added `translink-api` S3 bucket resource alongside existing `ttc-api` bucket.

### Notes

- The current branch `translink-data` already exists — task 0.0 is to confirm and use it.
- Mirror the TTC project patterns (`src/constants.py`, `src/get_api.py`, `dags/orchestrator.py`, `Dockerfile`, `tf/main.tf`) where applicable.
- The Translink Open API key must be stored in `.env` and injected at runtime — never hardcoded.
- Use `DockerOperator` only for all Airflow tasks.

## Instructions for Completing Tasks

**IMPORTANT:** As you complete each task, you must check it off in this markdown file by changing `- [ ]` to `- [x]`. This helps track progress and ensures you don't skip any steps.

Example:
- `- [ ] 1.1 Read file` → `- [x] 1.1 Read file` (after completing)

Update the file after completing each sub-task, not just after completing an entire parent task.

## Tasks

- [x] 0.0 Confirm feature branch
- [x] 1.0 Configure Translink API credentials and constants
- [x] 2.0 Build real-time ingestion layer (Translink API → S3)
- [x] 3.0 Build static GTFS ingestion layer (GTFS zip → S3 dimension tables)
- [x] 4.0 Build transformation layer (raw S3 → `stop_arrival_fact` via Polars)
- [x] 5.0 Build DuckDB warehouse layer (query S3 Parquet via `httpfs`)
- [x] 6.0 Wire up Airflow DAGs with DockerOperator
- [x] 7.0 Build Streamlit dashboard (real-time + historical views)
- [x] 8.0 Provision infrastructure (Terraform S3 bucket for Translink data)
