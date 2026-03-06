"""
DuckDB warehouse layer for Translink data.

Reads Parquet files directly from S3 via the httpfs extension.
All views are created over S3 paths — no data is copied locally.

Usage:
    import warehouse
    con = warehouse.connect()
    df = con.execute("SELECT * FROM stop_arrival_fact LIMIT 10").df()
"""

import os

import boto3
import constants
import duckdb


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with httpfs configured to read from S3,
    and all Translink views registered.
    """
    con = duckdb.connect(database=":memory:", read_only=False)
    _configure_s3(con)
    _create_views(con)
    return con


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    """Load httpfs and configure S3 credentials from the environment.

    Uses boto3 to resolve credentials so that ~/.aws/credentials, env vars,
    and IAM roles are all supported without extra DuckDB configuration.
    """
    con.execute("INSTALL httpfs; LOAD httpfs;")

    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    region = session.region_name or os.environ.get("AWS_DEFAULT_REGION", "ca-central-1")

    con.execute(f"""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE s3,
            KEY_ID '{creds.access_key}',
            SECRET '{creds.secret_key}',
            REGION '{region}'
        );
    """)


def _create_views(con: duckdb.DuckDBPyConnection) -> None:
    """Register S3 Parquet paths as DuckDB views."""
    bucket = constants.AWS_BUCKET

    # Fact table: all transformed dates, unioned via glob
    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW stop_arrival_fact AS
            SELECT * FROM read_parquet('s3://{bucket}/transformed/stop_arrival_fact/*.parquet');
        """)
    except Exception as e:
        import logging
        logging.warning(f"stop_arrival_fact view creation failed: {e}")
        con.execute("""
            CREATE OR REPLACE VIEW stop_arrival_fact AS
            SELECT
                NULL::VARCHAR AS date,
                NULL::VARCHAR AS trip_id,
                NULL::VARCHAR AS vehicle_id,
                NULL::VARCHAR AS route_id,
                NULL::VARCHAR AS route_name,
                NULL::VARCHAR AS transit_mode,
                NULL::VARCHAR AS stop_id,
                NULL::VARCHAR AS stop_name,
                NULL::INTEGER AS stop_sequence,
                NULL::VARCHAR AS scheduled_arrival_time,
                NULL::VARCHAR AS snapshot_time,
                NULL::INTEGER AS arrival_delay_seconds
            WHERE false;
        """)

    # GTFS dimension views — use the latest available date via glob + ORDER BY
    for name in constants.GTFS_FILES:
        try:
            con.execute(f"""
                CREATE OR REPLACE VIEW gtfs_{name} AS
                SELECT * FROM read_parquet(
                    (
                        SELECT last(path ORDER BY path)
                        FROM glob('s3://{bucket}/static/gtfs/*/{name}.parquet')
                    )
                );
            """)
        except Exception:
            pass

    # Convenience view: raw trip snapshots for today or all time
    try:
        con.execute(f"""
            CREATE OR REPLACE VIEW raw_buses AS
            SELECT * FROM read_parquet('s3://{bucket}/raw/trip/*/*.parquet');
        """)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Named queries used by the Streamlit dashboard
# ---------------------------------------------------------------------------

def query_realtime_delays(con: duckdb.DuckDBPyConnection, date: str) -> duckdb.DuckDBPyRelation:
    """
    Current delay by route for a given date (YYYYMMDD).
    Returns: route_id, route_name, transit_mode, avg_delay_seconds, snapshot_count
    """
    return con.execute(f"""
        SELECT
            route_id,
            route_name,
            transit_mode,
            ROUND(AVG(arrival_delay_seconds), 0)::INTEGER AS avg_delay_seconds,
            COUNT(*) AS snapshot_count
        FROM stop_arrival_fact
        WHERE date = '{date}'
        GROUP BY route_id, route_name, transit_mode
        ORDER BY avg_delay_seconds DESC;
    """)


def query_historical_delays(
    con: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
    transit_mode: str | None = None,
    route_id: str | None = None,
    stop_id: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """
    Average delay per route per day between start_date and end_date (YYYYMMDD).
    Optionally filter by transit_mode, route_id, or stop_id.
    """
    filters = [
        f"date >= '{start_date}'",
        f"date <= '{end_date}'",
    ]
    if transit_mode:
        filters.append(f"transit_mode = '{transit_mode}'")
    if route_id:
        filters.append(f"route_id = '{route_id}'")
    if stop_id:
        filters.append(f"stop_id = '{stop_id}'")

    where = " AND ".join(filters)
    return con.execute(f"""
        SELECT
            date,
            route_id,
            route_name,
            transit_mode,
            stop_id,
            stop_name,
            ROUND(AVG(arrival_delay_seconds), 0)::INTEGER AS avg_delay_seconds,
            COUNT(*) AS observation_count
        FROM stop_arrival_fact
        WHERE {where}
        GROUP BY date, route_id, route_name, transit_mode, stop_id, stop_name
        ORDER BY date, route_id, stop_id;
    """)


def query_routes(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """All distinct routes with their transit mode, for dashboard filter dropdowns."""
    return con.execute("""
        SELECT DISTINCT route_id, route_name, transit_mode
        FROM stop_arrival_fact
        ORDER BY transit_mode, route_id;
    """)


def query_stops(con: duckdb.DuckDBPyConnection, route_id: str) -> duckdb.DuckDBPyRelation:
    """All stops for a given route_id, for dashboard filter dropdowns."""
    return con.execute(f"""
        SELECT DISTINCT stop_id, stop_name
        FROM stop_arrival_fact
        WHERE route_id = '{route_id}'
        ORDER BY stop_name;
    """)
