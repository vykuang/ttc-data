import io
import logging
from datetime import datetime, timezone

import boto3
import constants
import polars as pl
from botocore.exceptions import ClientError
from google.transit import gtfs_realtime_pb2


def read_pb_from_s3(s3: object, key: str) -> gtfs_realtime_pb2.FeedMessage:
    obj = s3.get_object(Bucket=constants.AWS_BUCKET, Key=key)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(obj["Body"].read())
    return feed


def read_parquet_from_s3(s3: object, key: str) -> pl.DataFrame:
    obj = s3.get_object(Bucket=constants.AWS_BUCKET, Key=key)
    return pl.read_parquet(io.BytesIO(obj["Body"].read()))


def latest_gtfs_date(s3: object) -> str:
    resp = s3.list_objects_v2(Bucket=constants.AWS_BUCKET, Prefix="static/gtfs/", Delimiter="/")
    dates = [p["Prefix"].split("/")[2] for p in resp.get("CommonPrefixes", [])]
    if not dates:
        raise RuntimeError("No GTFS static data found in S3. Run get_gtfs_static first.")
    return sorted(dates)[-1]


def list_raw_keys(s3: object, date: str, item: str) -> list[str]:
    prefix = f"raw/{item}/{date}/"
    resp = s3.list_objects_v2(Bucket=constants.AWS_BUCKET, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", [])]


def decode_trip_updates(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Extract stop-level arrival delay rows from a GTFS-RT trip update feed."""
    rows = []
    feed_ts = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc).strftime("%H:%M:%S")
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        route_id = tu.trip.route_id
        vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else ""
        for stu in tu.stop_time_update:
            rows.append({
                "trip_id": trip_id,
                "route_id": route_id,
                "vehicle_id": vehicle_id,
                "stop_id": stu.stop_id,
                "stop_sequence": stu.stop_sequence,
                "arrival_delay_seconds": stu.arrival.delay if stu.HasField("arrival") else None,
                "snapshot_time": feed_ts,
            })
    return rows


def build_stop_arrival_fact(date: str | None = None) -> bool:
    """
    Decode raw GTFS-RT trip update protobuf files from S3, join with GTFS static
    dimension tables, and write stop_arrival_fact to S3.

    Grain: one row per stop per trip per feed snapshot.
    """
    s3 = boto3.client("s3")
    date = date or datetime.now().strftime("%Y%m%d")

    # --- decode all trip update .pb files for the day ---
    trip_keys = list_raw_keys(s3, date, "trip")
    if not trip_keys:
        logging.warning(f"No raw trip update files found for {date}")
        return False

    all_rows: list[dict] = []
    for key in trip_keys:
        feed = read_pb_from_s3(s3, key)
        all_rows.extend(decode_trip_updates(feed))

    if not all_rows:
        logging.warning("No trip update entities found in raw files")
        return False

    snapshots = pl.DataFrame(all_rows).with_columns([
        pl.col("trip_id").cast(pl.Utf8),
        pl.col("route_id").cast(pl.Utf8),
        pl.col("stop_id").cast(pl.Utf8),
    ])
    logging.info(f"decoded {len(snapshots)} stop-level rows from {len(trip_keys)} feed snapshots")

    # --- load GTFS static dimensions ---
    gtfs_date = latest_gtfs_date(s3)
    logging.info(f"using GTFS static from {gtfs_date}")

    stop_times = read_parquet_from_s3(s3, f"static/gtfs/{gtfs_date}/stop_times.parquet").with_columns([
        pl.col("trip_id").cast(pl.Utf8),
        pl.col("stop_id").cast(pl.Utf8),
    ]).select(["trip_id", "stop_id", "arrival_time"])

    stops = read_parquet_from_s3(s3, f"static/gtfs/{gtfs_date}/stops.parquet").with_columns([
        pl.col("stop_id").cast(pl.Utf8),
    ]).select(["stop_id", "stop_name"])

    routes = read_parquet_from_s3(s3, f"static/gtfs/{gtfs_date}/routes.parquet").with_columns([
        pl.col("route_id").cast(pl.Utf8),
    ])

    route_type_map = {0: "SkyTrain", 1: "SkyTrain", 2: "West Coast Express", 3: "Bus", 4: "SeaBus"}
    routes = routes.with_columns(
        pl.col("route_type").replace_strict(route_type_map, default="Unknown").alias("transit_mode")
    ).select(["route_id", "route_long_name", "transit_mode"])

    # --- join and build fact table ---
    fact = (
        snapshots
        .join(stop_times, on=["trip_id", "stop_id"], how="left")
        .join(stops, on="stop_id", how="left")
        .join(routes, on="route_id", how="left")
        .rename({"route_long_name": "route_name", "arrival_time": "scheduled_arrival_time"})
        .with_columns(pl.lit(date).alias("date"))
        .select([
            "date",
            "trip_id",
            "vehicle_id",
            "route_id",
            "route_name",
            "transit_mode",
            "stop_id",
            "stop_name",
            "stop_sequence",
            "scheduled_arrival_time",
            "snapshot_time",
            "arrival_delay_seconds",
        ])
        .unique(subset=["trip_id", "stop_id", "snapshot_time"])
    )

    logging.info(f"built stop_arrival_fact with {len(fact)} rows")

    # --- write to S3 ---
    buf = io.BytesIO()
    fact.write_parquet(buf)
    key = f"transformed/stop_arrival_fact/{date}.parquet"
    try:
        s3.put_object(
            Bucket=constants.AWS_BUCKET,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        logging.info(f"wrote s3://{constants.AWS_BUCKET}/{key}")
    except ClientError as e:
        logging.error(e)
        return False

    return True


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="Date to transform (YYYYMMDD). Defaults to today.")
    args = parser.parse_args()
    build_stop_arrival_fact(date=args.date)
