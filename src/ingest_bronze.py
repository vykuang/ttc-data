
import boto3
from google.transit import gtfs_realtime_pb2 as gtfs
import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone

from zoneinfo import ZoneInfo

# iterate through all pb


def convert_unix_to_datetime_est(unix_time: int) -> datetime:
    ds = (
        datetime
        .fromtimestamp(unix_time, tz=timezone.utc)
        .astimezone(ZoneInfo("America/Toronto"))
        # .isoformat()
    )
    # return ds.strftime(format="%H:%M:%S")
    return ds

def parse_trip_feed(pb_path: Path) -> pl.DataFrame:
    feed = gtfs.FeedMessage()
    with open(pb_path, 'rb') as pb:
        feed.ParseFromString(pb.read())
    stops = []
    for entity in feed.entity:
        tu = entity.trip_update
        for stu in tu.stop_time_update:
            stops.append({
                'trip_id': tu.trip.trip_id,
                'route_id': tu.trip.route_id,
                'vehicle_id': tu.vehicle.id,
                'stop_sequence': stu.stop_sequence,
                'stop_id': stu.stop_id,
                'actual_arrival_time': convert_unix_to_datetime_est(stu.arrival.time) if stu.HasField('arrival') else None,
                'actual_departure_time': convert_unix_to_datetime_est(stu.departure.time) if stu.HasField('departure') else None,
                'feed_timestamp': convert_unix_to_datetime_est(tu.timestamp),
            })
    return pl.DataFrame(stops)

def enrich_with_schedule(facts, stop_times) -> pl.DataFrame:
    cols = ['trip_id', 'arrival_time', 'departure_time', 'stop_sequence', 'stop_headsign']
    stop_times_sel = stop_times.select(cols)
    return facts.join(
        stop_times_sel,
        on=['trip_id', 'stop_sequence'],
        how='left',
        suffix='_sched'
    )

def upsert_fact_trips(facts: pl.DataFrame) -> None: