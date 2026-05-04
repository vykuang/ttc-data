# Stop-Level Facts Table: Transformation Schema

## Objective

Transform raw TTC GTFS-RT API data (Trips + Vehicles) into a `stop_arrival_fact` table at grain `(trip_id, stop_sequence)`.

## Source Schemas

### Trip API (`raw/trip/{date}/{time}.pb`)

Each entity contains one trip with multiple `stop_time_update` entries:

```
entity.id
trip_update.trip.trip_id
trip_update.trip.route_id
trip_update.vehicle.id
trip_update.timestamp
trip_update.stop_time_update[]:
  - stop_sequence (int)
  - stop_id (str)
  - arrival.time (unix seconds)
  - departure.time (unix seconds)
  - schedule_relationship (enum)
```

### Vehicle API (`raw/vehicle/{date}/{time}.pb`)

Each entity contains one vehicle's current state:

```
entity.id
vehicle.trip.trip_id
vehicle.trip.route_id
vehicle.position.{latitude, longitude, bearing, speed}
vehicle.current_stop_sequence
vehicle.current_status
vehicle.stop_id
vehicle.vehicle.id
vehicle.occupancy_status
vehicle.timestamp
```

### Dimension Tables

| Table | Key Fields |
|-------|------------|
| `dims/stop_times.parquet` | `trip_id`, `stop_sequence`, `arrival_time`, `departure_time` |
| `dims/stops.parquet` | `stop_id`, `stop_name`, `stop_lat`, `stop_lon` |

## Transformation Pipeline

### Step 1: Parse protobuf files

```python
from google.transit import gtfs_realtime_pb2
import polars as pl

def parse_trip_feed(path: str) -> pl.DataFrame:
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(path, 'rb') as f:
        feed.ParseFromString(f.read())
    
    rows = []
    for entity in feed.entity:
        tu = entity.trip_update
        for stu in tu.stop_time_update:
            rows.append({
                'trip_id': tu.trip.trip_id,
                'route_id': tu.trip.route_id,
                'vehicle_id': tu.vehicle.id,
                'stop_sequence': stu.stop_sequence,
                'stop_id': stu.stop_id,
                'actual_arrival_time': stu.arrival.time if stu.HasField('arrival') else None,
                'actual_departure_time': stu.departure.time if stu.HasField('departure') else None,
                'feed_timestamp': tu.timestamp,
            })
    return pl.DataFrame(rows)
```

### Step 2: Join with scheduled stop times

```python
def enrich_with_schedule(facts: pl.DataFrame, stop_times: pl.DataFrame) -> pl.DataFrame:
    return facts.join(
        stop_times.select(['trip_id', 'stop_sequence', 'arrival_time', 'departure_time']),
        on=['trip_id', 'stop_sequence'],
        how='left',
        suffix='_sched'
    )
```

### Step 3: Compute derived metrics

```python
def compute_metrics(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        (pl.col('actual_arrival_time') - pl.col('arrival_time_sched')).alias('arrival_delay_seconds'),
        (pl.col('actual_departure_time') - pl.col('actual_arrival_time')).alias('dwell_time_seconds'),
    ])
```

### Step 4: Enrich with stop dimension

```python
def enrich_with_stops(df: pl.DataFrame, stops: pl.DataFrame) -> pl.DataFrame:
    return df.join(
        stops.select(['stop_id', 'stop_name', 'stop_lat', 'stop_lon']),
        on='stop_id',
        how='left',
    )
```

## Target Schema: `stop_arrival_fact`

| Column | Type | Source |
|--------|------|--------|
| `trip_id` | str | Trip API |
| `vehicle_id` | str | Trip API |
| `route_id` | str | Trip API |
| `stop_id` | str | Trip API |
| `stop_sequence` | int | Trip API |
| `scheduled_arrival_time` | timestamp | dims/stop_times |
| `actual_arrival_time` | timestamp | Trip API |
| `arrival_delay_seconds` | int | derived |
| `departure_time` | timestamp | Trip API |
| `dwell_time_seconds` | int | derived |
| `stop_name` | str | dims/stops |
| `stop_lat` | float | dims/stops |
| `stop_lon` | float | dims/stops |
| `ingestion_date` | str | filename |
| `ingestion_time` | str | filename |

## Notes

- **Grain**: One row per `(trip_id, stop_sequence)` pair
- **Time resolution**: Unix seconds from API, scheduled times are HH:MM:SS strings (requires date context from feed timestamp)
- **Join strategy**: Left join on schedule dimensions; not all trips may have matching schedule data
- **Partition**: By `ingestion_date` for efficient historical queries
