import marimo

__generated_with = "0.23.9"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data modeling

    Transform raw trip/vehicle data to `stop_arrival_facts` schema

    ## api primer

    ### `/trips/update`

    each entity has
    - id
    - trip_update

    each trip_update:
    ```
    trip
        trip_id: str
        schedule_relationship: ['SCHEDULED', 'ADDED']
        route_id: str
    stop_time_update: list
        stop_sequence: int, [1, 2, ..., n]
        departure
            time: int, unix time in seconds
        stop_id: str
        schedule_relationship: ['SCHEDULED', ...]
    vehicle
        id: str
    timestamp: int, unix time in seconds
    ```
    ### `/vehicles/position`

    each entity:
    - id
    - vehicle

    each vehicle:
    ```
    trip
        trip_id: int
        schedule_relationship: ['SCHEDULED']
        route_id: str
    position
        latitude: int, 6 decimals
        longitude: int
        bearing: int, 0-360?
        speed: str, km/h
    current_stop_sequence: int
    current_status: ['INCOMING_AT]
    timestamp: int
    stop_id: str
    vehicle
        id: str
    occupancy_status: ['FEW_SEATS_AVAILABLE']
    ```
    ### questions

    - possible values for
        - trip_update.trip.schedule_relationship
        - trip_update.stop_time_update[i].schedule_relationship
        - vehicle.trip.schedule_relationship
        - vehicle.current_status
        - vehicle.occupancy_status

    ## plan

    - backbone is `/trips/update`
    - denormalize `trip_id`, `route_id`, `vehicle_id`
    - repeat them for each stop sequence
    - given stop sequences from 1 to n, have row for:
        - start: 1, end: 2, 2-3, ..., n-1 - n
    - `start_stop_id`, `end_stop_id`
    - join from `/vehicle/position`:
        - join on `trip_update.trip.trip_id` = `vehicle.trip.trip_id`
        - and `trip_udpate.vehicle_id` = `vehicle.vehicle.id`
    """)
    return


@app.cell
def _():
    import boto3
    from google.transit import gtfs_realtime_pb2 as gtfs
    import polars as pl
    from pathlib import Path
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime, date


    return Path, ThreadPoolExecutor, as_completed, boto3, datetime, gtfs, pl


@app.cell
def _():
    AWS_BUCKET = 'ttc-api'
    path = 'raw/{item}/{date}/{time}.pb'
    return (AWS_BUCKET,)


@app.cell
def _():
    CURR_DATE = '20260511'
    return (CURR_DATE,)


@app.cell
def _(boto3):
    s3 = boto3.client('s3')
    return (s3,)


@app.cell
def _(AWS_BUCKET, s3):
    try:
        folder = 'raw/trip/20260301'
        objs = s3.list_objects_v2(Bucket=AWS_BUCKET,Prefix=folder)
        print(f'{len(objs['Contents'])} objects listed')
    except Exception as e:
        print(f'error reading {folder}: {e}')
    return (objs,)


@app.cell
def _(objs):
    objs['Contents'][0]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Create set for each of following by iterating through all API results for 2026-5-11:
    - `trip_update.trip.schedule_relationship`
    - `trip_update.stop_time_update[i].schedule_relationship`
    - `vehicle.trip.schedule_relationship`
    - `vehicle.current_status`
    - `vehicle.occupancy_status`
    """)
    return


@app.cell
def _(AWS_BUCKET, Path, s3):
    def download_s3_obj(key: str, dest_root: str = '../data/', overwrite: bool = False):
        # ex key: raw/trip/date/time.pb
        local_path = Path(dest_root) / key
        if overwrite or (not local_path.exists()):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(AWS_BUCKET, key, local_path)
        return key

    return (download_s3_obj,)


@app.cell
def _(AWS_BUCKET, ThreadPoolExecutor, as_completed, download_s3_obj, s3):
    def download_s3_many(prefix, dest_root='../data/', max_workers=16):
        # downloads obj from prefix
        # retrieves list of keys from prefix
        paginator = s3.get_paginator('list_objects_v2')
        # paginator preempts the 1k keys limit, in case our prefix has >1k obj
        pages = paginator.paginate(Bucket=AWS_BUCKET, Prefix=prefix)
        keys = [obj.get('Key') 
            for page in pages 
            for obj in page.get('Contents', [])]
        # creates pool of worker threads to run funcs in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # dict[executor.submit(func, args): obj_key] schedules the funcs to run in worker threads
            # and returns a `Future` obj representing that in-flight task
            futures = {executor.submit(download_s3_obj, k, dest_root): k for k in keys}
            # yields each Future as soon as its thread finishes, in completion order
            # allows processing as soon as each are done
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    # .result() returns the func value, in our case, key
                    fut.result()
                except Exception as e:
                    # reason for using dict, so errors can be traced back to the obj_key
                    print(f"error downloading {key}: {e}")

    return (download_s3_many,)


@app.cell
def _(gtfs):
    feed = gtfs.FeedMessage()
    return (feed,)


@app.cell
def _(CURR_DATE, Path, download_s3_many, feed):

    prefix_template = 'raw/{item}/{date}'
    for item in ['trip', 'vehicle']:
        prefix = prefix_template.format(item=item, date=CURR_DATE)
        download_s3_many(prefix)
        pb_dir = Path('../data') / prefix
        print(f'processing {pb_dir}')
        glob_pbs = list(pb_dir.glob('*.pb'))
        total = len(glob_pbs)
        for i, pb in enumerate(glob_pbs):
            with open(pb, 'rb') as f:
                feed.ParseFromString(f.read())
                msg = f'processed {i}/{total} files'
                print(msg.ljust(60), end='\r', flush=True)
        print(f'{item} processed')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `print` uses the `enum` descriptors to render the names, even though underlying values are `int`s
    """)
    return


@app.cell
def _(gtfs):
    gtfs.TripDescriptor.ScheduleRelationship.Name(0)
    return


@app.cell
def _(gtfs):
    gtfs.TripDescriptor.ScheduleRelationship.Value('SCHEDULED')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    - [schedule relationship](https://github.com/google/transit/blob/87ff45cef97a79188b3c1c335c6f754b3cb5d1e3/gtfs-realtime/proto/gtfs-realtime.proto#L237)
        - `gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship`
        - 0: SCHEDULED
        - 1: ADDED
        - 2: UNSCHEDULED
        - 3: CANCELED
        - 5: REPLACEMENT
    - [vehicle stop status](https://github.com/google/transit/blob/87ff45cef97a79188b3c1c335c6f754b3cb5d1e3/gtfs-realtime/proto/gtfs-realtime.proto#L468)
        - `gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus`
        - 0: INCOMING_AT
        - 1: STOPPED_AT
        - 2: IN_TRANSIT_TO
    - [occupancy status](https://github.com/google/transit/blob/87ff45cef97a79188b3c1c335c6f754b3cb5d1e3/gtfs-realtime/proto/gtfs-realtime.proto#L505)
        - `gtfs_realtime_pb2.VehiclePosition.OccupancyStatus`

    Other enums can be found by ctrl-f `enum`, eg congestion level and alert severity
    """)
    return


@app.cell
def _(CURR_DATE, feed):
    CURR_TIME = '100001'
    with open(f'../data/raw/trip/{CURR_DATE}/{CURR_TIME}.pb', 'rb') as ff:
        feed.ParseFromString(ff.read())
        foo = feed.entity
    return CURR_TIME, foo


@app.cell
def _(datetime):
    from datetime import timezone
    from zoneinfo import ZoneInfo
    def convert_unix_to_datetime_est(unix_time: int) -> str:
        ds = (
            datetime
            .fromtimestamp(unix_time, tz=timezone.utc)
            .astimezone(ZoneInfo("America/Toronto"))
        )
        # return ds.strftime(format="%H:%M:%S")
        return ds.isoformat()

    return ZoneInfo, convert_unix_to_datetime_est, timezone


@app.cell
def _(ZoneInfo, convert_unix_to_datetime_est, datetime, timezone):
    ttss = datetime.fromtimestamp(1123456, tz=timezone.utc)
    print(f'utc: {ttss.strftime(format="%H:%M:%S")}')
    print(f'LA: {ttss.astimezone(tz=ZoneInfo('America/Los_Angeles')).strftime(format="%H:%M:%S")}')
    print(f'TO: {ttss.astimezone(tz=ZoneInfo('America/Toronto')).strftime(format="%H:%M:%S")}')
    print(f'convert: {convert_unix_to_datetime_est(1123456)}')
    return


@app.cell
def _(CURR_TIME, convert_unix_to_datetime_est, foo):
    sample_arrival_times = [convert_unix_to_datetime_est(stop_seq.arrival.time) for stop_seq in foo[0].trip_update.stop_time_update]
    print(f'arrival times for trip update from {CURR_TIME}:\n{sample_arrival_times}')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reading binary pb into polars

    Trips
    1. get trip_update entity
    1. get common
        - `trip.trip_id`
        - `trip.route_id`
        - `vehicle.id`
    1. denormalize for each stop update
    """)
    return


@app.cell
def _(foo):
    sample_trip_update = foo[100].trip_update
    sample_trip_id = sample_trip_update.trip.trip_id
    sample_route_id = sample_trip_update.trip.route_id
    print(f'sample trip_id: {sample_trip_id}, route_id {sample_route_id}')
    return sample_route_id, sample_trip_id


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Join on routes and stops for context
    """)
    return


@app.cell
def _(Path, pl):
    CALENDAR_START = '20260510'
    dims_dir = Path(f'../data/dims/{CALENDAR_START}')
    dim_routes = pl.read_parquet(dims_dir/'routes.parquet')
    dim_stops = pl.read_parquet(dims_dir/'stops.parquet')
    return dim_routes, dims_dir


@app.cell
def _(dim_routes):
    dim_routes.sample(5)
    return


@app.cell
def _(dim_routes, pl, sample_route_id):
    dim_routes.filter(pl.col('route_id') == sample_route_id)
    return


@app.cell
def _(dims_dir, pl):
    dim_stop_times = pl.read_parquet(dims_dir / 'stop_times.parquet')
    dim_stop_times.sample(5)
    return (dim_stop_times,)


@app.cell
def _(dim_stop_times, pl, sample_trip_id):
    dim_stop_times.filter(pl.col("trip_id") == sample_trip_id)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    - Only first "stop" has `departure.time` - starting point
    - All subsequent stops have `arrival.time`
    """)
    return


@app.cell
def _(pl):
    def parse_trip_leg(trip_update) -> pl.DataFrame:
        """
        parse list of stop time updates into a flat schema with leg-grain
        instead of stop-grain:
        trip_id
        route_id
        vehicle_id
        update_timestamp
        stop_seq
        start_stop_id
        end_stop_id
        arrival_time
        departure_time
        1. start records with stop_seq 1 -> 2. fill all except arrival and end stop
        2. iterate through stu[1:]
        3. for each, get stop_id. assign to prev rec's end_stop_id
        4. get arrival time. assign to prev reco's arrival
        5. if last, return records
        6. if not, create new record with start = current
        """
        stops = []
        start = trip_update.stop_time_update[0]
        stop = dict(
            trip_id=trip_update.trip.trip_id,
            route_id=trip_update.trip.route_id,
            vehicle_id=trip_update.vehicle.id,
            update_timestamp=trip_update.timestamp,
            stop_sequence=start.stop_sequence,
            start_stop_id=start.stop_id,
            departure_time=start.departure.time,
        )
        stops.append(stop)
        leng = len(trip_update.stop_time_update)
        print(f'{leng} stops')
        for i, stu in enumerate(trip_update.stop_time_update[1:]):
            stops[-1]["end_stop_id"] = stu.stop_id
            stops[-1]["arrival_time"] = stu.arrival.time
            if i == leng - 2:
                print(f'at stop {i}, break')
                break
            stops.append(dict(
                trip_id=trip_update.trip.trip_id,
                route_id=trip_update.trip.route_id,
                vehicle_id=trip_update.vehicle.id,
                update_timestamp=trip_update.timestamp,
                stop_sequence=stu.stop_sequence,
                start_stop_id=stu.stop_id,
                departure_time=None,
            ))
        return pl.DataFrame(stops)

    return


@app.cell
def _(Path, convert_unix_to_datetime_est, gtfs, pl):
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

    return (parse_trip_feed,)


@app.function
def enrich_with_stops(facts, stops, routes):
    stop_cols = ['stop_id', 'stop_name']
    stops_sel = stops.select(stop_cols)
    routes_cols = ['route_id', 'route_long_name']
    routes_sel = routes.select(routes_cols)
    return facts.join(
        stops_sel,
        left_on='end_stop_id',
        right_on='stop_id',
        how='left',
    ).join(
        routes_sel,
        on='route_id',
        how='left'
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Join with `stop_times` dimension table for scheduled arrival on:
    - trip_id
    - stop_sequence
    """)
    return


@app.cell
def _(pl):
    def enrich_with_schedule(facts, stop_times) -> pl.DataFrame:
        cols = ['trip_id', 'arrival_time', 'departure_time', 'stop_sequence', 'stop_headsign']
        stop_times_sel = stop_times.select(cols)
        return facts.join(
            stop_times_sel,
            on=['trip_id', 'stop_sequence'],
            how='left',
            suffix='_sched'
        )


    return (enrich_with_schedule,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Missing `trip_id`

    Fetched `trips/update` API are now outside the period set in `calendar.txt`. Must refresh dims according to dates set in `calendar`
    """)
    return


@app.cell
def _(dims_dir, pl):
    path_cal = dims_dir / "calendar.parquet"
    df_cal = pl.read_parquet(path_cal)
    df_cal.show()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Orchestrate

    - takes dir of protobuf
    - denormalizes the stop updates
    - enrich with stop_time to get scheduled times
    - optionally enrich with route info and stop info
    - append to facts table (persist as parquet)
    """)
    return


@app.cell
def _(Path, dim_stop_times, enrich_with_schedule, parse_trip_feed, pl):
    def get_stop_arrival_schema(pb_path: Path) -> pl.DataFrame:
        stops = parse_trip_feed(pb_path)
        sched = enrich_with_schedule(stops, dim_stop_times)
        return sched

    return (get_stop_arrival_schema,)


@app.cell
def _(CURR_DATE, CURR_TIME, Path, get_stop_arrival_schema):
    pb_path = f'../data/raw/trip/{CURR_DATE}/{CURR_TIME}.pb'
    pb_path = Path(pb_path)
    sched = get_stop_arrival_schema(pb_path)
    sched.show()
    return (sched,)


@app.cell
def _(pl, sched):
    sched.filter(pl.col('route_id') == '189').sort(pl.col(['trip_id', 'stop_sequence']))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## UPSERT approach

    Merge on
    - route_id
    - trip_id
    - vehicle_id
    - stop_sequence

    newer values overwrites old; 'actual' times before trip occurence is just copies of the schedule
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Data Layers

    - raw (bronze)
        - ingest from `txt`, `protobuf`
        - keep types as is
        - trips to `facts_stop_schema`
        - vehicles to `facts_vehicle_pos` - where all vehicle pos are given latest API call
    - cleaned (silver)
        - dims to `parquet`
            - cast types
            - string HHMMSS
        - `facts_stop_schema`
            - cast unix time to datetime
    - enriched (gold)
        - join with stop_times
        - coerce scheduled times to datetime with context from feed timestamp
        - dwell = departure - arrival
        - delay = actual_arrival - sched_arrival
    """)
    return


@app.cell
def _(dims_dir, pl):
    dim_trips = pl.read_parquet(dims_dir / 'trips.parquet')
    dim_trips.head(5)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
