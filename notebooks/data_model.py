import marimo

__generated_with = "0.22.4"
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
    - trip
        - trip_id: str
        - schedule_relationship: ['SCHEDULED', 'ADDED']
        - route_id: str
    - stop_time_update: list
        - stop_sequence: int, [1, 2, ..., n]
        - departure
            - time: int, unix time in seconds
        - stop_id: str
        - schedule_relationship: ['SCHEDULED', ...]
    - vehicle
        - id: str
    - timestamp: int, unix time in seconds

    ### `/vehicles/position`

    each entity:
    - id
    - vehicle

    each vehicle:
    - trip
        - trip_id: int
        - schedule_relationship: ['SCHEDULED']
        - route_id: str
    - position
        - latitude: int, 6 decimals
        - longitude: int
        - bearing: int, 0-360?
        - speed: str, km/h
    - current_stop_sequence: int
    - current_status: ['INCOMING_AT]
    - timestamp: int
    - stop_id: str
    - vehicle
        - id: str
    - occupancy_status: ['FEW_SEATS_AVAILABLE']

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
    from google.transit import gtfs_realtime_pb2
    import polars as pl
    from pathlib import Path
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime


    return (
        Path,
        ThreadPoolExecutor,
        as_completed,
        boto3,
        datetime,
        gtfs_realtime_pb2,
    )


@app.cell
def _():
    AWS_BUCKET = 'ttc-api'
    path = 'raw/{item}/{date}/{time}.pb'
    return (AWS_BUCKET,)


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
    Create set for each of following by iterating through all API results for 2026-3-1:
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
def _(stop_time_update_sched, trip_sched, v_occupancy, v_status, v_trip_sched):
    # iterate through all pb
    def update_enum_sets(feed, item: str = 'trip'):
        for entity in feed.entity:
            if item == 'trip':
                trip = entity.trip_update
                trip_sched.add(trip.trip.schedule_relationship)
                scheds = {trip.stop_time_update[i].schedule_relationship for i in range(len(entity.trip_update.stop_time_update))}
                stop_time_update_sched.update(scheds)
            elif item == 'vehicle':
                veh = entity.vehicle
                v_trip_sched.add(veh.trip.schedule_relationship)
                v_status.add(veh.current_status)
                v_occupancy.add(veh.occupancy_status)


    return (update_enum_sets,)


@app.cell
def _(gtfs_realtime_pb2):
    feed = gtfs_realtime_pb2.FeedMessage()
    return (feed,)


@app.cell
def _():
    # init sets
    trip_sched = set()
    stop_time_update_sched = set()
    v_trip_sched = set()
    v_status = set()
    v_occupancy = set()
    return (
        stop_time_update_sched,
        trip_sched,
        v_occupancy,
        v_status,
        v_trip_sched,
    )


@app.cell
def _(Path, download_s3_many, feed, update_enum_sets):
    prefix_template = 'raw/{item}/20260301'
    for item in ['trip', 'vehicle']:
        prefix = prefix_template.format(item=item)
        download_s3_many(prefix)
        pb_dir = Path('../data') / prefix
        print(f'processing {pb_dir}')
        glob_pbs = list(pb_dir.glob('*.pb'))
        total = len(glob_pbs)
        for i, pb in enumerate(glob_pbs):
            with open(pb, 'rb') as f:
                feed.ParseFromString(f.read())
                update_enum_sets(feed, item)
                msg = f'processed {i}/{total} files'
                print(msg.ljust(60), end='\r', flush=True)
    return


@app.cell
def _(stop_time_update_sched, trip_sched, v_occupancy, v_status, v_trip_sched):
    print(f'trip_sched: {trip_sched}')
    print(f'stop_time_update_sched: {stop_time_update_sched}')
    print(f'v_trip_sched: {v_trip_sched}')
    print(f'v_status: {v_status}')
    print(f'v_occupancy: {v_occupancy}')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `print` uses the `enum` descriptors to render the names, even though underlying values are `int`s
    """)
    return


@app.cell
def _(gtfs_realtime_pb2):
    gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(0)
    return


@app.cell
def _(gtfs_realtime_pb2):
    gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Value('SCHEDULED')

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
def _(feed):
    with open('../data/raw/trip/20260301/090001.pb', 'rb') as ff:
        feed.ParseFromString(ff.read())
        foo = feed.entity
    return (foo,)


@app.cell
def _(foo):
    tsr = {stu.schedule_relationship for stu in foo[25].trip_update.stop_time_update}
    tsr
    return


@app.cell
def _(datetime):
    def format_unix_time(unix_time: int):
        ds = datetime.fromtimestamp(unix_time)
        return ds.strftime(format="%H%M%S")

    return (format_unix_time,)


@app.cell
def _(foo, format_unix_time):
    sample_arrival_times = [format_unix_time(stop_seq.arrival.time) for stop_seq in foo[0].trip_update.stop_time_update]
    print(f'arrival times for trip update from 090001:\n{sample_arrival_times}')
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
    1. for each
    """)
    return


@app.cell
def _():
    # iterate over blob
    return


@app.cell
def _(feed):
    feed.entity[0]
    return


@app.cell
def _(feed):
    trip = feed.entity[0]
    trip.vehicle
    return (trip,)


@app.cell
def _(trip):
    len(trip.trip_update.stop_time_update)
    return


@app.cell
def _(trip):
    trip.trip_update.stop_time_update
    return


@app.cell
def _(trip):
    trip.trip_update.stop_time_update[0].departure.time
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
