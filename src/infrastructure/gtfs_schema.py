"""
GTFS schema for reading dimensions
instead of hard-coding polars type format, create an ENUM abstraction
then, depending on the format, eg polars or pandas, use the corresponding
format mapper to get from ENUM (eg FieldType.STRING) to format specific type
eg string: pl.Utf8 in _POLARS_MAP
"""
import polars as pl

# generated from https://gtfs.org/documentation/schedule/reference/#field-types
ROUTES_DTYPES = {
    "route_id": pl.Utf8,
    "agency_id": pl.Utf8,
    "route_short_name": pl.Utf8,
    "route_long_name": pl.Utf8,
    "route_desc": pl.Utf8,
    "route_type": pl.Int64,
    "route_url": pl.Utf8,
    "route_color": pl.Utf8,
    "route_text_color": pl.Utf8,
    "route_sort_order": pl.Int64,          # if present
    "continuous_pickup": pl.Int64,         # if present
    "continuous_drop_off": pl.Int64,       # if present
}
STOPS_DTYPES = {
    "stop_id": pl.Utf8,
    "stop_code": pl.Utf8,
    "stop_name": pl.Utf8,
    "stop_desc": pl.Utf8,
    "stop_lat": pl.Float64,
    "stop_lon": pl.Float64,
    "zone_id": pl.Utf8,
    "stop_url": pl.Utf8,
    "location_type": pl.Int64,
    "parent_station": pl.Utf8,
    "stop_timezone": pl.Utf8,
    "wheelchair_boarding": pl.Int64,
    "level_id": pl.Utf8,
    "platform_code": pl.Utf8,
}
TRIPS_DTYPES = {
    "route_id": pl.Utf8,
    "service_id": pl.Utf8,
    "trip_id": pl.Utf8,
    "trip_headsign": pl.Utf8,
    "trip_short_name": pl.Utf8,
    "direction_id": pl.Int64,
    "block_id": pl.Utf8,
    "shape_id": pl.Utf8,
    "wheelchair_accessible": pl.Int64,
    "bikes_allowed": pl.Int64,
}
STOP_TIMES_DTYPES = {
    "trip_id": pl.Utf8,
    "arrival_time": pl.Utf8,    # HH:MM:SS, sometimes >24h
    "departure_time": pl.Utf8,  # same
    "stop_id": pl.Utf8,
    "stop_sequence": pl.Int64,
    "stop_headsign": pl.Utf8,
    "pickup_type": pl.Int64,
    "drop_off_type": pl.Int64,
    "continuous_pickup": pl.Int64,
    "continuous_drop_off": pl.Int64,
    "shape_dist_traveled": pl.Float64,
    "timepoint": pl.Int64,
}
CALENDAR_DTYPES = {
    "service_id": pl.Utf8,
    "monday": pl.Int64,
    "tuesday": pl.Int64,
    "wednesday": pl.Int64,
    "thursday": pl.Int64,
    "friday": pl.Int64,
    "saturday": pl.Int64,
    "sunday": pl.Int64,
    "start_date": pl.Utf8,   # YYYYMMDD; can parse later
    "end_date": pl.Utf8,
}
CALENDAR_DATES_DTYPES = {
    "service_id": pl.Utf8,
    "date": pl.Utf8,         # YYYYMMDD
    "exception_type": pl.Int64,
}
SHAPES_DTYPES = {
    "shape_id": pl.Utf8,
    "shape_pt_lat": pl.Float64,
    "shape_pt_lon": pl.Float64,
    "shape_pt_sequence": pl.Int64,
    "shape_dist_traveled": pl.Float64,
}
AGENCY_DTYPES = {
    "agency_id": pl.Utf8,
    "agency_name": pl.Utf8,
    "agency_url": pl.Utf8,
    "agency_timezone": pl.Utf8,
    "agency_lang": pl.Utf8,
    "agency_phone": pl.Utf8,
    "agency_fare_url": pl.Utf8,
    "agency_email": pl.Utf8,
}
DIM_SCHEMA_MAP = {
    'routes': ROUTES_DTYPES,
    'agency': AGENCY_DTYPES,
    'calendar_dates': CALENDAR_DATES_DTYPES,
    'calendar': CALENDAR_DTYPES,
    'shapes': SHAPES_DTYPES,
    'stop_times': STOP_TIMES_DTYPES,
    'stops': STOPS_DTYPES,
    'trips': TRIPS_DTYPES,
}