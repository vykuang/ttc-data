import os

API_KEY = os.environ.get("TRANSLINK_API_KEY", "")
URL_BASE = "https://gtfsapi.translink.ca/v3"
URL_PATHS = {
    "trip": "/gtfsrealtime",
    "vehicle": "/gtfsposition",
}
HEADERS = {"User-Agent": "Mozilla/5.0"}
AWS_BUCKET = "vb2k-translink-api"

GTFS_STATIC_URL = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
GTFS_FILES = ["routes", "stops", "trips", "stop_times", "shapes"]
