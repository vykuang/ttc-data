import marimo

__generated_with = "0.19.9"
app = marimo.App()


@app.cell
def _(mo):
    mo.md(r"""
    # Dev notebook - Retrieve API content and store in S3

    - GET GTFS API
    - Store in S3 in plaintext protobuf
    """)
    return


@app.cell
def _():
    from google.transit import gtfs_realtime_pb2
    import requests
    from datetime import datetime
    import boto3
    from botocore.exceptions import ClientError

    return ClientError, boto3, datetime, gtfs_realtime_pb2, requests


@app.cell
def _():
    URL_BASE = "https://gtfsrt.ttc.ca"
    URL_PATHS = {"trip": "/trips/update", "vehicle": "/vehicles/position"}
    HEADERS = {"User-Agent": "Mozilla/5.0"}
    return HEADERS, URL_BASE, URL_PATHS


@app.cell
def _(HEADERS, URL_BASE, requests):
    def get_gtfs_raw(url_path, param: dict = {"format": "binary"}):
        """
        Retrieve raw bytes for S3 storage; keep unparsed for ground truth
        """
        query_str = "?" + "&".join(f"{key}={val}" for key, val in param.items())
        url = URL_BASE + url_path + query_str
        resp = requests.get(url=url, headers=HEADERS, timeout=3)
        resp.raise_for_status()
        return resp.content

    return (get_gtfs_raw,)


@app.cell
def _(URL_PATHS, get_gtfs_raw):
    raw_bytes = get_gtfs_raw(URL_PATHS["trip"])
    print(f"type(entities) = {type(raw_bytes)}")
    return (raw_bytes,)


@app.cell
def _(raw_bytes):
    # wb necessary to work with bytes
    with open(file="trips.pb", mode="wb") as f:
        f.write(raw_bytes)
    return


@app.cell
def _():
    with open(file="trips.pb", mode="rb") as fr:
        raw_read = fr.read()
    return (raw_read,)


@app.cell
def _(gtfs_realtime_pb2):
    def parse_gtfs(raw_bytes):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        for i, entity in enumerate(feed.entity):
            if i > 3:
                break
            print(entity)

    return (parse_gtfs,)


@app.cell
def _(parse_gtfs, raw_read):
    parse_gtfs(raw_read)
    return


@app.cell
def _(datetime):
    datetime.date(datetime.now())
    return


@app.cell
def _(datetime):
    d = datetime.now().strftime(format="%Y%m%d")
    t = datetime.now().strftime(format="%H%M%S")
    print(d, t)
    return


@app.cell
def _(ClientError, boto3):
    def put_s3(bucket, obj, key):
        s3 = boto3.client("s3")
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=obj,
                ContentType="application/x-protobuf",
            )
        except ClientError as e:
            print(e)
            return False
        return True

    return (put_s3,)


@app.cell
def _(datetime, put_s3, raw_bytes):
    bucket = "ttc-api"
    d = datetime.now().strftime(format="%Y%m%d")
    t = datetime.now().strftime(format="%H%M%S")
    key = f"raw/trips/{d}/{t}.pb"
    put_s3(bucket=bucket, obj=raw_bytes, key=key)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
