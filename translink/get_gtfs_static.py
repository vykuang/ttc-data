import io
import logging
import zipfile
from datetime import datetime

import boto3
import constants
import polars as pl
import requests
from botocore.exceptions import ClientError


def get_gtfs_static() -> bool:
    """
    Download the Translink GTFS static zip, extract each CSV file,
    convert to Parquet, and write to S3 under static/gtfs/{date}/{name}.parquet.
    """
    try:
        resp = requests.get(url=constants.GTFS_STATIC_URL, timeout=60)
        logging.info(f"downloaded GTFS static from {constants.GTFS_STATIC_URL}")
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(e)
        return False

    s3 = boto3.client("s3")
    d = datetime.now().strftime("%Y%m%d")
    success = True

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in constants.GTFS_FILES:
            filename = f"{name}.txt"
            if filename not in zf.namelist():
                logging.warning(f"{filename} not found in GTFS zip, skipping")
                continue

            with zf.open(filename) as f:
                df = pl.read_csv(f, infer_schema_length=None)

            buf = io.BytesIO()
            df.write_parquet(buf)
            key = f"static/gtfs/{d}/{name}.parquet"
            try:
                s3.put_object(
                    Bucket=constants.AWS_BUCKET,
                    Key=key,
                    Body=buf.getvalue(),
                    ContentType="application/octet-stream",
                )
                logging.info(f"wrote s3://{constants.AWS_BUCKET}/{key} ({len(df)} rows)")
            except ClientError as e:
                logging.error(e)
                success = False

    return success


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    stored = get_gtfs_static()
    logging.info(f"GTFS static ingest status: {stored}")
