import argparse
import logging
from datetime import datetime

import boto3
import constants
import requests
from botocore.exceptions import ClientError


def get_gtfs_realtime(item: str) -> bool:
    """
    Fetch GTFS-RT binary feed from Translink and write raw bytes to S3
    under raw/{item}/{date}/{time}.pb.
    """
    url = constants.URL_BASE + constants.URL_PATHS[item]
    params = {"apikey": constants.API_KEY}
    try:
        resp = requests.get(
            url=url, params=params, headers=constants.HEADERS, timeout=10
        )
        logging.info(f"querying {resp.url}")
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(e)
        return False

    s3 = boto3.client("s3")
    d = datetime.now().strftime("%Y%m%d")
    t = datetime.now().strftime("%H%M%S")
    key = f"raw/{item}/{d}/{t}.pb"
    try:
        s3.put_object(
            Bucket=constants.AWS_BUCKET,
            Key=key,
            Body=resp.content,
            ContentType="application/x-protobuf",
        )
        logging.info(f"wrote s3://{constants.AWS_BUCKET}/{key}")
    except ClientError as e:
        logging.error(e)
        return False

    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--item", choices=list(constants.URL_PATHS.keys()), default="trip")
    args = parser.parse_args()
    stored = get_gtfs_realtime(item=args.item)
    logging.info(f"S3 PUT status: {stored}")
