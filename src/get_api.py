import argparse
import logging
from datetime import datetime
import os
import boto3
import constants
import requests
from botocore.exceptions import ClientError
from typing import Literal

def build_url(item: Literal['trip', 'vehicle']) -> str:
    url_path = constants.URL_PATHS[item]
    return constants.URL_BASE + url_path

def get_gtfs_raw(item: Literal['trip', 'vehicle'], format: str):
    """
    Retrieve raw bytes for S3 storage; keep unparsed for ground truth
    """
    url = build_url(item)
    params = {"format": format}
    try:
        resp = requests.get(
            url=url, params=params, headers=constants.HEADERS, timeout=3
        )
        logging.info(f"querying {resp.url}")

        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(e)
    AWS_BUCKET = os.getenv('AWS_BUCKET')

    s3 = boto3.client("s3")
    d = datetime.now().strftime(format="%Y%m%d")
    t = datetime.now().strftime(format="%H%M%S")
    key = f"raw/{item}/{d}/{t}.pb"
    try:
        s3.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=resp.content,
            ContentType="application/x-protobuf",
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    opt = parser.add_argument
    opt("--item", choices=["trip", "vehicle"], default="trip")
    opt("--format", choices=["text", "binary"], default="binary")
    args = parser.parse_args()
    stored = get_gtfs_raw(item=args.item, format=args.format)
    logging.info(f"S3 PUT status: {stored}")
