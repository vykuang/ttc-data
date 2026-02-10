"""
How DAG would look like if airflow ran it
"""

import logging
from datetime import datetime

import boto3
import requests
from airflow.sdk import dag, task
from botocore.exceptions import ClientError

import constants


@dag()
def extract_load(item: str = "trip", params: dict = {"format": "binary"}):
    """ """

    @task()
    def get_gtfs_raw(item: str, params: dict):
        """
        Retrieve raw bytes for S3 storage; keep unparsed for ground truth
        """
        url_path = constants.URL_PATHS[item]
        # query_str = "?" + "&".join(f"{key}={val}" for key, val in param.items())
        url = constants.URL_BASE + url_path
        try:
            resp = requests.get(
                url=url, params=params, headers=constants.HEADERS, timeout=3
            )
            logging.info(f"querying {resp.url}")

            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(e)

        s3 = boto3.client("s3")
        d = datetime.now().strftime(format="%Y%m%d")
        t = datetime.now().strftime(format="%H%M%S")
        key = f"raw/{item}/{d}/{t}.pb"
        try:
            s3.put_object(
                Bucket=constants.AWS_BUCKET,
                Key=key,
                Body=resp.content,
                ContentType="application/x-protobuf",
            )
        except ClientError as e:
            logging.error(e)
            return False
        return True

    stored = get_gtfs_raw(item=item, params=params)
    logging.info(f"S3 PUT status: {stored}")


extract_load()
