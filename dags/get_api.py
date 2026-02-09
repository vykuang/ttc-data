from datetime import datetime

import boto3
import requests
from airflow.sdk import dag, task
from botocore.exceptions import ClientError

from app import constants


@dag()
def extract_load(item: str = "trip"):
    """ """

    @task()
    def get_gtfs_raw(url_path, param: dict = {"format": "binary"}):
        """
        Retrieve raw bytes for S3 storage; keep unparsed for ground truth
        """
        query_str = "?" + "&".join(f"{key}={val}" for key, val in param.items())
        url = constants.URL_BASE + url_path + query_str
        resp = requests.get(url=url, headers=constants.HEADERS, timeout=3)
        resp.raise_for_status()
        return resp.content

    @task()
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

    resp = get_gtfs_raw(url_path=constants.URL_PATHS[item])
    d = datetime.now().strftime(format="%Y%m%d")
    t = datetime.now().strftime(format="%H%M%S")
    key = f"raw/{item}/{d}/{t}.pb"
    put_s3(bucket=constants.AWS_BUCKET, obj=resp, key=key)


extract_load()
