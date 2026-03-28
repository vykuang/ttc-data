# tests/test_get_api.py
import pytest
import responses as resp_mock
from moto import mock_aws
import boto3
import os

TRIP_FIXTURE = open("tests/fixtures/trip_update.pb", "rb").read()  # save a real .pb once

@mock_aws
@resp_mock.activate
def test_get_gtfs_raw_trip(monkeypatch):
    # point code at test bucket
    monkeypatch.setenv("AWS_BUCKET", "ttc-api-test")

    # create the mock S3 bucket
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="ttc-api-test")

    # mock the TTC API response
    resp_mock.add(
        resp_mock.GET,
        "https://gtfsrt.ttc.ca/trips/update",
        body=TRIP_FIXTURE,
        content_type="application/x-protobuf",
        status=200,
    )

    from get_api import get_gtfs_raw
    result = get_gtfs_raw(item="trip", format="binary")

    assert result is True

    # verify the object landed in S3
    s3 = boto3.client("s3", region_name="us-east-1")
    objects = s3.list_objects_v2(Bucket="ttc-api-test", Prefix="raw/trip/")
    assert objects["KeyCount"] == 1
    assert objects["Contents"][0]["Key"].endswith(".pb")