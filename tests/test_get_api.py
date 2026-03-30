# tests/test_get_api.py
import responses as resp_mock
from moto import mock_aws
import boto3
from get_api import build_url, get_gtfs_raw
import pytest

@mock_aws
@resp_mock.activate
@pytest.mark.parametrize("item,fixture_name", [
    ('trip', 'get_trip_fixture'),
    ('vehicle', 'get_vehicle_fixture'),
])
def test_get_gtfs_raw(monkeypatch, item, fixture_name, request):
    """
    monkeypatch provided by pytest automatically when pytest runs this
    parametrize means pytest will run this for each pair of item,fixture_name
    requests is another built-in fixture that has a .getfixturevalue method
    to dynamically retrieve fixture at runtime by name
    """
    # point code at test bucket
    monkeypatch.setenv("AWS_BUCKET", "ttc-api-test")

    # create the mock S3 bucket
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="ttc-api-test")

    # mock the TTC API response
    resp_mock.add(
        method=resp_mock.GET,
        url=build_url(item),
        body=request.getfixturevalue(fixture_name),
        content_type="application/x-protobuf",
        status=200,
    )
    # imported here so that monkeypatch can setenv first before AWS_BUCKET is read
    # at module level in get_api
    result = get_gtfs_raw(item=item, format="binary")
    assert result is True

    # verify the object landed in S3
    s3 = boto3.client("s3", region_name="us-east-1")
    objects = s3.list_objects_v2(Bucket="ttc-api-test", Prefix=f"raw/{item}/")
    assert objects["KeyCount"] == 1
    assert objects["Contents"][0]["Key"].endswith(".pb")
