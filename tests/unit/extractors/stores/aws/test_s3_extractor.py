import json

import pytest
from hamcrest import assert_that, has_items, has_length
from moto import mock_s3

BUCKET_NAME = "bucket"
PREFIX = "prefix"
NUM_OBJECTS = 1000


@pytest.fixture
def s3_client():
    with mock_s3():
        import boto3

        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def subject(mocker):
    from nodestream.extractors.stores.aws.credential_utils import AwsClientFactory
    from nodestream.extractors.stores.aws.s3_extractor import S3Extractor

    mocker.patch.object(AwsClientFactory, "make_client")
    return S3Extractor(
        bucket="bucket",
        prefix=PREFIX,
        s3_client=AwsClientFactory().make_client("s3"),
    )


@pytest.fixture
def subject_with_populated_objects(subject, s3_client):
    subject.s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body="hello".encode())
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json",
            Body=json.dumps({"hello": i}),
        )
    return subject


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_files(subject_with_populated_objects):
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result async for result in subject_with_populated_objects.extract_records()
    ]
    assert_that(results, has_length(1000))
    assert_that(results, has_items(*expected_results))


@pytest.mark.parametrize(
    "key,object_format_arg,expected_object_format",
    [
        ["foo/bar/baz.json", None, ".json"],
        ["foo/bar/baz.csv", None, ".csv"],
        ["foo/bar/baz", None, None],
        ["foo/bar/baz.jsonbar", ".json", ".json"],
        ["foo/bar/baz", ".json", ".json"],
    ],
)
def test_infer_object_format(subject, key, object_format_arg, expected_object_format):
    subject.object_format = object_format_arg
    if expected_object_format is None:
        with pytest.raises(ValueError):
            subject.infer_object_format(key)
    else:
        assert_that(subject.infer_object_format(key), expected_object_format)
