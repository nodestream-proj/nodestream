import json

import pytest
from hamcrest import assert_that, has_items, has_key, has_length, not_
from moto import mock_s3

BUCKET_NAME = "bucket"
PREFIX = "prefix"
NUM_OBJECTS = 10


@pytest.fixture
def s3_client():
    with mock_s3():
        import boto3

        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def subject(mocker):
    from nodestream.pipeline.extractors.stores.aws.credential_utils import (
        AwsClientFactory,
    )
    from nodestream.pipeline.extractors.stores.aws.s3_extractor import S3Extractor

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


@pytest.fixture
def subject_with_populated_csv_objects(subject, s3_client):
    subject.s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body="hello".encode())
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/bar/{i}.csv",
            Body=f"column1,column2\nvalue{i},value{i+10}",
        )
    return subject

@pytest.fixture
def subject_with_populated_jsonl_objects(subject, s3_client):
    subject.s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    for i in range(1):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/bar/{i}.jsonl",
            Body='{"test":"test"}\n{"test2":"test2"}',
        )
    return subject

@pytest.fixture
def subject_with_archieved_objects(subject_with_archiving_enabled, s3_client):
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"archieve/{i}.json",
            Body=json.dumps({"hello": i}),
        )
    return subject_with_archiving_enabled


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_csv_files(
    subject_with_populated_csv_objects,
):
    expected_results = [
        {"column1": f"value{i}", "column2": f"value{i+10}"} for i in range(NUM_OBJECTS)
    ]
    results = [
        result async for result in subject_with_populated_csv_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))

@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_jsonl_files(
    subject_with_populated_jsonl_objects,
):
    expected_results = [
        {"test": "test"}, {"test2": "test2"}
    ]
    results = [
        result async for result in subject_with_populated_jsonl_objects.extract_records()
    ]
    assert_that(results, has_length(2))
    assert_that(results, has_items(*expected_results))


@pytest.fixture
def subject_with_archiving_enabled(subject_with_populated_objects):
    subject_with_populated_objects.archive_dir = "archive"
    return subject_with_populated_objects


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_files(subject_with_populated_objects):
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result async for result in subject_with_populated_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_archives_objects(subject_with_archiving_enabled, s3_client):
    [result async for result in subject_with_archiving_enabled.extract_records()]
    assert_that(
        s3_client.list_objects(Bucket=BUCKET_NAME, Prefix="archive", MaxKeys=1000)[
            "Contents"
        ],
        has_length(NUM_OBJECTS),
    )


@pytest.mark.asyncio
async def test_s3_extractor_does_not_archive_objects_when_archive_dir_is_none(
    subject_with_populated_objects, s3_client
):
    [result async for result in subject_with_populated_objects.extract_records()]
    assert_that(
        s3_client.list_objects(Bucket=BUCKET_NAME, Prefix="archive", MaxKeys=1000),
        not_(has_key("Contents")),
    )


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
